// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::pin::Pin;
use std::borrow::Cow;
use std::sync::Arc;

use futures::Stream;
use futures::stream::unfold;
use nativelink_proto::com::github::trace_machina::nativelink::bep::{
    BuildInfo, ListBuildsRequest, ListBuildsResponse, WatchBuildRequest, WatchBuildResponse,
    build_event_subscription_server::{BuildEventSubscription, BuildEventSubscriptionServer},
};
use nativelink_proto::com::github::trace_machina::nativelink::events::BepEvent;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use parking_lot::RwLock;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{Level, instrument};

use crate::bep_server::{BepEventNotification, BepIndex};

#[derive(Debug)]
pub struct BepSubscriptionService {
    event_rx_factory: tokio::sync::broadcast::Sender<BepEventNotification>,
    index: Arc<RwLock<BepIndex>>,
    store: Store,
}

impl BepSubscriptionService {
    pub fn new(
        event_tx: tokio::sync::broadcast::Sender<BepEventNotification>,
        index: Arc<RwLock<BepIndex>>,
        store: Store,
    ) -> Self {
        Self {
            event_rx_factory: event_tx,
            index,
            store,
        }
    }

    pub fn into_service(self) -> BuildEventSubscriptionServer<Self> {
        BuildEventSubscriptionServer::new(self)
    }
}

type WatchBuildStream =
    Pin<Box<dyn Stream<Item = Result<WatchBuildResponse, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl BuildEventSubscription for BepSubscriptionService {
    #[instrument(err, level = Level::ERROR, skip_all)]
    async fn list_builds(
        &self,
        _request: Request<ListBuildsRequest>,
    ) -> Result<Response<ListBuildsResponse>, Status> {
        let idx = self.index.read();
        let builds = idx
            .values()
            .map(|meta| BuildInfo {
                build_id: meta.build_id.clone(),
                invocation_id: meta.invocation_id.clone(),
                identity: meta.identity.clone(),
                start_time: meta.start_time.clone(),
                finished: meta.finished,
                command: meta.command.clone(),
                event_count: meta.event_count,
            })
            .collect();
        Ok(Response::new(ListBuildsResponse { builds }))
    }

    type WatchBuildStream = WatchBuildStream;

    #[instrument(err, level = Level::ERROR, skip_all, fields(request = ?grpc_request.get_ref()))]
    async fn watch_build(
        &self,
        grpc_request: Request<WatchBuildRequest>,
    ) -> Result<Response<Self::WatchBuildStream>, Status> {
        let req = grpc_request.into_inner();
        let build_id = req.build_id;
        let invocation_id = req.invocation_id;
        let start_sequence = req.start_sequence;

        let rx = self.event_rx_factory.subscribe();

        let build_finished = {
            let idx = self.index.read();
            idx.get(&format!("{build_id}:{invocation_id}"))
                .map_or(false, |m| m.finished)
        };

        let mut replay_events = Vec::new();
        let mut seq = if start_sequence == 0 { 1 } else { start_sequence };
        loop {
            let store_key = StoreKey::Str(Cow::Owned(format!(
                "BepEvent:be:{build_id}:{invocation_id}:{seq}",
            )));
            match self.store.has(store_key.clone()).await {
                Ok(Some(_)) => {
                    if let Ok(data) = self
                        .store
                        .get_part_unchunked(store_key, 0, None)
                        .await
                    {
                        if let Ok(bep_event) = BepEvent::decode(data.as_ref()) {
                            let (bazel_bytes, timestamp) = extract_bazel_event(&bep_event);
                            replay_events.push(WatchBuildResponse {
                                sequence_number: seq,
                                bazel_event: bazel_bytes,
                                event_time: timestamp,
                            });
                        }
                    }
                    seq += 1;
                }
                _ => break,
            }
        }

        enum Phase {
            Replay(std::vec::IntoIter<WatchBuildResponse>),
            Live,
        }

        struct State {
            rx: tokio::sync::broadcast::Receiver<BepEventNotification>,
            build_id: String,
            invocation_id: String,
            phase: Phase,
            finished: bool,
        }

        let stream = unfold(
            Some(State {
                rx,
                build_id: build_id.clone(),
                invocation_id: invocation_id.clone(),
                phase: Phase::Replay(replay_events.into_iter()),
                finished: build_finished,
            }),
            move |maybe_state| async move {
                let mut state = maybe_state?;
                loop {
                    match &mut state.phase {
                        Phase::Replay(iter) => {
                            if let Some(event) = iter.next() {
                                return Some((Ok(event), Some(state)));
                            }
                            if state.finished {
                                return None;
                            }
                            state.phase = Phase::Live;
                        }
                        Phase::Live => match state.rx.recv().await {
                            Ok(notification) => {
                                if notification.build_id == state.build_id
                                    && notification.invocation_id == state.invocation_id
                                {
                                    let resp = WatchBuildResponse {
                                        sequence_number: notification.sequence_number,
                                        bazel_event: notification.bazel_event_bytes,
                                        event_time: notification.timestamp,
                                    };
                                    return Some((Ok(resp), Some(state)));
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                return None;
                            }
                        },
                    }
                }
            },
        );

        Ok(Response::new(Box::pin(stream)))
    }
}

fn extract_bazel_event(bep_event: &BepEvent) -> (bytes::Bytes, Option<::prost_types::Timestamp>) {
    use nativelink_proto::com::github::trace_machina::nativelink::events::bep_event::Event;
    match &bep_event.event {
        Some(Event::BuildToolEvent(req)) => {
            crate::bep_server::extract_bazel_event_from_request(req)
        }
        _ => (bytes::Bytes::new(), None),
    }
}
