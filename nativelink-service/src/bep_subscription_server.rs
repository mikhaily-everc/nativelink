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
use futures::StreamExt;
use futures::future::ready;
use futures::stream::unfold;
use nativelink_proto::com::github::trace_machina::nativelink::bep::{
    BuildInfo, ListBuildsRequest, ListBuildsResponse, SchedulerEvent, WatchBuildRequest,
    WatchBuildResponse,
    build_event_subscription_server::{BuildEventSubscription, BuildEventSubscriptionServer},
};
use nativelink_proto::com::github::trace_machina::nativelink::events::BepEvent;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use parking_lot::RwLock;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{Level, instrument};

use crate::bep_server::{BepEventNotification, BepIndex, BepPayload};

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
                task_id: meta.task_id.clone(),
                task_name: meta.task_name.clone(),
                scheduler_event_count: meta.scheduler_event_count,
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
        let start_scheduler_sequence = req.start_scheduler_sequence;

        // Subscribe BEFORE snapshotting counts so no live event is missed between
        // the snapshot and subscription. One receiver per sub-stream (broadcast
        // receivers can't be cloned); both observe the same notification stream.
        let bazel_rx = self.event_rx_factory.subscribe();
        let sched_rx = self.event_rx_factory.subscribe();

        // Read the finished flag and both authoritative counts under a single
        // lock. Each count is `highest_seq + 1` for its namespace, so the
        // contiguous stored run to replay is exactly [start, count).
        let (build_finished, event_count, scheduler_event_count) = {
            let idx = self.index.read();
            idx.get(&format!("{build_id}:{invocation_id}")).map_or(
                (false, 0, 0),
                |m| (m.finished, m.event_count, m.scheduler_event_count),
            )
        };

        let start_seq = if start_sequence == 0 { 1 } else { start_sequence };
        // se: sequences are server-assigned and contiguous from 0.
        let start_sched_seq = start_scheduler_sequence.max(0);

        // Replay keeps REPLAY_CONCURRENCY store fetches continuously in flight
        // (ordered), so per-event round-trip latency overlaps streaming instead
        // of serializing behind fixed prefetch windows. Otherwise fatal for a
        // large build whose events have aged into the slow (S3) tier (~hundreds
        // of ms each): 20k events would walk hundreds of sequential windows.
        const REPLAY_CONCURRENCY: usize = 256;

        // --- Bazel (`be:`) sub-stream: replay [start_seq, event_count) then live.
        let bazel_replay = {
            let store = self.store.clone();
            let build_id = build_id.clone();
            let invocation_id = invocation_id.clone();
            futures::stream::iter(start_seq..event_count)
                .map(move |seq| {
                    let store = store.clone();
                    let key = StoreKey::Str(Cow::Owned(format!(
                        "BepEvent:be:{build_id}:{invocation_id}:{seq}",
                    )));
                    async move { (seq, store.get_part_unchunked(key, 0, None).await) }
                })
                .buffered(REPLAY_CONCURRENCY)
                // Stop the contiguous run at the first missing/erroring key, as a
                // safety net against a hole in the stored sequence.
                .take_while(|(_, res)| ready(res.is_ok()))
                // Skip undecodable events but keep streaming the rest.
                .filter_map(|(seq, res)| {
                    ready(res.ok().and_then(|data| {
                        BepEvent::decode(data.as_ref()).ok().map(|bep_event| {
                            let (bazel_event, event_time) = extract_bazel_event(&bep_event);
                            Ok(WatchBuildResponse {
                                sequence_number: seq,
                                bazel_event,
                                event_time,
                                scheduler_event: None,
                            })
                        })
                    }))
                })
        };

        let bazel_live = unfold(
            LiveState {
                rx: bazel_rx,
                build_id: build_id.clone(),
                invocation_id: invocation_id.clone(),
                replay_end: event_count,
                active: !build_finished,
            },
            move |mut state| async move {
                if !state.active {
                    return None;
                }
                loop {
                    match state.rx.recv().await {
                        Ok(notification) => {
                            if notification.build_id != state.build_id
                                || notification.invocation_id != state.invocation_id
                            {
                                continue;
                            }
                            if let BepPayload::Bazel {
                                sequence_number,
                                bazel_event_bytes,
                                timestamp,
                            } = notification.payload
                            {
                                if sequence_number >= state.replay_end {
                                    let resp = WatchBuildResponse {
                                        sequence_number,
                                        bazel_event: bazel_event_bytes,
                                        event_time: timestamp,
                                        scheduler_event: None,
                                    };
                                    return Some((Ok(resp), state));
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            },
        );

        // --- Scheduler (`se:`) sub-stream: replay [start_sched_seq, count) then live.
        let sched_replay = {
            let store = self.store.clone();
            let build_id = build_id.clone();
            let invocation_id = invocation_id.clone();
            futures::stream::iter(start_sched_seq..scheduler_event_count)
                .map(move |seq| {
                    let store = store.clone();
                    let key = StoreKey::Str(Cow::Owned(format!(
                        "BepEvent:se:{build_id}:{invocation_id}:{seq}",
                    )));
                    async move { (seq, store.get_part_unchunked(key, 0, None).await) }
                })
                .buffered(REPLAY_CONCURRENCY)
                .take_while(|(_, res)| ready(res.is_ok()))
                .filter_map(|(seq, res)| {
                    ready(res.ok().and_then(|data| {
                        SchedulerEvent::decode(data.as_ref()).ok().map(|event| {
                            Ok(scheduler_response(seq, event))
                        })
                    }))
                })
        };

        let sched_live = unfold(
            LiveState {
                rx: sched_rx,
                build_id: build_id.clone(),
                invocation_id: invocation_id.clone(),
                replay_end: scheduler_event_count,
                active: !build_finished,
            },
            move |mut state| async move {
                if !state.active {
                    return None;
                }
                loop {
                    match state.rx.recv().await {
                        Ok(notification) => {
                            if notification.build_id != state.build_id
                                || notification.invocation_id != state.invocation_id
                            {
                                continue;
                            }
                            if let BepPayload::Scheduler { sched_seq, event } = notification.payload
                            {
                                if sched_seq >= state.replay_end {
                                    return Some((Ok(scheduler_response(sched_seq, event)), state));
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            },
        );

        // Each namespace stays strictly ordered within itself (the resume cursor
        // the client relies on); `select` interleaves the two by arrival. The
        // client correlates scheduler events to targets by `target_id`, not by
        // position relative to a bazel frame.
        let bazel_sub: WatchBuildStream = Box::pin(bazel_replay.chain(bazel_live));
        let sched_sub: WatchBuildStream = Box::pin(sched_replay.chain(sched_live));
        Ok(Response::new(Box::pin(futures::stream::select(
            bazel_sub, sched_sub,
        ))))
    }
}

/// Shared live-tail state for one namespace's broadcast subscription.
struct LiveState {
    rx: tokio::sync::broadcast::Receiver<BepEventNotification>,
    build_id: String,
    invocation_id: String,
    /// Replay covered [start, replay_end); only forward strictly-newer live
    /// events so replay and live never overlap (no duplicates).
    replay_end: i64,
    active: bool,
}

/// Build a `WatchBuildResponse` carrying a scheduler frame: `bazel_event` empty,
/// `scheduler_event` set, `sequence_number` = the `se:` cursor, `event_time` =
/// the transition time so the client can order the overlay.
fn scheduler_response(sched_seq: i64, event: SchedulerEvent) -> WatchBuildResponse {
    WatchBuildResponse {
        sequence_number: sched_seq,
        bazel_event: bytes::Bytes::new(),
        event_time: event.transition_time.clone(),
        scheduler_event: Some(event),
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
