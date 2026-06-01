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
use core::time::Duration;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use futures::Stream;
use futures::stream::unfold;
use nativelink_error::{Error, ResultExt};
use nativelink_proto::build_event_stream::{self, build_event};
use nativelink_proto::com::github::trace_machina::nativelink::bep::{BuildInfo, SchedulerEvent};
use nativelink_proto::com::github::trace_machina::nativelink::events::{BepEvent, bep_event};
use nativelink_proto::google::devtools::build::v1::publish_build_event_server::{
    PublishBuildEvent, PublishBuildEventServer,
};
use nativelink_proto::google::devtools::build::v1::{
    PublishBuildToolEventStreamRequest, PublishBuildToolEventStreamResponse,
    PublishLifecycleEventRequest,
};
use nativelink_store::store_manager::StoreManager;
use nativelink_util::background_spawn;
use nativelink_util::spawn;
use nativelink_util::store_trait::{Store, StoreDriver, StoreKey, StoreLike};
use nativelink_util::task::JoinHandleDropGuard;
use opentelemetry::baggage::BaggageExt;
use opentelemetry::context::Context;
use opentelemetry_semantic_conventions::attribute::ENDUSER_ID;
use parking_lot::RwLock;
use prost::Message;
use tonic::{Request, Response, Result, Status, Streaming};
use tracing::{Level, instrument};

/// Current version of the BEP event. This might be used in the future if
/// there is a breaking change in the BEP event format.
const BEP_EVENT_VERSION: u32 = 0;

const BROADCAST_CAPACITY: usize = 4096;

#[allow(clippy::result_large_err, reason = "TODO Fix this. Breaks on nightly")]
fn get_identity() -> Option<String> {
    Context::current()
        .baggage()
        .get(ENDUSER_ID)
        .map(|value| value.as_str().to_string())
}

/// Live notification broadcast to WatchBuild subscribers. A notification is
/// either a Bazel BES event (`be:` namespace, sequence assigned by the client)
/// or a scheduler operation event (`se:` namespace, sequence assigned by this
/// server). The two namespaces have independent monotonic counters; subscribers
/// filter on the variant and resume each from its own cursor.
#[derive(Debug, Clone)]
pub enum BepPayload {
    Bazel {
        sequence_number: i64,
        bazel_event_bytes: bytes::Bytes,
        timestamp: Option<::prost_types::Timestamp>,
    },
    Scheduler {
        sched_seq: i64,
        event: SchedulerEvent,
    },
}

#[derive(Debug, Clone)]
pub struct BepEventNotification {
    pub build_id: String,
    pub invocation_id: String,
    pub payload: BepPayload,
}

#[derive(Debug, Clone)]
pub struct BuildMeta {
    pub build_id: String,
    pub invocation_id: String,
    pub identity: String,
    pub start_time: Option<::prost_types::Timestamp>,
    pub finished: bool,
    pub command: String,
    pub event_count: i64,
    /// Aspect task id/name from the BuildMetadata BEP event (keys
    /// ASPECT_TASK_ID / ASPECT_TASK_NAME). Empty until that event arrives.
    pub task_id: String,
    pub task_name: String,
    /// Count of scheduler events in the `se:` namespace for this build
    /// (`highest_sched_seq + 1`). Server-assigned, independent of `event_count`.
    pub scheduler_event_count: i64,
    /// Wall-clock time the most recent event for this build was ingested.
    /// Used by the reaper to detect abandoned builds. Not serialized.
    pub last_event_at: Instant,
}

pub type BepIndex = HashMap<String, BuildMeta>;

fn index_key(build_id: &str, invocation_id: &str) -> String {
    format!("{build_id}:{invocation_id}")
}

/// Mark a build `finished` in the index. Called when a build's event stream
/// terminates (clean close or disconnect) — the only signal a killed/crashed
/// bazel emits, since it never sends a terminal lifecycle event.
fn mark_finished(index: &Arc<RwLock<BepIndex>>, store: &Store, seen: Option<&(String, String)>) {
    if let Some((build_id, invocation_id)) = seen {
        let mut idx = index.write();
        if let Some(meta) = idx.get_mut(&index_key(build_id, invocation_id)) {
            meta.finished = true;
            persist_meta(store, meta);
        }
    }
}

/// Mark every unfinished build with no events since `now - idle` as finished
/// and persist it. Separated from the timer loop so it can be unit-tested
/// deterministically with a synthetic `now`.
pub fn reap_idle(index: &Arc<RwLock<BepIndex>>, store: &Store, idle: Duration, now: Instant) {
    let mut idx = index.write();
    for meta in idx.values_mut() {
        if !meta.finished && now.saturating_duration_since(meta.last_event_at) >= idle {
            meta.finished = true;
            persist_meta(store, meta);
        }
    }
}

/// Persist a build's metadata under `BepIndex:meta:{build}:{invocation}` so the
/// index can be rebuilt after a process restart. Fire-and-forget — the build
/// list is best-effort and a failed write just means one fewer build after a
/// restart.
fn persist_meta(store: &Store, meta: &BuildMeta) {
    let info = BuildInfo {
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
    };
    let mut buf = BytesMut::new();
    if let Err(e) = info.encode(&mut buf) {
        tracing::warn!("Could not encode BEP index meta: {e:?}");
        return;
    }
    let key = StoreKey::Str(Cow::Owned(format!(
        "BepIndex:meta:{}:{}",
        meta.build_id, meta.invocation_id
    )));
    let store = store.clone();
    let bytes = buf.freeze();
    background_spawn!("bep_persist_meta", async move {
        if let Err(e) = store.update_oneshot(key, bytes).await {
            tracing::warn!("Failed to persist BEP index meta: {e:?}");
        }
    });
}

/// Repopulate `index` from `BepIndex:meta:*` keys in an enumerable store.
/// Returns the number of builds loaded. Used on startup so the build list
/// survives a restart. Existing in-memory entries are not overwritten.
pub async fn rebuild_index_from_store(
    store: &Store,
    index: &Arc<RwLock<BepIndex>>,
) -> Result<u64, Error> {
    // Bounded range so an enumerable backend (e.g. redis SCAN) only walks the
    // meta keys, not the far larger BepEvent:be:* set. ';' is the byte after ':'.
    let start = StoreKey::Str(Cow::Borrowed("BepIndex:meta:"));
    let end = StoreKey::Str(Cow::Borrowed("BepIndex:meta;"));
    let mut keys: Vec<String> = Vec::new();
    store
        .list(start..end, |key| {
            if let StoreKey::Str(s) = key {
                keys.push(s.to_string());
            }
            true
        })
        .await
        .err_tip(|| "While listing BepIndex:meta keys")?;

    let mut loaded = 0u64;
    for key in keys {
        let Ok(bytes) = store
            .get_part_unchunked(StoreKey::Str(Cow::Owned(key)), 0, None)
            .await
        else {
            continue;
        };
        let Ok(info) = BuildInfo::decode(bytes.as_ref()) else {
            continue;
        };
        let meta = BuildMeta {
            build_id: info.build_id,
            invocation_id: info.invocation_id,
            identity: info.identity,
            start_time: info.start_time,
            finished: info.finished,
            command: info.command,
            event_count: info.event_count,
            task_id: info.task_id,
            task_name: info.task_name,
            scheduler_event_count: info.scheduler_event_count,
            last_event_at: Instant::now(),
        };
        let key = index_key(&meta.build_id, &meta.invocation_id);
        index.write().entry(key).or_insert(meta);
        loaded += 1;
    }
    Ok(loaded)
}

#[derive(Debug)]
pub struct BepServer {
    store: Store,
    event_tx: tokio::sync::broadcast::Sender<BepEventNotification>,
    index: Arc<RwLock<BepIndex>>,
    /// Held so the periodic reaper task is aborted when the server is dropped.
    _reaper: Option<JoinHandleDropGuard<()>>,
}

impl BepServer {
    pub fn new(
        config: &nativelink_config::cas_server::BepConfig,
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let store = store_manager
            .get_store(&config.store)
            .err_tip(|| format!("Expected store {} to exist in store manager", &config.store))?;

        let (event_tx, _) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
        let index = Arc::new(RwLock::new(HashMap::new()));

        // Reaper: periodically mark abandoned (no-events-for-a-while, never
        // finished) builds as finished so they don't spin in the UI forever.
        // Backstops the stream-end detection in publish_build_tool_event_stream
        // for cases where the transport never cleanly closes (half-open socket).
        let reaper = if config.reap_idle_seconds > 0 {
            let reaper_index = Arc::clone(&index);
            let reaper_store = store.clone();
            let idle = Duration::from_secs(u64::from(config.reap_idle_seconds));
            let tick = Duration::from_secs(u64::from(config.reap_interval_seconds.max(1)));
            Some(spawn!("bep_index_reaper", async move {
                let mut ticker = tokio::time::interval(tick);
                loop {
                    ticker.tick().await;
                    reap_idle(&reaper_index, &reaper_store, idle, Instant::now());
                }
            }))
        } else {
            None
        };

        // Rebuild the index from a persisted store on startup so the build list
        // survives a process restart. Runs in the background; until it
        // finishes, ListBuilds simply returns the builds loaded so far.
        if let Some(index_store_name) = config.index_store.as_ref() {
            let rebuild_store = store_manager.get_store(index_store_name).err_tip(|| {
                format!("Expected index_store {index_store_name} to exist in store manager")
            })?;
            let rebuild_index = Arc::clone(&index);
            background_spawn!("bep_index_rebuild", async move {
                match rebuild_index_from_store(&rebuild_store, &rebuild_index).await {
                    Ok(n) => tracing::info!("Rebuilt BEP build index with {n} build(s)"),
                    Err(e) => tracing::warn!("Failed to rebuild BEP build index: {e:?}"),
                }
            });
        }

        Ok(Self {
            store,
            event_tx,
            index,
            _reaper: reaper,
        })
    }

    pub fn into_service(self) -> PublishBuildEventServer<Self> {
        PublishBuildEventServer::new(self)
    }

    pub fn sender(&self) -> tokio::sync::broadcast::Sender<BepEventNotification> {
        self.event_tx.clone()
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<BepEventNotification> {
        self.event_tx.subscribe()
    }

    pub fn index(&self) -> Arc<RwLock<BepIndex>> {
        Arc::clone(&self.index)
    }

    pub fn store(&self) -> Store {
        self.store.clone()
    }

    fn notify_event(
        &self,
        build_id: &str,
        invocation_id: &str,
        sequence_number: i64,
        identity: &str,
        request: &PublishBuildToolEventStreamRequest,
    ) {
        let (bazel_event_bytes, timestamp) = extract_bazel_event_from_request(&request);

        {
            let mut idx = self.index.write();
            let key = index_key(build_id, invocation_id);
            let meta = idx.entry(key).or_insert_with(|| BuildMeta {
                build_id: build_id.to_string(),
                invocation_id: invocation_id.to_string(),
                identity: identity.to_string(),
                start_time: timestamp.clone(),
                finished: false,
                command: String::new(),
                event_count: 0,
                task_id: String::new(),
                task_name: String::new(),
                scheduler_event_count: 0,
                last_event_at: Instant::now(),
            });
            meta.event_count = sequence_number + 1;
            meta.last_event_at = Instant::now();
        }

        let _ = self.event_tx.send(BepEventNotification {
            build_id: build_id.to_string(),
            invocation_id: invocation_id.to_string(),
            payload: BepPayload::Bazel {
                sequence_number,
                bazel_event_bytes,
                timestamp,
            },
        });
    }

    fn notify_lifecycle(
        &self,
        build_id: &str,
        invocation_id: &str,
        request: &PublishLifecycleEventRequest,
    ) {
        let event_type = request
            .build_event
            .as_ref()
            .and_then(|obe| obe.event.as_ref())
            .and_then(|evt| {
                use nativelink_proto::google::devtools::build::v1::build_event::Event;
                match &evt.event {
                    Some(Event::InvocationAttemptFinished(_))
                    | Some(Event::BuildFinished(_)) => Some(true),
                    Some(Event::InvocationAttemptStarted(_)) => {
                        let mut idx = self.index.write();
                        let key = index_key(build_id, invocation_id);
                        let meta = idx.entry(key).or_insert_with(|| BuildMeta {
                            build_id: build_id.to_string(),
                            invocation_id: invocation_id.to_string(),
                            identity: String::new(),
                            start_time: evt.event_time.clone(),
                            finished: false,
                            command: String::new(),
                            event_count: 0,
                            task_id: String::new(),
                            task_name: String::new(),
                            scheduler_event_count: 0,
                            last_event_at: Instant::now(),
                        });
                        meta.start_time = evt.event_time.clone();
                        None
                    }
                    _ => None,
                }
            });

        if let Some(true) = event_type {
            let mut idx = self.index.write();
            let key = index_key(build_id, invocation_id);
            if let Some(meta) = idx.get_mut(&key) {
                meta.finished = true;
                persist_meta(&self.store, meta);
            }
        }
    }

    async fn inner_publish_lifecycle_event(
        &self,
        request: PublishLifecycleEventRequest,
        identity: Option<String>,
    ) -> Result<Response<()>, Error> {
        let build_event = request
            .build_event
            .as_ref()
            .err_tip(|| "Expected build_event to be set")?;
        let stream_id = build_event
            .stream_id
            .as_ref()
            .err_tip(|| "Expected stream_id to be set")?;

        let sequence_number = build_event.sequence_number;

        let store_key = StoreKey::Str(Cow::Owned(format!(
            "BepEvent:le:{}:{}:{}",
            &stream_id.build_id, &stream_id.invocation_id, sequence_number,
        )));

        let bep_event = BepEvent {
            version: BEP_EVENT_VERSION,
            identity: identity.unwrap_or_default(),
            event: Some(bep_event::Event::LifecycleEvent(request.clone())),
        };
        let mut buf = BytesMut::new();
        bep_event
            .encode(&mut buf)
            .err_tip(|| "Could not encode PublishLifecycleEventRequest proto")?;

        self.store
            .update_oneshot(store_key.clone(), buf.freeze())
            .await
            .err_tip(|| format!("Failed to store PublishLifecycleEventRequest for {store_key}",))?;

        self.notify_lifecycle(
            &stream_id.build_id,
            &stream_id.invocation_id,
            &request,
        );

        Ok(Response::new(()))
    }

    async fn inner_publish_build_tool_event_stream(
        &self,
        stream: Streaming<PublishBuildToolEventStreamRequest>,
        identity: Option<String>,
    ) -> Result<Response<PublishBuildToolEventStreamStream>, Error> {
        async fn process_request(
            store: Pin<&dyn StoreDriver>,
            request: PublishBuildToolEventStreamRequest,
            identity: String,
            event_tx: &tokio::sync::broadcast::Sender<BepEventNotification>,
            index: &Arc<RwLock<BepIndex>>,
        ) -> Result<PublishBuildToolEventStreamResponse, Status> {
            let ordered_build_event = request
                .ordered_build_event
                .as_ref()
                .err_tip(|| "Expected ordered_build_event to be set")?;
            let stream_id = ordered_build_event
                .stream_id
                .as_ref()
                .err_tip(|| "Expected stream_id to be set")?
                .clone();

            let sequence_number = ordered_build_event.sequence_number;

            let bep_event = BepEvent {
                version: BEP_EVENT_VERSION,
                identity: identity.clone(),
                event: Some(bep_event::Event::BuildToolEvent(request.clone())),
            };
            let mut buf = BytesMut::new();

            bep_event
                .encode(&mut buf)
                .err_tip(|| "Could not encode PublishBuildToolEventStreamRequest proto")?;

            store
                .update_oneshot(
                    StoreKey::Str(Cow::Owned(format!(
                        "BepEvent:be:{}:{}:{}",
                        &stream_id.build_id, &stream_id.invocation_id, sequence_number,
                    ))),
                    buf.freeze(),
                )
                .await
                .err_tip(|| "Failed to store PublishBuildToolEventStreamRequest")?;

            let (bazel_event_bytes, timestamp) = extract_bazel_event_from_request(&request);

            // The BuildMetadata BEP event carries Aspect's task id/name; capture
            // them so the build list can show a friendly label instead of the
            // invocation hash. Only that payload has them; others decode to None.
            let aspect_metadata =
                build_event_stream::BuildEvent::decode(bazel_event_bytes.as_ref())
                    .ok()
                    .and_then(|event| match event.payload {
                        Some(build_event::Payload::BuildMetadata(m)) => Some(m.metadata),
                        _ => None,
                    });

            {
                let mut idx = index.write();
                let key = index_key(&stream_id.build_id, &stream_id.invocation_id);
                let meta = idx.entry(key).or_insert_with(|| BuildMeta {
                    build_id: stream_id.build_id.clone(),
                    invocation_id: stream_id.invocation_id.clone(),
                    identity: identity.clone(),
                    start_time: timestamp.clone(),
                    finished: false,
                    command: String::new(),
                    event_count: 0,
                    task_id: String::new(),
                    task_name: String::new(),
                    scheduler_event_count: 0,
                    last_event_at: Instant::now(),
                });
                meta.event_count = sequence_number + 1;
                meta.last_event_at = Instant::now();
                if let Some(metadata) = aspect_metadata {
                    if let Some(id) = metadata.get("ASPECT_TASK_ID") {
                        meta.task_id = id.clone();
                    }
                    if let Some(name) = metadata.get("ASPECT_TASK_NAME") {
                        meta.task_name = name.clone();
                    }
                    // A Claude Code session tags its builds with
                    // `--build_metadata=CLAUDE_CODE_SESSION_ID=...` so the
                    // claude-statusline can filter ListBuilds to its own builds.
                    // Surface it through the existing `identity` field (no proto
                    // change). The OTel-baggage identity isn't populated for BES
                    // streams, so this is the authoritative source when present.
                    if let Some(session) = metadata.get("CLAUDE_CODE_SESSION_ID") {
                        meta.identity = session.clone();
                    }
                }
            }

            let _ = event_tx.send(BepEventNotification {
                build_id: stream_id.build_id.clone(),
                invocation_id: stream_id.invocation_id.clone(),
                payload: BepPayload::Bazel {
                    sequence_number,
                    bazel_event_bytes,
                    timestamp,
                },
            });

            Ok(PublishBuildToolEventStreamResponse {
                stream_id: Some(stream_id.clone()),
                sequence_number,
            })
        }

        struct State {
            store: Store,
            stream: Streaming<PublishBuildToolEventStreamRequest>,
            identity: String,
            event_tx: tokio::sync::broadcast::Sender<BepEventNotification>,
            index: Arc<RwLock<BepIndex>>,
            // (build_id, invocation_id) of this stream, captured from the first
            // event so we can mark the build finished when the stream ends.
            seen: Option<(String, String)>,
        }

        let response_stream =
            unfold(
                Some(State {
                    store: self.store.clone(),
                    stream,
                    identity: identity.unwrap_or_default(),
                    event_tx: self.event_tx.clone(),
                    index: Arc::clone(&self.index),
                    seen: None,
                }),
                move |maybe_state| async move {
                    let mut state = maybe_state?;
                    let request =
                        match state.stream.message().await.err_tip(
                            || "While receiving message in publish_build_tool_event_stream",
                        ) {
                            Ok(Some(request)) => request,
                            // Stream closed cleanly or errored (client crash /
                            // Ctrl-C). Either way the build is over — mark it
                            // finished so it doesn't stay "active" forever.
                            Ok(None) => {
                                mark_finished(&state.index, &state.store, state.seen.as_ref());
                                return None;
                            }
                            Err(e) => {
                                mark_finished(&state.index, &state.store, state.seen.as_ref());
                                return Some((Err(e.into()), None));
                            }
                        };
                    // Remember which build this stream belongs to.
                    if let Some(stream_id) = request
                        .ordered_build_event
                        .as_ref()
                        .and_then(|obe| obe.stream_id.as_ref())
                    {
                        state.seen = Some((
                            stream_id.build_id.clone(),
                            stream_id.invocation_id.clone(),
                        ));
                    }
                    process_request(
                        state.store.as_store_driver_pin(),
                        request,
                        state.identity.clone(),
                        &state.event_tx,
                        &state.index,
                    )
                    .await
                    .map_or_else(
                        |e| Some((Err(e), None)),
                        |response| Some((Ok(response), Some(state))),
                    )
                },
            );

        Ok(Response::new(Box::pin(response_stream)))
    }
}

type PublishBuildToolEventStreamStream = Pin<
    Box<dyn Stream<Item = Result<PublishBuildToolEventStreamResponse, Status>> + Send + 'static>,
>;

#[tonic::async_trait]
impl PublishBuildEvent for BepServer {
    type PublishBuildToolEventStreamStream = PublishBuildToolEventStreamStream;

    #[instrument(
        err,
        ret(level = Level::INFO),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn publish_lifecycle_event(
        &self,
        grpc_request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        self.inner_publish_lifecycle_event(grpc_request.into_inner(), get_identity())
            .await
            .map_err(Error::into)
    }

    #[instrument(
      err,
      level = Level::ERROR,
      skip_all,
      fields(request = ?grpc_request.get_ref())
    )]
    async fn publish_build_tool_event_stream(
        &self,
        grpc_request: Request<Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
        self.inner_publish_build_tool_event_stream(grpc_request.into_inner(), get_identity())
            .await
            .map_err(Error::into)
    }
}

/// Publish a scheduler operation event into the BEP pipeline under the `se:`
/// namespace, given the same three handles the `BepServer` exposes via
/// `index()` / `store()` / `sender()`. Assigns the next per-build scheduler
/// sequence, persists the encoded `SchedulerEvent` so it replays for finished
/// builds, and broadcasts it live. Used by the scheduler-event bridge — a free
/// function (not a `BepServer` method) because `BepServer` is consumed by
/// `into_service()` before the bridge is spawned. Mirrors the BES ingest path
/// but with a server-assigned counter independent of Bazel's `be:` sequence.
pub async fn publish_scheduler_event(
    store: &Store,
    index: &Arc<RwLock<BepIndex>>,
    event_tx: &tokio::sync::broadcast::Sender<BepEventNotification>,
    build_id: &str,
    invocation_id: &str,
    event: SchedulerEvent,
) {
    // Assign the next se: sequence under the index write lock. `or_insert_with`
    // creates the BuildMeta if a scheduler event arrives before the first BES
    // event (the build then shows in ListBuilds with event_count 0).
    let sched_seq = {
        let mut idx = index.write();
        let key = index_key(build_id, invocation_id);
        let meta = idx.entry(key).or_insert_with(|| BuildMeta {
            build_id: build_id.to_string(),
            invocation_id: invocation_id.to_string(),
            identity: String::new(),
            start_time: None,
            finished: false,
            command: String::new(),
            event_count: 0,
            task_id: String::new(),
            task_name: String::new(),
            scheduler_event_count: 0,
            last_event_at: Instant::now(),
        });
        let seq = meta.scheduler_event_count;
        meta.scheduler_event_count = seq + 1;
        meta.last_event_at = Instant::now();
        seq
    };

    let mut buf = BytesMut::new();
    if let Err(e) = event.encode(&mut buf) {
        tracing::warn!("Could not encode SchedulerEvent: {e:?}");
        return;
    }
    let key = StoreKey::Str(Cow::Owned(format!(
        "BepEvent:se:{build_id}:{invocation_id}:{sched_seq}",
    )));
    if let Err(e) = store.update_oneshot(key, buf.freeze()).await {
        tracing::warn!("Failed to store SchedulerEvent: {e:?}");
        return;
    }

    let _ = event_tx.send(BepEventNotification {
        build_id: build_id.to_string(),
        invocation_id: invocation_id.to_string(),
        payload: BepPayload::Scheduler { sched_seq, event },
    });
}

pub fn extract_bazel_event_from_request(
    request: &PublishBuildToolEventStreamRequest,
) -> (bytes::Bytes, Option<::prost_types::Timestamp>) {
    use nativelink_proto::google::devtools::build::v1::build_event::Event;

    let obe = match request.ordered_build_event.as_ref() {
        Some(obe) => obe,
        None => return (bytes::Bytes::new(), None),
    };
    let evt = match obe.event.as_ref() {
        Some(evt) => evt,
        None => return (bytes::Bytes::new(), None),
    };
    let timestamp = evt.event_time.clone();
    let bazel_bytes = match &evt.event {
        Some(Event::BazelEvent(any)) => bytes::Bytes::from(any.value.clone()),
        _ => bytes::Bytes::new(),
    };
    (bazel_bytes, timestamp)
}
