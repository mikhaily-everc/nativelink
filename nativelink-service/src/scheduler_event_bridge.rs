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

//! Bridges scheduler operation state into the BEP pipeline so the WatchBuild UI
//! can overlay remote-execution data (worker, stage, queue/exec timing) onto the
//! build it is already streaming.
//!
//! Design: a single supervisor task periodically lists all awaited actions
//! (`ClientStateManager::filter_operations` with the default Any filter) to
//! discover operations created since the last tick. For each newly-seen
//! operation it spawns a watcher task that streams the operation's transitions
//! via `ActionStateResult::changed()` and emits a `SchedulerEvent` per real
//! stage advance. Each operation is correlated to its build via the REv2
//! `RequestMetadata` captured on the Execute call (Phase 1): the
//! `correlated_invocations_id` is the BES `build_id` and the `tool_invocation_id`
//! is the BES `invocation_id`, so events land on the exact build the UI watches.
//!
//! Coverage note: cache-hit operations resolve in the cache-lookup layer without
//! entering the awaited-action DB, so they are not seen here. The UI infers
//! "cached" for a target that completes with no scheduler events — no event is
//! needed for them.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use nativelink_proto::com::github::trace_machina::nativelink::bep::{
    ExecutionStage, SchedulerEvent,
};
use nativelink_util::action_messages::{ActionStage, ActionState};
use nativelink_util::operation_state_manager::{
    ActionStateResult, ClientStateManager, OperationFilter,
};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::spawn;
use nativelink_util::store_trait::Store;
use nativelink_util::task::JoinHandleDropGuard;
use parking_lot::RwLock;
use tokio::sync::broadcast::Sender;

use crate::bep_server::{BepEventNotification, BepIndex, publish_scheduler_event};

/// An operation no longer seen in the filter results for this many consecutive
/// polls is pruned from the per-op stage map. Generously past the scheduler's
/// completed-retention window (120s at the default 3s poll = ~40 polls) so a
/// transient single-poll miss never drops a still-live op and re-emits it.
const PRUNE_AFTER_MISSING_POLLS: u64 = 80;

/// The three BEP-pipeline handles needed to publish an event. All cheap
/// `Arc`/`Sender`/`Store` clones.
#[derive(Clone)]
struct Handles {
    store: Store,
    index: Arc<RwLock<BepIndex>>,
    event_tx: Sender<BepEventNotification>,
}

/// Owns the supervisor task; dropping aborts it.
#[derive(Debug)]
pub struct SchedulerEventBridge {
    _supervisor: JoinHandleDropGuard<()>,
}

impl SchedulerEventBridge {
    pub fn new(
        schedulers: Vec<Arc<dyn ClientStateManager>>,
        store: Store,
        index: Arc<RwLock<BepIndex>>,
        event_tx: Sender<BepEventNotification>,
        poll_interval: Duration,
    ) -> Self {
        let handles = Handles {
            store,
            index,
            event_tx,
        };
        let supervisor = spawn!("scheduler_event_bridge", async move {
            supervisor_loop(schedulers, handles, poll_interval).await;
        });
        Self {
            _supervisor: supervisor,
        }
    }
}

/// Per-operation tracking: the last stage we emitted and the last poll number we
/// observed the op (for pruning aged-out ops).
struct OpState {
    last_stage: ExecutionStage,
    last_seen_poll: u64,
}

async fn supervisor_loop(
    schedulers: Vec<Arc<dyn ClientStateManager>>,
    handles: Handles,
    poll_interval: Duration,
) {
    // Single-task, race-free design: each poll lists all awaited actions and
    // emits a SchedulerEvent for any op whose stage advanced since we last saw
    // it. No concurrent per-op watchers (which could double-emit on re-discovery).
    // Sub-poll-interval transitions are collapsed, but the terminal Completed
    // event carries the full ExecutionMetadata timestamps so the overlay stays
    // complete; intermediate stages stream at poll resolution for longer ops.
    let mut ops: HashMap<String, OpState> = HashMap::new();
    let mut poll: u64 = 0;
    let mut ticker = tokio::time::interval(poll_interval);
    loop {
        ticker.tick().await;
        poll += 1;
        for scheduler in &schedulers {
            let mut stream = match scheduler.filter_operations(OperationFilter::default()).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::debug!("scheduler_event_bridge: filter_operations failed: {e:?}");
                    continue;
                }
            };
            while let Some(result) = stream.next().await {
                let (state, origin) = match result.as_state().await {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let op_id = state.client_operation_id.to_string();
                let stage = execution_stage(&state.stage);
                let advanced = ops.get(&op_id).map(|o| o.last_stage) != Some(stage);
                if advanced {
                    if let Some((build_id, invocation_id)) = correlate(origin.as_ref()) {
                        let event = build_scheduler_event(&state, origin.as_ref(), stage);
                        publish_scheduler_event(
                            &handles.store,
                            &handles.index,
                            &handles.event_tx,
                            &build_id,
                            &invocation_id,
                            event,
                        )
                        .await;
                    }
                }
                ops.insert(
                    op_id,
                    OpState {
                        last_stage: stage,
                        last_seen_poll: poll,
                    },
                );
            }
        }
        // Prune ops not seen for a while (aged out of the scheduler's window).
        ops.retain(|_, o| poll.saturating_sub(o.last_seen_poll) < PRUNE_AFTER_MISSING_POLLS);
    }
}

/// Map an operation to its BES `(build_id, invocation_id)` via the captured
/// RequestMetadata. Returns `None` for operations with no/empty metadata (they
/// can't be attributed to a build and are dropped).
fn correlate(origin: Option<&OriginMetadata>) -> Option<(String, String)> {
    let metadata = origin?.bazel_metadata.as_ref()?;
    if metadata.correlated_invocations_id.is_empty() {
        return None;
    }
    Some((
        metadata.correlated_invocations_id.clone(),
        metadata.tool_invocation_id.clone(),
    ))
}

fn build_scheduler_event(
    state: &ActionState,
    origin: Option<&OriginMetadata>,
    stage: ExecutionStage,
) -> SchedulerEvent {
    let metadata = origin.and_then(|o| o.bazel_metadata.as_ref());
    let mut event = SchedulerEvent {
        client_operation_id: state.client_operation_id.to_string(),
        action_digest: state.action_digest.to_string(),
        target_id: metadata.map(|m| m.target_id.clone()).unwrap_or_default(),
        action_mnemonic: metadata
            .map(|m| m.action_mnemonic.clone())
            .unwrap_or_default(),
        action_id: metadata.map(|m| m.action_id.clone()).unwrap_or_default(),
        stage: stage as i32,
        transition_time: Some(state.last_transition_timestamp.into()),
        worker_id: String::new(),
        queued_timestamp: None,
        worker_start_timestamp: None,
        worker_completed_timestamp: None,
        exit_code: 0,
        cached: false,
    };
    match &state.stage {
        ActionStage::Completed(result) => {
            let metadata = &result.execution_metadata;
            event.worker_id = metadata.worker.clone();
            event.queued_timestamp = Some(metadata.queued_timestamp.into());
            event.worker_start_timestamp = Some(metadata.worker_start_timestamp.into());
            event.worker_completed_timestamp = Some(metadata.worker_completed_timestamp.into());
            event.exit_code = result.exit_code;
        }
        ActionStage::CompletedFromCache(proto) => {
            event.cached = true;
            event.exit_code = proto.exit_code;
            if let Some(metadata) = &proto.execution_metadata {
                event.worker_id = metadata.worker.clone();
                event.queued_timestamp = metadata.queued_timestamp.clone();
                event.worker_start_timestamp = metadata.worker_start_timestamp.clone();
                event.worker_completed_timestamp = metadata.worker_completed_timestamp.clone();
            }
        }
        _ => {}
    }
    event
}

fn execution_stage(stage: &ActionStage) -> ExecutionStage {
    match stage {
        ActionStage::Unknown => ExecutionStage::Unknown,
        ActionStage::CacheCheck => ExecutionStage::CacheCheck,
        ActionStage::Queued => ExecutionStage::Queued,
        ActionStage::Executing => ExecutionStage::Executing,
        ActionStage::Completed(_) => ExecutionStage::Completed,
        ActionStage::CompletedFromCache(_) => ExecutionStage::CompletedFromCache,
    }
}
