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

use core::hash::{Hash, Hasher};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_error::{Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectionResult, StartExecute, UpdateForWorker, update_for_worker,
};
use nativelink_util::action_messages::{ActionInfo, OperationId, WorkerId};
use nativelink_util::metrics_utils::{AsyncCounterWrapper, CounterWithTime, FuncCounterWrapper};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use tokio::sync::mpsc::UnboundedSender;

pub type WorkerTimestamp = u64;

/// Monotonically-increasing identifier minted by the scheduler each time a
/// `Worker` is added to the pool. Reservations capture the generation of the
/// worker they were issued against so that a reservation held across a
/// worker reconnect (which replaces the `Worker` in the pool under the same
/// `WorkerId`) can detect staleness at commit time and refuse to apply.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerGeneration(pub u64);

/// Represents the action info and the platform properties of the action.
/// These platform properties have the type of the properties as well as
/// the value of the properties, unlike `ActionInfo`, which only has the
/// string value of the properties.
#[derive(Clone, Debug, MetricsComponent)]
pub struct ActionInfoWithProps {
    /// The action info of the action.
    #[metric(group = "action_info")]
    pub inner: Arc<ActionInfo>,
    /// The platform properties of the action.
    #[metric(group = "platform_properties")]
    pub platform_properties: PlatformProperties,
    /// Origin metadata used when publishing scheduler-side telemetry for this action.
    pub origin_metadata: OriginMetadata,
    /// `OriginEvent` id for the `scheduler_start_execute` request.
    pub scheduler_start_execute_event_id: Option<String>,
}

/// Notifications to send worker about a requested state change.
#[derive(Debug)]
pub enum WorkerUpdate {
    /// Requests that the worker begin executing this action.
    RunAction(Box<(OperationId, ActionInfoWithProps)>),

    /// Request that the worker is no longer in the pool and may discard any jobs.
    Disconnect,
}

#[derive(Debug, MetricsComponent)]
pub struct PendingActionInfoData {
    #[metric]
    pub action_info: ActionInfoWithProps,
}

/// Represents a connection to a worker and used as the medium to
/// interact with the worker from the client/scheduler.
#[derive(Debug, MetricsComponent)]
pub struct Worker {
    /// Unique identifier of the worker.
    #[metric(help = "The unique identifier of the worker.")]
    pub id: WorkerId,

    /// Properties that describe the capabilities of this worker.
    #[metric(group = "platform_properties")]
    pub platform_properties: PlatformProperties,

    /// Channel to send commands from scheduler to worker.
    pub tx: UnboundedSender<UpdateForWorker>,

    /// The action info of the running actions on the worker.
    #[metric(group = "running_action_infos")]
    pub running_action_infos: HashMap<OperationId, PendingActionInfoData>,

    /// If the properties were restored already then it's added to this set.
    pub restored_platform_properties: HashSet<OperationId>,

    /// Timestamp of last time this worker had been communicated with.
    // Warning: Do not update this timestamp without updating the placement of the worker in
    // the LRUCache in the Workers struct.
    #[metric(help = "Last time this worker was communicated with.")]
    pub last_update_timestamp: WorkerTimestamp,

    /// Whether the worker rejected the last action due to back pressure.
    #[metric(help = "If the worker is paused.")]
    pub is_paused: bool,

    /// Whether the worker is draining.
    #[metric(help = "If the worker is draining.")]
    pub is_draining: bool,

    /// Maximum inflight tasks for this worker (or 0 for unlimited)
    #[metric(help = "Maximum inflight tasks for this worker (or 0 for unlimited)")]
    pub max_inflight_tasks: u64,

    /// Generation tag assigned by the scheduler at `add_worker` time. Used to
    /// fence reservations against worker reconnects (see `WorkerGeneration`).
    /// Defaults to `WorkerGeneration(0)` at construction; overwritten by the
    /// scheduler before the worker enters the pool.
    generation: WorkerGeneration,

    /// Number of reservations issued against this worker that have not yet
    /// been committed or released. Included in `can_accept_work` so two
    /// concurrent matches against the same worker cannot both pass the
    /// inflight-capacity check before either has entered `running_action_infos`.
    pending_action_count: usize,

    /// Stats about the worker.
    #[metric]
    metrics: Arc<Metrics>,
}

fn send_msg_to_worker(
    tx: &UnboundedSender<UpdateForWorker>,
    msg: update_for_worker::Update,
) -> Result<(), Error> {
    tx.send(UpdateForWorker { update: Some(msg) })
        .map_err(|err| Error::from_std_err(Code::Internal, &err).append("Worker disconnected"))
}

impl Worker {
    pub fn new(
        id: WorkerId,
        platform_properties: PlatformProperties,
        tx: UnboundedSender<UpdateForWorker>,
        timestamp: WorkerTimestamp,
        max_inflight_tasks: u64,
    ) -> Self {
        Self {
            id,
            platform_properties,
            tx,
            running_action_infos: HashMap::new(),
            restored_platform_properties: HashSet::new(),
            last_update_timestamp: timestamp,
            is_paused: false,
            is_draining: false,
            max_inflight_tasks,
            generation: WorkerGeneration(0),
            pending_action_count: 0,
            metrics: Arc::new(Metrics {
                connected_timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                actions_completed: CounterWithTime::default(),
                run_action: AsyncCounterWrapper::default(),
                keep_alive: FuncCounterWrapper::default(),
                notify_disconnect: CounterWithTime::default(),
            }),
        }
    }

    /// Sends the initial connection information to the worker. This generally is just meta info.
    /// This should only be sent once and should always be the first item in the stream.
    pub fn send_initial_connection_result(&mut self) -> Result<(), Error> {
        send_msg_to_worker(
            &self.tx,
            update_for_worker::Update::ConnectionResult(ConnectionResult {
                worker_id: self.id.clone().into(),
            }),
        )
        .err_tip(|| format!("Failed to send ConnectionResult to worker : {}", self.id))
    }

    /// Notifies the worker of a requested state change.
    pub async fn notify_update(&mut self, worker_update: WorkerUpdate) -> Result<(), Error> {
        match worker_update {
            WorkerUpdate::RunAction(action) => {
                let (operation_id, action_info) = *action;
                self.run_action(operation_id, action_info).await
            }
            WorkerUpdate::Disconnect => {
                self.metrics.notify_disconnect.inc();
                send_msg_to_worker(&self.tx, update_for_worker::Update::Disconnect(()))
            }
        }
    }

    pub fn keep_alive(&mut self) -> Result<(), Error> {
        let tx = &mut self.tx;
        let id = &self.id;
        self.metrics.keep_alive.wrap(move || {
            send_msg_to_worker(tx, update_for_worker::Update::KeepAlive(()))
                .err_tip(|| format!("Failed to send KeepAlive to worker : {id}"))
        })
    }

    async fn run_action(
        &mut self,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> Result<(), Error> {
        // Legacy one-shot path: reserve budget + finalize in a single step.
        // Retained for callers outside the matcher (e.g. tests, health paths)
        // that do not need the reserve/commit/release split.
        let _debits = self.reserve_budget(&action_info.platform_properties);
        self.finalize_run(operation_id, action_info).await
    }

    /// Debit the worker's `Minimum` budget for a pending match and bump the
    /// pending counter. Returns the list of debits so they can be restored
    /// via `restore_budget` if the match never commits.
    ///
    /// Must only be called by the scheduler holding the pool lock — the
    /// returned debits are consumed asymmetrically by either `finalize_run`
    /// (which leaves the budget debited because the action is now running)
    /// or `restore_budget` (which refunds the budget on rollback).
    pub(crate) fn reserve_budget(
        &mut self,
        action_props: &PlatformProperties,
    ) -> Vec<(String, PlatformPropertyValue)> {
        debug_assert!(action_props.is_satisfied_by(&self.platform_properties, false));
        let mut debits: Vec<(String, PlatformPropertyValue)> = Vec::new();
        for (property, prop_value) in &action_props.properties {
            if let PlatformPropertyValue::Minimum(value) = prop_value {
                let worker_props = &mut self.platform_properties.properties;
                if let Some(PlatformPropertyValue::Minimum(worker_value)) =
                    worker_props.get_mut(property)
                {
                    *worker_value -= value;
                    debits.push((property.clone(), PlatformPropertyValue::Minimum(*value)));
                }
            }
        }
        self.pending_action_count += 1;
        debits
    }

    /// Inverse of `reserve_budget`: add the debited values back to the worker
    /// and decrement the pending counter. Invoked on reservation release
    /// (explicit `release_reservation` from the matcher, or Drop-triggered
    /// cleanup via the release channel).
    pub(crate) fn restore_budget(&mut self, debits: &[(String, PlatformPropertyValue)]) {
        for (property, prop_value) in debits {
            if let PlatformPropertyValue::Minimum(value) = prop_value {
                let worker_props = &mut self.platform_properties.properties;
                if let Some(PlatformPropertyValue::Minimum(worker_value)) =
                    worker_props.get_mut(property)
                {
                    *worker_value += value;
                }
            }
        }
        self.pending_action_count = self.pending_action_count.saturating_sub(1);
    }

    /// Commit a previously-reserved match onto the worker: insert into
    /// `running_action_infos`, decrement the pending counter, and send
    /// `StartAction` to the worker process. Does NOT re-debit platform
    /// properties — `reserve_budget` already did that.
    ///
    /// Combined (state + send) path. Preserved for the legacy one-shot
    /// `Worker::run_action` entry. The matcher's reserve → commit path
    /// uses the split `finalize_run_state_only` + `FinalizedRun::send`
    /// pair so the `tx.send` can run outside the pool mutex.
    pub(crate) async fn finalize_run(
        &mut self,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> Result<(), Error> {
        self.finalize_run_state_only(operation_id, action_info)
            .send()
    }

    /// State-only half of `finalize_run`: inserts the op into
    /// `running_action_infos`, decrements `pending_action_count`, and
    /// prepares the `StartAction` payload. Returns a `FinalizedRun` handle
    /// whose `.send()` fires the worker notification via an unbounded
    /// channel (non-blocking). The state mutation MUST happen under the
    /// pool lock (for atomicity vs. `add_worker`/`remove_worker` and the
    /// generation fence); `.send()` can safely run without it.
    pub(crate) fn finalize_run_state_only(
        &mut self,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> FinalizedRun {
        let worker_id = self.id.clone().into();
        let start_execute = StartExecute {
            execute_request: Some(action_info.inner.as_ref().into()),
            operation_id: operation_id.to_string(),
            queued_timestamp: Some(action_info.inner.insert_timestamp.into()),
            platform: Some((&action_info.platform_properties).into()),
            worker_id,
        };
        self.running_action_infos
            .insert(operation_id, PendingActionInfoData { action_info });
        self.pending_action_count = self.pending_action_count.saturating_sub(1);
        FinalizedRun {
            tx: self.tx.clone(),
            payload: update_for_worker::Update::StartAction(start_execute),
            metrics: Arc::clone(&self.metrics),
        }
    }

    pub(crate) fn generation(&self) -> WorkerGeneration {
        self.generation
    }

    pub(crate) fn set_generation(&mut self, generation: WorkerGeneration) {
        self.generation = generation;
    }

    pub(crate) fn pending_action_count(&self) -> usize {
        self.pending_action_count
    }

    pub fn pending_action_count_for_test(&self) -> usize {
        self.pending_action_count
    }

    /// Force-sets `pending_action_count`. Tests use this to simulate the
    /// production leak (channel-saturated Drops that pre-fix never decremented
    /// the counter) so the matcher safety net can be exercised against a
    /// stuck `can_accept_work() == false` state without needing 256 real
    /// reservation churns.
    pub fn set_pending_action_count_for_test(&mut self, count: usize) {
        self.pending_action_count = count;
    }

    pub(crate) fn execution_complete(&mut self, operation_id: &OperationId) {
        if let Some((operation_id, pending_action_info)) =
            self.running_action_infos.remove_entry(operation_id)
        {
            self.restored_platform_properties
                .insert(operation_id.clone());
            self.restore_platform_properties(&pending_action_info.action_info.platform_properties);
            self.running_action_infos
                .insert(operation_id, pending_action_info);
        }
    }

    pub(crate) async fn complete_action(
        &mut self,
        operation_id: &OperationId,
    ) -> Result<(), Error> {
        let pending_action_info = self.running_action_infos.remove(operation_id).err_tip(|| {
            format!(
                "Worker {} tried to complete operation {} that was not running",
                self.id, operation_id
            )
        })?;
        if !self.restored_platform_properties.remove(operation_id) {
            self.restore_platform_properties(&pending_action_info.action_info.platform_properties);
        }
        self.is_paused = false;
        self.metrics.actions_completed.inc();
        Ok(())
    }

    pub fn has_actions(&self) -> bool {
        !self.running_action_infos.is_empty()
    }

    fn restore_platform_properties(&mut self, props: &PlatformProperties) {
        for (property, prop_value) in &props.properties {
            if let PlatformPropertyValue::Minimum(value) = prop_value {
                let worker_props = &mut self.platform_properties.properties;
                if let PlatformPropertyValue::Minimum(worker_value) =
                    worker_props.get_mut(property).unwrap()
                {
                    *worker_value += value;
                }
            }
        }
    }

    pub fn can_accept_work(&self) -> bool {
        !self.is_paused
            && !self.is_draining
            && (self.max_inflight_tasks == 0
                || u64::try_from(self.running_action_infos.len() + self.pending_action_count)
                    .unwrap_or(u64::MAX)
                    < self.max_inflight_tasks)
    }
}

/// Deferred half of `Worker::finalize_run`: carries the cloned worker
/// sender + the `StartAction` payload produced under the pool lock. The
/// owner calls `.send()` AFTER dropping the pool lock so the worker-
/// dispatch notification doesn't contend with the mutex.
///
/// `tx` is a cheap `Arc` clone (`tokio::mpsc::UnboundedSender`); `send`
/// is non-blocking and fails only if the worker's receive side has been
/// dropped.
pub(crate) struct FinalizedRun {
    tx: UnboundedSender<UpdateForWorker>,
    payload: update_for_worker::Update,
    metrics: Arc<Metrics>,
}

impl FinalizedRun {
    /// Dispatch `StartAction` to the worker. Must be called exactly once
    /// per `finalize_run_state_only` return value. Counts success/failure
    /// via the same `run_action` metric wrapper as the combined path.
    pub(crate) fn send(self) -> Result<(), Error> {
        self.metrics
            .run_action
            .wrap_fn(|| send_msg_to_worker(&self.tx, self.payload))
    }
}

impl PartialEq for Worker {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Worker {}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Debug, Default, MetricsComponent)]
struct Metrics {
    #[metric(help = "The timestamp of when this worker connected.")]
    connected_timestamp: u64,
    #[metric(help = "The number of actions completed for this worker.")]
    actions_completed: CounterWithTime,
    #[metric(help = "The number of actions started for this worker.")]
    run_action: AsyncCounterWrapper,
    #[metric(help = "The number of keep_alive sent to this worker.")]
    keep_alive: FuncCounterWrapper,
    #[metric(help = "The number of notify_disconnect sent to this worker.")]
    notify_disconnect: CounterWithTime,
}
