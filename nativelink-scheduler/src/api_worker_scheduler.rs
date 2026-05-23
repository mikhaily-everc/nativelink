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

use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::sync::{Arc, Weak};
use std::time::{Instant, UNIX_EPOCH};

use async_lock::Mutex;
use lru::LruCache;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Code, Error, ResultExt, error_if, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
    RootMetricsComponent, group,
};
use nativelink_util::action_messages::{OperationId, WorkerId};
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::sync::{Notify, mpsc};
use tokio::sync::mpsc::error::TrySendError;
use tonic::async_trait;
use tracing::{error, info, trace, warn};

/// Metrics for tracking scheduler performance.
#[derive(Debug, Default)]
pub struct SchedulerMetrics {
    /// Total number of worker additions.
    pub workers_added: AtomicU64,
    /// Total number of worker removals.
    pub workers_removed: AtomicU64,
    /// Total number of `find_worker_for_action` calls.
    pub find_worker_calls: AtomicU64,
    /// Total number of successful worker matches.
    pub find_worker_hits: AtomicU64,
    /// Total number of failed worker matches (no worker found).
    pub find_worker_misses: AtomicU64,
    /// Total time spent in `find_worker_for_action` (nanoseconds).
    pub find_worker_time_ns: AtomicU64,
    /// Total number of workers iterated during find operations.
    pub workers_iterated: AtomicU64,
    /// Total number of successful action dispatches (post `commit_reservation`).
    pub actions_dispatched: AtomicU64,
    /// Total number of keep-alive updates.
    pub keep_alive_updates: AtomicU64,
    /// Total number of worker timeouts.
    pub worker_timeouts: AtomicU64,
    /// Total number of worker reservations successfully created.
    pub reservations_created: AtomicU64,
    /// Total number of reservations that committed to a running action.
    pub reservations_committed: AtomicU64,
    /// Total number of reservations released (explicit release + Drop-enqueued
    /// cleanup processed by the releaser task).
    pub reservations_released: AtomicU64,
    /// Total number of `commit_reservation` calls that failed (generation
    /// mismatch, worker eviction, or send failure during finalize).
    pub reservation_commit_failures: AtomicU64,
    /// Subset of `reservation_commit_failures` caused by a worker being
    /// replaced (reconnected) between reserve and commit.
    pub reservation_generation_mismatches: AtomicU64,
    /// Drop-time enqueue attempts that failed because the bounded release
    /// channel was saturated. Each one represents worker budget that will
    /// only be reclaimed when the affected worker is evicted.
    pub reservation_leak_on_drop_enqueue_failed: AtomicU64,
    /// Double-disarm detections (logic bug, soft-warned and counted rather
    /// than panicking in release builds).
    pub reservation_disarm_bugs: AtomicU64,
    /// Cumulative nanoseconds spent waiting to acquire `inner` mutex on the
    /// match hot paths (`reserve_worker_for_action`, `commit_reservation`,
    /// `release_reservation`). A high average wait indicates pool-mutex
    /// contention is the bottleneck rather than per-call work.
    pub inner_lock_wait_ns: AtomicU64,
    /// Count of `inner` mutex acquisitions sampled on the match hot paths.
    /// Paired with `inner_lock_wait_ns` to derive average wait per acquire.
    pub inner_lock_wait_samples: AtomicU64,
    /// `do_try_match` cycles where the queue had at least one queued op and
    /// zero ops were dispatched. Persistent nonzero values are the smoking
    /// gun for a worker-budget leak (every worker reports `!can_accept_work`).
    pub do_try_match_starved_cycles: AtomicU64,
    /// `do_try_match` cycles initiated by the safety-net interval rather than
    /// by `task_change_notify` / `worker_change_notify`. Should stay near
    /// zero in healthy operation; growth means the notify path is failing or
    /// the queue is being drained slower than the safety-net cadence.
    pub matcher_interval_kicks: AtomicU64,
    /// Drop-time channel-saturation events that successfully restored worker
    /// budget via the spawned fallback (rather than leaking like the legacy
    /// behaviour). Counted alongside `reservation_leak_on_drop_enqueue_failed`
    /// so the diff measures how much budget was reclaimed by the fallback.
    pub reservation_drop_fallback_restores: AtomicU64,
}

/// Capacity of the bounded release channel. Sized for the worst-case burst
/// of simultaneously-dropped reservations (much larger than any realistic
/// `N × pod-shutdown` scenario); overflow is a loud error.
const RELEASE_CHANNEL_CAPACITY: usize = 256;

use crate::platform_property_manager::PlatformPropertyManager;
use crate::worker::{
    ActionInfoWithProps, FinalizedRun, Worker, WorkerGeneration, WorkerTimestamp, WorkerUpdate,
};
use crate::worker_capability_index::WorkerCapabilityIndex;
use crate::worker_registry::SharedWorkerRegistry;
use crate::worker_scheduler::WorkerScheduler;

/// Payload owned by an active `WorkerReservation`. Moved out of the outer
/// handle on disarm (commit / release / Drop) so exactly one of those three
/// code paths ever processes a given reservation.
#[derive(Debug)]
struct WorkerReservationInner {
    worker_id: WorkerId,
    generation: WorkerGeneration,
    debits: Vec<(String, PlatformPropertyValue)>,
    release_tx: mpsc::Sender<WorkerReservationInner>,
    /// Weak ref to the owning scheduler so the `Drop` fallback (used when
    /// `release_tx` is saturated) can spawn an async task that takes the
    /// pool lock and restores the worker's budget directly. Without this,
    /// channel-full saturation leaks `pending_action_count` permanently
    /// (until the worker is evicted), which over time pushes every worker's
    /// `can_accept_work` to false and parks the matcher.
    scheduler: Weak<ApiWorkerScheduler>,
}

/// Handle representing an exclusive claim on a worker's budget slot for a
/// pending match. The inner payload is consumed by one of three terminal
/// paths — `commit_reservation`, `release_reservation`, or Drop — whichever
/// fires first. After disarm, Drop is a no-op; before disarm, Drop enqueues
/// the payload on the release channel so the releaser task can restore the
/// budget under the pool lock.
#[derive(Debug)]
pub struct WorkerReservation {
    inner: Option<WorkerReservationInner>,
    metrics: Arc<SchedulerMetrics>,
}

impl WorkerReservation {
    /// Consume the payload for explicit commit / release paths. Drop becomes
    /// a no-op afterwards. Returns `None` if already disarmed — a logic bug;
    /// debug-asserts in debug builds, logs + counts `reservation_disarm_bugs`
    /// in all builds and returns `None` so callers can short-circuit rather
    /// than taking the process down.
    fn disarm(mut self) -> Option<WorkerReservationInner> {
        let inner = self.inner.take();
        if inner.is_none() {
            debug_assert!(false, "WorkerReservation already disarmed");
            error!("WorkerReservation disarm called twice — logic bug");
            self.metrics
                .reservation_disarm_bugs
                .fetch_add(1, Ordering::Relaxed);
        }
        inner
    }

    /// Target worker's id while the reservation is armed.
    pub fn worker_id(&self) -> Option<&WorkerId> {
        self.inner.as_ref().map(|i| &i.worker_id)
    }

    /// Worker generation captured at reservation time.
    pub fn generation(&self) -> Option<WorkerGeneration> {
        self.inner.as_ref().map(|i| i.generation)
    }
}

impl Drop for WorkerReservation {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let tx = inner.release_tx.clone();
        match tx.try_send(inner) {
            Ok(()) => {
                // Releaser task will process under the mutex and increment
                // `reservations_released` there.
            }
            Err(TrySendError::Full(dropped)) => {
                // Channel saturated. Rather than leak the budget (the legacy
                // behaviour, which under sustained reservation churn pushed
                // every worker's `pending_action_count` past `max_inflight`,
                // making `can_accept_work` permanently false and parking the
                // matcher), spawn a fallback task that takes the pool lock
                // directly and restores the worker's budget. Same generation
                // fence as the releaser task so a concurrently-replaced
                // worker is not perturbed.
                self.metrics
                    .reservation_leak_on_drop_enqueue_failed
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    worker_id = %dropped.worker_id,
                    "release channel saturated; restoring worker budget via fallback task"
                );
                let metrics = Arc::clone(&self.metrics);
                let weak = dropped.scheduler.clone();
                let worker_id = dropped.worker_id.clone();
                let generation = dropped.generation;
                let debits = dropped.debits;
                tokio::spawn(async move {
                    let Some(scheduler) = weak.upgrade() else {
                        // Scheduler dropped between Drop and the spawn
                        // running; pool is gone, nothing to restore.
                        return;
                    };
                    let mut inner = scheduler.inner.lock().await;
                    if let Some(worker) = inner.workers.get_mut(&worker_id)
                        && worker.generation() == generation
                    {
                        worker.restore_budget(&debits);
                    }
                    drop(inner);
                    metrics
                        .reservation_drop_fallback_restores
                        .fetch_add(1, Ordering::Relaxed);
                    metrics
                        .reservations_released
                        .fetch_add(1, Ordering::Relaxed);
                });
            }
            Err(TrySendError::Closed(_)) => {
                // Scheduler shutting down; worker pool is being torn down too.
            }
        }
    }
}

#[derive(Debug)]
struct Workers(LruCache<WorkerId, Worker>);

impl Deref for Workers {
    type Target = LruCache<WorkerId, Worker>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Workers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Note: This could not be a derive macro because this derive-macro
// does not support LruCache and nameless field structs.
impl MetricsComponent for Workers {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        let _enter = group!("workers").entered();
        for (worker_id, worker) in self.iter() {
            let _enter = group!(worker_id).entered();
            worker.publish(MetricKind::Component, MetricFieldData::default())?;
        }
        Ok(MetricPublishKnownKindData::Component)
    }
}

/// A collection of workers that are available to run tasks.
#[derive(MetricsComponent)]
struct ApiWorkerSchedulerImpl {
    /// A `LruCache` of workers available based on `allocation_strategy`.
    #[metric(group = "workers")]
    workers: Workers,

    /// The worker state manager.
    #[metric(group = "worker_state_manager")]
    worker_state_manager: Arc<dyn WorkerStateManager>,
    /// The allocation strategy for workers.
    allocation_strategy: WorkerAllocationStrategy,
    /// A channel to notify the matching engine that the worker pool has changed.
    worker_change_notify: Arc<Notify>,
    /// Worker registry for tracking worker liveness.
    worker_registry: SharedWorkerRegistry,

    /// Whether the worker scheduler is shutting down.
    shutting_down: bool,

    /// Index for fast worker capability lookup.
    /// Used to accelerate `find_worker_for_action` by filtering candidates
    /// based on properties before doing linear scan.
    capability_index: WorkerCapabilityIndex,

    /// Monotonically-increasing counter that mints a `WorkerGeneration` each
    /// time a worker enters the pool. Reservations capture the worker's
    /// generation at issue time; commit checks the pool generation still
    /// matches, refusing stale reservations across reconnects.
    next_generation: AtomicU64,
}

impl core::fmt::Debug for ApiWorkerSchedulerImpl {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ApiWorkerSchedulerImpl")
            .field("workers", &self.workers)
            .field("allocation_strategy", &self.allocation_strategy)
            .field("worker_change_notify", &self.worker_change_notify)
            .field(
                "capability_index_size",
                &self.capability_index.worker_count(),
            )
            .field("worker_registry", &self.worker_registry)
            .finish_non_exhaustive()
    }
}

impl ApiWorkerSchedulerImpl {
    /// Refreshes the lifetime of the worker with the given timestamp.
    ///
    /// Instead of sending N keepalive messages (one per operation),
    /// we now send a single worker heartbeat. The worker registry tracks worker liveness,
    /// and timeout detection checks the worker's `last_seen` instead of per-operation timestamps.
    ///
    /// Note: This only updates the local worker state. The worker registry is updated
    /// separately after releasing the inner lock to reduce contention.
    fn refresh_lifetime(
        &mut self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let worker = self.workers.0.peek_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in refresh_lifetime() {}",
                worker_id
            )
        })?;
        error_if!(
            worker.last_update_timestamp > timestamp,
            "Worker already had a timestamp of {}, but tried to update it with {}",
            worker.last_update_timestamp,
            timestamp
        );
        worker.last_update_timestamp = timestamp;

        trace!(
            ?worker_id,
            running_operations = worker.running_action_infos.len(),
            "Worker keepalive received"
        );

        Ok(())
    }

    /// Adds a worker to the pool.
    /// Note: This function will not do any task matching.
    fn add_worker(&mut self, mut worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id.clone();
        let platform_properties = worker.platform_properties.clone();
        // Mint a fresh generation for this worker instance. A reconnect lands
        // a new `Worker` under the same `WorkerId` via `LruCache::put` (which
        // replaces), so any reservation still holding the previous generation
        // will fail the fence check at commit time.
        let generation = WorkerGeneration(self.next_generation.fetch_add(1, Ordering::Relaxed));
        worker.set_generation(generation);
        self.workers.put(worker_id.clone(), worker);

        // Add to capability index for fast matching
        self.capability_index
            .add_worker(&worker_id, &platform_properties);

        // Worker is not cloneable, and we do not want to send the initial connection results until
        // we have added it to the map, or we might get some strange race conditions due to the way
        // the multi-threaded runtime works.
        let worker = self.workers.peek_mut(&worker_id).unwrap();
        let res = worker
            .send_initial_connection_result()
            .err_tip(|| "Failed to send initial connection result to worker");
        if let Err(err) = &res {
            error!(
                ?worker_id,
                ?err,
                "Worker connection appears to have been closed while adding to pool"
            );
        }
        self.worker_change_notify.notify_one();
        res
    }

    /// Removes worker from pool.
    /// Note: The caller is responsible for any rescheduling of any tasks that might be
    /// running.
    fn remove_worker(&mut self, worker_id: &WorkerId) -> Option<Worker> {
        // Remove from capability index
        self.capability_index.remove_worker(worker_id);

        let result = self.workers.pop(worker_id);
        self.worker_change_notify.notify_one();
        result
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(
        &mut self,
        worker_id: &WorkerId,
        is_draining: bool,
    ) -> Result<(), Error> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .err_tip(|| format!("Worker {worker_id} doesn't exist in the pool"))?;
        worker.is_draining = is_draining;
        self.worker_change_notify.notify_one();
        Ok(())
    }

    fn inner_find_worker_for_action(
        &self,
        platform_properties: &PlatformProperties,
        full_worker_logging: bool,
    ) -> Option<WorkerId> {
        // Do a fast check to see if any workers are available at all for work allocation
        if !self.workers.iter().any(|(_, w)| w.can_accept_work()) {
            if full_worker_logging {
                info!("All workers are fully allocated");
            }
            return None;
        }

        // Use capability index to get candidate workers that match STATIC properties
        // (Exact, Unknown) and have the required property keys (Priority, Minimum).
        // This reduces complexity from O(W × P) to O(P × log(W)) for exact properties.
        let candidates = self
            .capability_index
            .find_matching_workers(platform_properties, full_worker_logging);

        if candidates.is_empty() {
            if full_worker_logging {
                info!("No workers in capability index match required properties");
            }
            return None;
        }

        // Check function for availability AND dynamic Minimum property verification.
        // The index only does presence checks for Minimum properties since their
        // values change dynamically as jobs are assigned to workers.
        let worker_matches = |(worker_id, w): &(&WorkerId, &Worker)| -> bool {
            if !w.can_accept_work() {
                if full_worker_logging {
                    info!(
                        "Worker {worker_id} cannot accept work: is_paused={}, is_draining={}, inflight={}/{} (running={}, pending={})",
                        w.is_paused,
                        w.is_draining,
                        w.running_action_infos.len() + w.pending_action_count(),
                        w.max_inflight_tasks,
                        w.running_action_infos.len(),
                        w.pending_action_count(),
                    );
                }
                return false;
            }

            // Verify Minimum properties at runtime (their values are dynamic)
            if !platform_properties.is_satisfied_by(&w.platform_properties, full_worker_logging) {
                return false;
            }

            true
        };

        // Now check constraints on filtered candidates.
        // Iterate in LRU order based on allocation strategy.
        let workers_iter = self.workers.iter();

        let worker_id = match self.allocation_strategy {
            // Use rfind to get the least recently used that satisfies the properties.
            WorkerAllocationStrategy::LeastRecentlyUsed => workers_iter
                .rev()
                .filter(|(worker_id, _)| candidates.contains(worker_id))
                .find(&worker_matches)
                .map(|(_, w)| w.id.clone()),

            // Use find to get the most recently used that satisfies the properties.
            WorkerAllocationStrategy::MostRecentlyUsed => workers_iter
                .filter(|(worker_id, _)| candidates.contains(worker_id))
                .find(&worker_matches)
                .map(|(_, w)| w.id.clone()),
        };
        if full_worker_logging && worker_id.is_none() {
            warn!("No workers matched!");
        }
        worker_id
    }

    /// Notifies the specified worker to run the given action and handles errors by evicting
    /// the worker if the notification fails.
    async fn worker_notify_run_action(
        &mut self,
        worker_id: WorkerId,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> Result<(), Error> {
        if let Some(worker) = self.workers.get_mut(&worker_id) {
            let notify_worker_result = worker
                .notify_update(WorkerUpdate::RunAction((operation_id, action_info.clone())))
                .await;

            if let Err(notify_worker_result) = notify_worker_result {
                warn!(
                    ?worker_id,
                    ?action_info,
                    ?notify_worker_result,
                    "Worker command failed, removing worker",
                );

                // A slightly nasty way of figuring out that the worker disconnected
                // from send_msg_to_worker without introducing complexity to the
                // code path from here to there.
                let is_disconnect = notify_worker_result.code == Code::Internal
                    && notify_worker_result.messages.len() == 1
                    && notify_worker_result.messages[0] == "Worker Disconnected";

                let err = make_err!(
                    Code::Internal,
                    "Worker command failed, removing worker {worker_id} -- {notify_worker_result:?}",
                );

                return Result::<(), _>::Err(err.clone()).merge(
                    self.immediate_evict_worker(&worker_id, err, is_disconnect)
                        .await,
                );
            }
            Ok(())
        } else {
            warn!(
                ?worker_id,
                %operation_id,
                ?action_info,
                "Worker not found in worker map in worker_notify_run_action"
            );
            // Ensure the operation is put back to queued state.
            self.worker_state_manager
                .update_operation(
                    &operation_id,
                    &worker_id,
                    UpdateOperationType::UpdateWithDisconnect,
                )
                .await
        }
    }

    /// Evicts the worker from the pool and puts items back into the queue if anything was being executed on it.
    async fn immediate_evict_worker(
        &mut self,
        worker_id: &WorkerId,
        err: Error,
        is_disconnect: bool,
    ) -> Result<(), Error> {
        let mut result = Ok(());
        if let Some(mut worker) = self.remove_worker(worker_id) {
            // We don't care if we fail to send message to worker, this is only a best attempt.
            drop(worker.notify_update(WorkerUpdate::Disconnect).await);
            let update = if is_disconnect {
                UpdateOperationType::UpdateWithDisconnect
            } else {
                UpdateOperationType::UpdateWithError(err)
            };
            for (operation_id, _) in worker.running_action_infos.drain() {
                result = result.merge(
                    self.worker_state_manager
                        .update_operation(&operation_id, worker_id, update.clone())
                        .await,
                );
            }
        }
        // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
        // TODO(palfrey) This should be moved to inside the Workers struct.
        self.worker_change_notify.notify_one();
        result
    }
}

#[derive(Debug, MetricsComponent)]
pub struct ApiWorkerScheduler {
    #[metric]
    inner: Mutex<ApiWorkerSchedulerImpl>,
    #[metric(group = "platform_property_manager")]
    platform_property_manager: Arc<PlatformPropertyManager>,

    #[metric(
        help = "Timeout of how long to evict workers if no response in this given amount of time in seconds."
    )]
    worker_timeout_s: u64,
    /// Shared worker registry for checking worker liveness.
    worker_registry: SharedWorkerRegistry,

    /// Performance metrics for observability.
    metrics: Arc<SchedulerMetrics>,

    /// Bounded sender used by `WorkerReservation::Drop` to enqueue
    /// cancellation cleanup on the releaser task. Cloned into every
    /// reservation handle. Capacity is `RELEASE_CHANNEL_CAPACITY`.
    release_tx: mpsc::Sender<WorkerReservationInner>,

    /// Self-referential weak handle, populated via `Arc::new_cyclic` in
    /// `ApiWorkerScheduler::new`. Cloned into every issued `WorkerReservation`
    /// so that the `Drop` fallback (channel-saturated path) can spawn an
    /// async task that re-acquires the pool lock and restores worker
    /// budget — closing the leak window that previously deadlocked the
    /// matcher under reservation churn.
    weak_self: Weak<ApiWorkerScheduler>,

    /// Abort-on-drop handle for the releaser task spawned in `new`. Without
    /// this guard, the `tokio::spawn` that drains the release channel kept
    /// running for the lifetime of the tokio runtime — relying on the
    /// channel sender being dropped to terminate the loop. Under
    /// `tokio::test`, the runtime drops at end-of-test and tries to join
    /// all spawned tasks; the releaser's `recv().await` is cancel-safe in
    /// principle but the abort signal isn't observed until the runtime
    /// finishes shutdown. Wrapping in `JoinHandleDropGuard` ensures the
    /// task is `.abort()`'d the moment this struct drops, before runtime
    /// shutdown gets a chance to wait on it.
    _releaser_handle: JoinHandleDropGuard<()>,
}

impl ApiWorkerScheduler {
    pub fn new(
        worker_state_manager: Arc<dyn WorkerStateManager>,
        platform_property_manager: Arc<PlatformPropertyManager>,
        allocation_strategy: WorkerAllocationStrategy,
        worker_change_notify: Arc<Notify>,
        worker_timeout_s: u64,
        worker_registry: SharedWorkerRegistry,
    ) -> Arc<Self> {
        let (release_tx, release_rx) =
            mpsc::channel::<WorkerReservationInner>(RELEASE_CHANNEL_CAPACITY);
        let metrics = Arc::new(SchedulerMetrics::default());
        let arc_self = Arc::new_cyclic(|weak_self| {
            // Releaser task: drains reservations that were Drop-enqueued by
            // future cancellation (pod shutdown, stream drop, panic). Uses
            // the cyclic Weak so its existence does not keep the scheduler
            // alive. Spawned via `spawn!` (NOT raw `tokio::spawn`) so the
            // returned `JoinHandleDropGuard` aborts the task when this
            // struct drops — required so `tokio::test` runtime shutdown
            // doesn't hang waiting for the releaser to observe channel
            // closure.
            let weak_for_releaser = weak_self.clone();
            let metrics_for_releaser = Arc::clone(&metrics);
            Self {
                inner: Mutex::new(ApiWorkerSchedulerImpl {
                    workers: Workers(LruCache::unbounded()),
                    worker_state_manager,
                    allocation_strategy,
                    worker_change_notify,
                    worker_registry: worker_registry.clone(),
                    shutting_down: false,
                    capability_index: WorkerCapabilityIndex::new(),
                    // Start at 1 so `WorkerGeneration(0)` (the `Worker::new`
                    // default) is always distinguishable from a live generation.
                    next_generation: AtomicU64::new(1),
                }),
                platform_property_manager,
                worker_timeout_s,
                worker_registry,
                metrics: Arc::clone(&metrics),
                release_tx,
                weak_self: weak_self.clone(),
                _releaser_handle: spawn!(
                    "api_worker_scheduler_releaser",
                    Self::run_releaser(weak_for_releaser, release_rx, metrics_for_releaser),
                ),
            }
        });

        arc_self
    }

    async fn run_releaser(
        weak_self: Weak<Self>,
        mut release_rx: mpsc::Receiver<WorkerReservationInner>,
        metrics: Arc<SchedulerMetrics>,
    ) {
        while let Some(payload) = release_rx.recv().await {
            let Some(scheduler) = weak_self.upgrade() else {
                return;
            };
            {
                let mut inner = scheduler.inner.lock().await;
                if let Some(worker) = inner.workers.get_mut(&payload.worker_id) {
                    // If the generation changed the worker has been replaced
                    // and the new instance has its own (untouched) budget —
                    // nothing to restore.
                    if worker.generation() == payload.generation {
                        worker.restore_budget(&payload.debits);
                    }
                }
            }
            metrics
                .reservations_released
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Attempts to find a worker capable of running an action and reserves
    /// its budget slot. The returned reservation must be consumed by
    /// `commit_reservation` or `release_reservation`; if neither is called
    /// the Drop impl enqueues cleanup on the releaser task.
    pub async fn reserve_worker_for_action(
        &self,
        platform_properties: &PlatformProperties,
        full_worker_logging: bool,
    ) -> Option<WorkerReservation> {
        let start = Instant::now();
        self.metrics
            .find_worker_calls
            .fetch_add(1, Ordering::Relaxed);

        let wait_start = Instant::now();
        let mut inner = self.inner.lock().await;
        self.metrics
            .inner_lock_wait_ns
            .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.metrics
            .inner_lock_wait_samples
            .fetch_add(1, Ordering::Relaxed);
        let worker_count = inner.workers.len() as u64;
        let maybe_worker_id =
            inner.inner_find_worker_for_action(platform_properties, full_worker_logging);

        self.metrics
            .workers_iterated
            .fetch_add(worker_count, Ordering::Relaxed);

        #[allow(clippy::cast_possible_truncation)]
        self.metrics
            .find_worker_time_ns
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let Some(worker_id) = maybe_worker_id else {
            self.metrics
                .find_worker_misses
                .fetch_add(1, Ordering::Relaxed);
            return None;
        };

        // Found a candidate — debit its budget under the lock so no other
        // concurrent match can reserve the same slot.
        let worker = inner
            .workers
            .get_mut(&worker_id)
            .expect("inner_find_worker_for_action returned a worker in the pool");
        let generation = worker.generation();
        let debits = worker.reserve_budget(platform_properties);

        self.metrics
            .find_worker_hits
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .reservations_created
            .fetch_add(1, Ordering::Relaxed);

        Some(WorkerReservation {
            inner: Some(WorkerReservationInner {
                worker_id,
                generation,
                debits,
                release_tx: self.release_tx.clone(),
                scheduler: self.weak_self.clone(),
            }),
            metrics: Arc::clone(&self.metrics),
        })
    }

    /// Commits a reservation to a running action: verifies the worker's
    /// generation still matches, inserts the op into `running_action_infos`,
    /// and sends `StartAction` to the worker.
    ///
    /// On generation mismatch or worker-gone, returns `Err((Some(res), err))`
    /// with the reservation still armed — caller must release it so the
    /// budget is refunded.
    ///
    /// On send failure (worker disconnected mid-finalize) the worker is
    /// evicted (which requeues the just-inserted op via `UpdateWithDisconnect`)
    /// and `Err((None, err))` is returned.
    pub async fn commit_reservation(
        &self,
        res: WorkerReservation,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> Result<(), (Option<WorkerReservation>, Error)> {
        let worker_id = res
            .worker_id()
            .expect("commit_reservation called on disarmed reservation")
            .clone();
        let expected_generation = res
            .generation()
            .expect("commit_reservation called on disarmed reservation");

        let wait_start = Instant::now();
        let mut inner = self.inner.lock().await;
        self.metrics
            .inner_lock_wait_ns
            .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.metrics
            .inner_lock_wait_samples
            .fetch_add(1, Ordering::Relaxed);

        // Phase 1: generation fence (read-only on `inner.workers`).
        let pool_generation = inner.workers.peek(&worker_id).map(Worker::generation);
        match pool_generation {
            Some(found) if found == expected_generation => { /* pass */ }
            Some(found) => {
                self.metrics
                    .reservation_generation_mismatches
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .reservation_commit_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err((
                    Some(res),
                    make_err!(
                        Code::Aborted,
                        "worker {worker_id} generation mismatch: reservation was for {:?}, pool now has {:?}",
                        expected_generation,
                        found
                    ),
                ));
            }
            None => {
                self.metrics
                    .reservation_generation_mismatches
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .reservation_commit_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err((
                    Some(res),
                    make_err!(Code::Aborted, "worker {worker_id} no longer in pool"),
                ));
            }
        }

        // Phase 2a (under lock): disarm and record the run in the worker's
        // state. The mutation is atomic w.r.t. add_worker/remove_worker so
        // the generation fence still holds — a concurrent reconnect minting
        // a new generation cannot observe or inherit this op.
        let _payload = res.disarm();
        let finalized: FinalizedRun = {
            let worker = inner
                .workers
                .get_mut(&worker_id)
                .expect("generation check held; worker still present under lock");
            worker.finalize_run_state_only(operation_id, action_info)
        };

        // Phase 2b (no lock): fire the `StartAction` notification via the
        // worker's unbounded sender. The send is synchronous and
        // non-blocking; taking it outside the pool mutex is the primary
        // round-2 win — match-hot-path reserve/commit/release no longer
        // contend with this dispatch.
        drop(inner);
        let send_res = finalized.send();

        match send_res {
            Ok(()) => {
                self.metrics
                    .reservations_committed
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .actions_dispatched
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(notify_err) => {
                let is_disconnect = notify_err.code == Code::Internal
                    && notify_err.messages.len() == 1
                    && notify_err.messages[0] == "Worker Disconnected";
                let err = make_err!(
                    Code::Internal,
                    "Worker command failed during commit, removing worker {worker_id} -- {notify_err:?}",
                );

                // Re-acquire the pool lock to evict the worker. Guard the
                // eviction with a generation re-check: between `drop(inner)`
                // and this re-acquire the worker may have been evicted and
                // replaced (reconnect) already, in which case our op has
                // already been drained + requeued by whichever path evicted
                // the OLD worker. Blindly calling `immediate_evict_worker`
                // would incorrectly evict the NEW generation under the same
                // `WorkerId`.
                let wait_start = Instant::now();
                let mut inner = self.inner.lock().await;
                self.metrics.inner_lock_wait_ns.fetch_add(
                    wait_start.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                self.metrics
                    .inner_lock_wait_samples
                    .fetch_add(1, Ordering::Relaxed);
                let current_generation = inner.workers.peek(&worker_id).map(Worker::generation);
                let evict_res: Result<(), Error> =
                    if current_generation == Some(expected_generation) {
                        inner
                            .immediate_evict_worker(&worker_id, err.clone(), is_disconnect)
                            .await
                    } else {
                        // OLD worker is already gone (different generation
                        // or absent). Its `running_action_infos` — which now
                        // holds our op — was drained by that eviction path
                        // and the op requeued. Nothing else to clean up.
                        Ok(())
                    };
                self.metrics
                    .reservation_commit_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err((
                    None,
                    Result::<(), _>::Err(err).merge(evict_res).unwrap_err(),
                ))
            }
        }
    }

    /// Explicitly releases a reservation: restores the worker's debited
    /// budget and increments `reservations_released`. On double-disarm the
    /// call is a no-op (logged + counted via `reservation_disarm_bugs`).
    pub async fn release_reservation(&self, res: WorkerReservation) {
        let Some(payload) = res.disarm() else {
            return;
        };
        {
            let wait_start = Instant::now();
            let mut inner = self.inner.lock().await;
            self.metrics
                .inner_lock_wait_ns
                .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.metrics
                .inner_lock_wait_samples
                .fetch_add(1, Ordering::Relaxed);
            if let Some(worker) = inner.workers.get_mut(&payload.worker_id) {
                if worker.generation() == payload.generation {
                    worker.restore_budget(&payload.debits);
                }
                // Worker-replaced case: new instance has its own untouched
                // budget; nothing to do.
            }
        }
        self.metrics
            .reservations_released
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a reference to the worker registry.
    pub const fn worker_registry(&self) -> &SharedWorkerRegistry {
        &self.worker_registry
    }

    /// Legacy one-shot dispatch: reserve + commit in a single locked step.
    /// Retained for callers outside the matcher (tests, health paths) that
    /// do not use the reserve/commit/release split. The matcher now goes
    /// through `reserve_worker_for_action` + `commit_reservation`.
    pub async fn worker_notify_run_action(
        &self,
        worker_id: WorkerId,
        operation_id: OperationId,
        action_info: ActionInfoWithProps,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        let result = inner
            .worker_notify_run_action(worker_id, operation_id, action_info)
            .await;
        if result.is_ok() {
            self.metrics
                .actions_dispatched
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Returns the scheduler metrics for observability.
    #[must_use]
    pub const fn get_metrics(&self) -> &Arc<SchedulerMetrics> {
        &self.metrics
    }

    /// Attempts to find a worker that is capable of running this action.
    // TODO(palfrey) This algorithm is not very efficient. Simple testing using a tree-like
    // structure showed worse performance on a 10_000 worker * 7 properties * 1000 queued tasks
    // simulation of worst cases in a single threaded environment.
    pub async fn find_worker_for_action(
        &self,
        platform_properties: &PlatformProperties,
        full_worker_logging: bool,
    ) -> Option<WorkerId> {
        let start = Instant::now();
        self.metrics
            .find_worker_calls
            .fetch_add(1, Ordering::Relaxed);

        let inner = self.inner.lock().await;
        let worker_count = inner.workers.len() as u64;
        let result = inner.inner_find_worker_for_action(platform_properties, full_worker_logging);

        // Track workers iterated (worst case is all workers)
        self.metrics
            .workers_iterated
            .fetch_add(worker_count, Ordering::Relaxed);

        if result.is_some() {
            self.metrics
                .find_worker_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .find_worker_misses
                .fetch_add(1, Ordering::Relaxed);
        }

        #[allow(clippy::cast_possible_truncation)]
        self.metrics
            .find_worker_time_ns
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    /// Checks to see if the worker exists in the worker pool. Should only be used in unit tests.
    #[must_use]
    pub async fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner = self.inner.lock().await;
        inner.workers.contains(worker_id)
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub async fn send_keep_alive_to_worker_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        let worker = inner.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.keep_alive()
    }

    /// Returns the worker's `pending_action_count` if the worker exists. Used
    /// by tests verifying the Drop-fallback budget-restore path.
    pub async fn pending_action_count_of_worker_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Option<usize> {
        let inner = self.inner.lock().await;
        inner
            .workers
            .peek(worker_id)
            .map(Worker::pending_action_count_for_test)
    }

    /// Force-sets the worker's `pending_action_count`. Used by tests to
    /// simulate the production leak that this fix closes — pre-fix, channel-
    /// saturation Drops would leak `pending_action_count`, making
    /// `can_accept_work` return `false` indefinitely.
    pub async fn set_pending_action_count_for_test(
        &self,
        worker_id: &WorkerId,
        count: usize,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        let worker = inner.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.set_pending_action_count_for_test(count);
        Ok(())
    }
}

#[async_trait]
impl WorkerScheduler for ApiWorkerScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.platform_property_manager.as_ref()
    }

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id.clone();
        let worker_timestamp = worker.last_update_timestamp;
        let mut inner = self.inner.lock().await;
        if inner.shutting_down {
            warn!("Rejected worker add during shutdown: {}", worker_id);
            return Err(make_err!(
                Code::Unavailable,
                "Received request to add worker while shutting down"
            ));
        }
        let result = inner
            .add_worker(worker)
            .err_tip(|| "Error while adding worker, removing from pool");
        if let Err(err) = result {
            return Result::<(), _>::Err(err.clone())
                .merge(inner.immediate_evict_worker(&worker_id, err, false).await);
        }

        let now = UNIX_EPOCH + Duration::from_secs(worker_timestamp);
        self.worker_registry.register_worker(&worker_id, now).await;

        self.metrics.workers_added.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        // Phase A (under pool lock): validate membership, classify the
        // update, and short-circuit `ExecutionComplete` (which only touches
        // in-memory worker state). Capture the state-manager + notify
        // handles to drive Phase B without holding the lock.
        let (is_finished, due_to_backpressure, worker_state_manager, worker_change_notify) = {
            let wait_start = Instant::now();
            let mut inner = self.inner.lock().await;
            self.metrics.inner_lock_wait_ns.fetch_add(
                wait_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
            self.metrics
                .inner_lock_wait_samples
                .fetch_add(1, Ordering::Relaxed);

            let worker = inner.workers.get_mut(worker_id).err_tip(|| {
                format!("Worker {worker_id} does not exist in SimpleScheduler::update_action")
            })?;

            if !worker.running_action_infos.contains_key(operation_id) {
                let err = make_err!(
                    Code::Internal,
                    "Operation {operation_id} should not be running on worker {worker_id} in SimpleScheduler::update_action"
                );
                return Result::<(), _>::Err(err.clone())
                    .merge(inner.immediate_evict_worker(worker_id, err, false).await);
            }

            let (is_finished, due_to_backpressure) = match &update {
                UpdateOperationType::UpdateWithActionStage(action_stage) => {
                    (action_stage.is_finished(), false)
                }
                UpdateOperationType::KeepAlive => (false, false),
                UpdateOperationType::UpdateWithError(err) => {
                    (true, err.code == Code::ResourceExhausted)
                }
                UpdateOperationType::UpdateWithDisconnect => (true, false),
                UpdateOperationType::ExecutionComplete => {
                    // Pure in-memory property restore — no state-manager
                    // round trip; short-circuit under the lock and return.
                    worker.execution_complete(operation_id);
                    inner.worker_change_notify.notify_one();
                    return Ok(());
                }
            };

            (
                is_finished,
                due_to_backpressure,
                inner.worker_state_manager.clone(),
                inner.worker_change_notify.clone(),
            )
        };

        // Phase B (no pool lock): run the state-manager update. This is
        // the previously-contended Redis round trip — moving it outside
        // the pool mutex is the primary round-2 win on this path.
        let update_operation_res = worker_state_manager
            .update_operation(operation_id, worker_id, update)
            .await
            .err_tip(|| "in update_operation on SimpleScheduler::update_action");
        if let Err(err) = update_operation_res {
            error!(
                %operation_id,
                ?worker_id,
                ?err,
                "Failed to update_operation on update_action"
            );
            return Err(err);
        }

        if !is_finished {
            return Ok(());
        }

        // Phase C (re-acquire pool lock, is_finished branch only): clear
        // the action from the worker and apply the backpressure pause
        // check. The worker may have been evicted/replaced between B and
        // C; the state-manager update in B already authoritatively
        // recorded the finish, so if the worker is gone we early-return.
        let complete_action_res = {
            let wait_start = Instant::now();
            let mut inner = self.inner.lock().await;
            self.metrics.inner_lock_wait_ns.fetch_add(
                wait_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
            self.metrics
                .inner_lock_wait_samples
                .fetch_add(1, Ordering::Relaxed);

            let Some(worker) = inner.workers.get_mut(worker_id) else {
                // Worker evicted between Phase B and Phase C. State
                // manager has already captured the finish; nothing to do.
                return Ok(());
            };
            let res = worker.complete_action(operation_id).await;
            if (due_to_backpressure || !worker.can_accept_work()) && worker.has_actions() {
                worker.is_paused = true;
            }
            res
        };

        worker_change_notify.notify_one();
        complete_action_res
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        {
            let mut inner = self.inner.lock().await;
            inner
                .refresh_lifetime(worker_id, timestamp)
                .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()")?;
        }
        let now = UNIX_EPOCH + Duration::from_secs(timestamp);
        self.worker_registry
            .update_worker_heartbeat(worker_id, now)
            .await;
        Ok(())
    }

    async fn remove_worker(&self, worker_id: &WorkerId, reason: Error) -> Result<(), Error> {
        self.worker_registry.remove_worker(worker_id).await;

        let mut inner = self.inner.lock().await;
        inner
            .immediate_evict_worker(worker_id, reason, false)
            .await
    }

    async fn shutdown(&self, shutdown_guard: ShutdownGuard) {
        let mut inner = self.inner.lock().await;
        inner.shutting_down = true; // should reject further worker registration
        while let Some(worker_id) = inner
            .workers
            .peek_lru()
            .map(|(worker_id, _worker)| worker_id.clone())
        {
            if let Err(err) = inner
                .immediate_evict_worker(
                    &worker_id,
                    make_err!(Code::Internal, "Scheduler shutdown"),
                    true,
                )
                .await
            {
                error!(?err, "Error evicting worker on shutdown.");
            }
        }
        drop(shutdown_guard);
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        // Check worker liveness using both the local timestamp (from LRU)
        // and the worker registry. A worker is alive if either source says it's alive.
        let timeout = Duration::from_secs(self.worker_timeout_s);
        let now = UNIX_EPOCH + Duration::from_secs(now_timestamp);
        let timeout_threshold = now_timestamp.saturating_sub(self.worker_timeout_s);

        let workers_to_check: Vec<(WorkerId, bool)> = {
            let inner = self.inner.lock().await;
            inner
                .workers
                .iter()
                .map(|(worker_id, worker)| {
                    let local_alive = worker.last_update_timestamp > timeout_threshold;
                    (worker_id.clone(), local_alive)
                })
                .collect()
        };

        let mut worker_ids_to_remove = Vec::new();
        for (worker_id, local_alive) in workers_to_check {
            if local_alive {
                continue;
            }

            let registry_alive = self
                .worker_registry
                .is_worker_alive(&worker_id, timeout, now)
                .await;

            if !registry_alive {
                trace!(
                    ?worker_id,
                    local_alive,
                    registry_alive,
                    timeout_threshold,
                    "Worker timed out - neither local nor registry shows alive"
                );
                worker_ids_to_remove.push(worker_id);
            }
        }

        if worker_ids_to_remove.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock().await;
        let mut result = Ok(());

        for worker_id in &worker_ids_to_remove {
            warn!(?worker_id, "Worker timed out, removing from pool");
            result = result.merge(
                inner
                    .immediate_evict_worker(
                        worker_id,
                        make_err!(
                            Code::Internal,
                            "Worker {worker_id} timed out, removing from pool"
                        ),
                        false,
                    )
                    .await,
            );
        }

        result
    }

    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        inner.set_drain_worker(worker_id, is_draining).await
    }
}

impl RootMetricsComponent for ApiWorkerScheduler {}
