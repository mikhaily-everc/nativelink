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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt, future};
use nativelink_config::schedulers::SimpleSpec;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::{MetricsComponent, RootMetricsComponent};
use nativelink_proto::com::github::trace_machina::nativelink::events::OriginEvent;
use nativelink_util::action_messages::{ActionInfo, ActionState, OperationId, WorkerId};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, OperationStageFlags, OrderDirection, UpdateOperationType,
};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::shutdown_guard::ShutdownGuard;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use opentelemetry::KeyValue;
use opentelemetry::baggage::BaggageExt;
use opentelemetry::context::{Context, FutureExt as OtelFutureExt};
use opentelemetry_semantic_conventions::attribute::ENDUSER_ID;
use tokio::sync::{Notify, mpsc};
use tokio::time::Duration;
use tracing::{debug, error, info, info_span, warn};

use crate::api_worker_scheduler::ApiWorkerScheduler;
use crate::awaited_action_db::{AwaitedActionDb, CLIENT_KEEPALIVE_DURATION};
use crate::platform_property_manager::PlatformPropertyManager;
use crate::simple_scheduler_state_manager::SimpleSchedulerStateManager;
use crate::worker::{ActionInfoWithProps, Worker, WorkerTimestamp};
use crate::worker_registry::WorkerRegistry;
use crate::worker_scheduler::WorkerScheduler;

/// Default timeout for workers in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_WORKER_TIMEOUT_S: u64 = 5;

/// Mark operations as completed with error if no client has updated them
/// within this duration.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_CLIENT_ACTION_TIMEOUT_S: u64 = 60;

/// Default maximum number of reserve→commit matches driven concurrently
/// by `do_try_match`. Chosen so the matcher's peak Redis connection usage
/// (roughly one `assign_operation` round-trip per in-flight match) leaves
/// headroom in the pool for subscriber polls and other command flows
/// (`cas.json` `connection_pool_size: 20`). Override via
/// `SimpleSpec::max_concurrent_matches`.
const DEFAULT_MAX_CONCURRENT_MATCHES: usize = 8;

/// Default upper bound on the time between matcher cycles when neither
/// `task_change_notify` nor `worker_change_notify` fires. Acts as a safety
/// net so that any future regression that silently drops notifications, or
/// a per-worker budget leak that makes every worker report
/// `!can_accept_work`, cannot park `do_try_match` indefinitely with a
/// non-empty queue. Override via `SimpleSpec::matcher_safety_net_interval_s`.
const DEFAULT_MATCHER_SAFETY_NET_INTERVAL_S: u64 = 10;

/// Outcome of a single `do_try_match` cycle. Used by the matcher loop to
/// distinguish "made progress" from "saw queued work but couldn't dispatch
/// any of it" — the latter is a leak/starvation signal that bumps the
/// `do_try_match_starved_cycles` counter.
#[derive(Debug, Clone, Copy, Default)]
pub struct DoTryMatchStats {
    /// Number of queued ops the cycle observed when it started.
    pub queued: usize,
    /// Number of those ops that successfully completed `commit_reservation`
    /// during this cycle. `match_one`'s benign no-progress paths (no worker
    /// available, op aborted by state manager) do NOT count as dispatched.
    pub dispatched: usize,
}

/// Per-cycle aggregation of time spent in each phase of `do_try_match`.
/// Populated concurrently by in-flight `match_one` futures; read once at
/// cycle end to emit the slow-cycle warn log.
#[derive(Default)]
struct CyclePhaseMs {
    reserve_pool_ns: AtomicU64,
    assign_ns: AtomicU64,
    commit_pool_ns: AtomicU64,
}

struct SimpleSchedulerActionStateResult {
    client_operation_id: OperationId,
    action_state_result: Box<dyn ActionStateResult>,
}

impl SimpleSchedulerActionStateResult {
    fn new(
        client_operation_id: OperationId,
        action_state_result: Box<dyn ActionStateResult>,
    ) -> Self {
        Self {
            client_operation_id,
            action_state_result,
        }
    }
}

#[async_trait]
impl ActionStateResult for SimpleSchedulerActionStateResult {
    async fn as_state(&self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        let (mut action_state, origin_metadata) = self
            .action_state_result
            .as_state()
            .await
            .err_tip(|| "In SimpleSchedulerActionStateResult")?;
        // We need to ensure the client is not aware of the downstream
        // operation id, so override it before it goes out.
        Arc::make_mut(&mut action_state).client_operation_id = self.client_operation_id.clone();
        Ok((action_state, origin_metadata))
    }

    async fn changed(&mut self) -> Result<(Arc<ActionState>, Option<OriginMetadata>), Error> {
        let (mut action_state, origin_metadata) = self
            .action_state_result
            .changed()
            .await
            .err_tip(|| "In SimpleSchedulerActionStateResult")?;
        // We need to ensure the client is not aware of the downstream
        // operation id, so override it before it goes out.
        Arc::make_mut(&mut action_state).client_operation_id = self.client_operation_id.clone();
        Ok((action_state, origin_metadata))
    }

    async fn as_action_info(&self) -> Result<(Arc<ActionInfo>, Option<OriginMetadata>), Error> {
        self.action_state_result
            .as_action_info()
            .await
            .err_tip(|| "In SimpleSchedulerActionStateResult")
    }
}

/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
#[derive(MetricsComponent)]
pub struct SimpleScheduler {
    /// Manager for matching engine side of the state manager.
    #[metric(group = "matching_engine_state_manager")]
    matching_engine_state_manager: Arc<dyn MatchingEngineStateManager>,

    /// Manager for client state of this scheduler.
    #[metric(group = "client_state_manager")]
    client_state_manager: Arc<dyn ClientStateManager>,

    /// Manager for platform of this scheduler.
    #[metric(group = "platform_properties")]
    platform_property_manager: Arc<PlatformPropertyManager>,

    /// A `Workers` pool that contains all workers that are available to execute actions in a priority
    /// order based on the allocation strategy.
    #[metric(group = "worker_scheduler")]
    worker_scheduler: Arc<ApiWorkerScheduler>,

    /// The sender to send origin events to the origin events.
    maybe_origin_event_tx: Option<mpsc::Sender<OriginEvent>>,

    /// Background task that tries to match actions to workers. If this struct
    /// is dropped the spawn will be cancelled as well.
    task_worker_matching_spawn: JoinHandleDropGuard<()>,

    /// Every duration, do logging of worker matching
    /// e.g. "worker busy", "can't find any worker"
    /// Set to None to disable. This is quite noisy, so we limit it
    worker_match_logging_interval: Option<Duration>,

    /// Runtime-resolved value for the matcher's concurrency ceiling.
    /// Sourced from `SimpleSpec::max_concurrent_matches` (with 0/None
    /// falling back to `DEFAULT_MAX_CONCURRENT_MATCHES`).
    max_concurrent_matches: usize,
}

impl core::fmt::Debug for SimpleScheduler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SimpleScheduler")
            .field("platform_property_manager", &self.platform_property_manager)
            .field("worker_scheduler", &self.worker_scheduler)
            .field("maybe_origin_event_tx", &self.maybe_origin_event_tx)
            .field(
                "task_worker_matching_spawn",
                &self.task_worker_matching_spawn,
            )
            .finish_non_exhaustive()
    }
}

impl SimpleScheduler {
    /// Attempts to find a worker to execute an action and begins executing it.
    /// If an action is already running that is cacheable it may merge this
    /// action with the results and state changes of the already running
    /// action. If the task cannot be executed immediately it will be queued
    /// for execution based on priority and other metrics.
    /// All further updates to the action will be provided through the returned
    /// value.
    async fn inner_add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        let action_state_result = self
            .client_state_manager
            .add_action(client_operation_id.clone(), action_info)
            .await
            .err_tip(|| "In SimpleScheduler::add_action")?;
        Ok(Box::new(SimpleSchedulerActionStateResult::new(
            client_operation_id.clone(),
            action_state_result,
        )))
    }

    async fn inner_filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'_>, Error> {
        self.client_state_manager
            .filter_operations(filter)
            .await
            .err_tip(|| "In SimpleScheduler::find_by_client_operation_id getting filter result")
    }

    async fn get_queued_operations(&self) -> Result<ActionStateResultStream<'_>, Error> {
        let filter = OperationFilter {
            stages: OperationStageFlags::Queued,
            order_by_priority_direction: Some(OrderDirection::Desc),
            ..Default::default()
        };
        self.matching_engine_state_manager
            .filter_operations(filter)
            .await
            .err_tip(|| "In SimpleScheduler::get_queued_operations getting filter result")
    }

    pub async fn do_try_match_for_test(&self) -> Result<DoTryMatchStats, Error> {
        self.do_try_match(true).await
    }

    /// Returns the inner `ApiWorkerScheduler` so callers can observe
    /// scheduler metrics or (in tests) exercise the reserve/commit/release
    /// API directly.
    pub fn worker_scheduler(&self) -> &Arc<ApiWorkerScheduler> {
        &self.worker_scheduler
    }

    /// Returns the matching-engine state manager so tests can drive
    /// `assign_operation(...)` directly — used to verify the five-point
    /// rollback contract (`Err(Code::ResourceExhausted)` returns an op to
    /// `Queued` without bumping `attempts`).
    pub fn matching_engine_state_manager(&self) -> &Arc<dyn MatchingEngineStateManager> {
        &self.matching_engine_state_manager
    }

    /// Runtime-resolved matcher concurrency ceiling. Reflects the value of
    /// `SimpleSpec::max_concurrent_matches` after the `None`/`Some(0)` →
    /// `DEFAULT_MAX_CONCURRENT_MATCHES` fallback.
    pub const fn max_concurrent_matches(&self) -> usize {
        self.max_concurrent_matches
    }

    // FIXME(scheduler-test-hang): this concurrent pipeline (the change
    // landed in 97738436, "feat(scheduler): parallelize match loop with
    // reserve/commit/release + generation fencing") is the prime suspect
    // for the `simple_scheduler_test_test` 300s TIMEOUT with zero stdout
    // documented on the `#[ignore]`'d
    // `action_timeout_is_enforced_backend_side_test` and reproducing for
    // at least `basic_add_action_with_one_worker_test`. Diagnose by
    // running the linux-x86_64 test binary with `tokio-console` (or
    // `RUST_LOG=trace nativelink_scheduler=trace`) attached to confirm
    // whether `match_one` futures are stuck in `FuturesUnordered` polling
    // or whether the outer `task_worker_matching` loop misses an early
    // `task_change_notify.notify_one()` before its `notified()` is
    // registered. See the FIXME block on
    // `tests/simple_scheduler_test.rs::action_timeout_is_enforced_backend_side_test`
    // for the full diagnostic context.
    async fn do_try_match(&self, full_worker_logging: bool) -> Result<DoTryMatchStats, Error> {
        let start = Instant::now();

        // Drain the queued-operations stream into an owned Vec before
        // running the concurrent pipeline. The stream itself borrows the
        // matching-engine state manager (its `'a` lifetime parameter), and
        // `StreamExt::map(...).buffer_unordered(N)` compositions tripped
        // a higher-ranked trait-bound inference that clashed with the
        // outer `spawn!` `'static` requirement. A `Vec` + manual
        // `FuturesUnordered` pump loop sidesteps that and makes the
        // concurrency and priority-order semantics explicit.
        let stream = self
            .get_queued_operations()
            .await
            .err_tip(|| "Failed to get queued operations in do_try_match")?;
        let filter_setup_elapsed = start.elapsed();

        let mut actions: std::collections::VecDeque<Box<dyn ActionStateResult>> =
            stream.collect::<Vec<_>>().await.into();

        let collect_elapsed = start.elapsed();
        let stream_drain_elapsed = collect_elapsed.saturating_sub(filter_setup_elapsed);
        let queued_count = actions.len();
        if collect_elapsed > Duration::from_secs(1) {
            warn!(
                elapsed_ms = collect_elapsed.as_millis(),
                filter_setup_ms = filter_setup_elapsed.as_millis(),
                stream_drain_ms = stream_drain_elapsed.as_millis(),
                queued_count,
                "Slow get_queued_operations query"
            );
        }

        // Drive up to `max_concurrent_matches` matches concurrently across
        // their `.await` chains. Reservations are issued in
        // priority/`state_sort_key` order (stream was ordered; the VecDeque
        // preserves order; we pop from the front). Redis `assign_operation`
        // and commit phases then run in parallel. `reserve_worker_for_action`
        // fences over-subscription under the pool mutex, so concurrency
        // here is safe wrt. worker capacity and budget.
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut result: Result<(), Error> = Ok(());
        let mut dispatched: usize = 0;
        let phase_ms = Arc::new(CyclePhaseMs::default());
        let limit = self.max_concurrent_matches;

        // Initial fill.
        while in_flight.len() < limit {
            let Some(action) = actions.pop_front() else {
                break;
            };
            in_flight.push(match_one(
                action,
                Arc::clone(&self.worker_scheduler),
                Arc::clone(&self.matching_engine_state_manager),
                Arc::clone(&self.platform_property_manager),
                Arc::clone(&phase_ms),
                full_worker_logging,
            ));
        }

        // Drain + refill loop. `match_one` returns Ok(true) only when the
        // commit_reservation succeeded and an action was actually dispatched
        // to a worker. The benign no-progress paths (no worker available,
        // op aborted upstream) return Ok(false), and surface here so the
        // matcher loop can detect "saw work but couldn't place it".
        while let Some(r) = in_flight.next().await {
            match &r {
                Ok(true) => dispatched += 1,
                Ok(false) | Err(_) => {}
            }
            result = result.merge(r.map(|_| ()));
            if let Some(action) = actions.pop_front() {
                in_flight.push(match_one(
                    action,
                    Arc::clone(&self.worker_scheduler),
                    Arc::clone(&self.matching_engine_state_manager),
                    Arc::clone(&self.platform_property_manager),
                    Arc::clone(&phase_ms),
                    full_worker_logging,
                ));
            }
        }

        let total_elapsed = start.elapsed();
        if total_elapsed > Duration::from_secs(5) {
            let ns_to_ms = |ns: u64| ns / 1_000_000;
            warn!(
                total_ms = total_elapsed.as_millis(),
                collect_ms = collect_elapsed.as_millis(),
                filter_setup_ms = filter_setup_elapsed.as_millis(),
                stream_drain_ms = stream_drain_elapsed.as_millis(),
                queued_count,
                reserve_pool_ms = ns_to_ms(phase_ms.reserve_pool_ns.load(Ordering::Relaxed)),
                redis_assign_ms = ns_to_ms(phase_ms.assign_ns.load(Ordering::Relaxed)),
                commit_pool_ms = ns_to_ms(phase_ms.commit_pool_ns.load(Ordering::Relaxed)),
                max_concurrent_matches = limit,
                "Slow do_try_match cycle"
            );
        }

        result.map(|()| DoTryMatchStats {
            queued: queued_count,
            dispatched,
        })
    }
}

/// Per-action reserve → assign → commit pipeline. Owns the action through
/// the full match lifecycle; returns `Ok(())` both on successful commit and
/// on benign abort paths (no worker found, op already assigned elsewhere).
///
/// Rollback contract: on any failure after `assign_operation` has already
/// committed state, we rewrite the op back to `Queued` by re-issuing
/// `assign_operation(Err(Code::ResourceExhausted))` — which the state
/// manager classifies as backpressure (see `simple_scheduler_state_manager.rs`
/// `UpdateWithError` handling) and therefore does NOT bump
/// `awaited_action.attempts`. The worker's debited budget is refunded via
/// `release_reservation`. See the plan's five-point rollback contract.
/// Returns `Ok(true)` iff `commit_reservation` succeeded and the action was
/// dispatched to a worker. The benign no-progress paths (no worker available,
/// op aborted by the state manager) return `Ok(false)` so `do_try_match` can
/// distinguish "made progress" from "saw work but couldn't place it".
async fn match_one(
    action_state_result: Box<dyn ActionStateResult>,
    workers: Arc<ApiWorkerScheduler>,
    state_manager: Arc<dyn MatchingEngineStateManager>,
    ppm: Arc<PlatformPropertyManager>,
    phase_ms: Arc<CyclePhaseMs>,
    full_worker_logging: bool,
) -> Result<bool, Error> {
    let (action_info, maybe_origin_metadata) = action_state_result
        .as_action_info()
        .await
        .err_tip(|| "Failed to get action_info from as_action_info_result stream")?;

    // TODO(palfrey) We should not compute this every time and instead store
    // it with the ActionInfo when we receive it.
    let platform_properties = ppm
        .make_platform_properties(action_info.platform_properties.clone())
        .err_tip(|| "Failed to make platform properties in SimpleScheduler::do_try_match")?;

    let action_info = ActionInfoWithProps {
        inner: action_info,
        platform_properties,
    };

    let reserve_start = Instant::now();
    let maybe_reservation = workers
        .reserve_worker_for_action(&action_info.platform_properties, full_worker_logging)
        .await;
    phase_ms.reserve_pool_ns.fetch_add(
        reserve_start.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );
    let Some(reservation) = maybe_reservation else {
        return Ok(false);
    };

    let worker_id = reservation
        .worker_id()
        .expect("reservation just issued is armed")
        .clone();

    let origin_metadata = maybe_origin_metadata.unwrap_or_default();
    let ctx = Context::current_with_baggage(vec![KeyValue::new(
        ENDUSER_ID,
        origin_metadata.identity,
    )]);

    let attach_fut = async move {
        let operation_id = {
            let (action_state, _origin_metadata) = action_state_result
                .as_state()
                .await
                .err_tip(|| "Failed to get action_info from as_state_result stream")?;
            action_state.client_operation_id.clone()
        };

        let assign_start = Instant::now();
        let assign_result = state_manager
            .assign_operation(&operation_id, Ok(&worker_id))
            .await;
        phase_ms
            .assign_ns
            .fetch_add(assign_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        match assign_result {
            Ok(()) => {
                debug!(%worker_id, %operation_id, ?action_info, "Notifying worker of operation");
                let commit_start = Instant::now();
                let commit_result = workers
                    .commit_reservation(reservation, operation_id.clone(), action_info)
                    .await;
                phase_ms.commit_pool_ns.fetch_add(
                    commit_start.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                match commit_result {
                    Ok(()) => Ok(true),
                    Err((Some(res), commit_err)) => {
                        // Commit failed BEFORE finalize_run mutated any
                        // worker state (generation fence or worker-gone).
                        // The reservation is still armed — roll back the
                        // assign we just committed using a backpressure
                        // error code so `attempts` is not incremented, then
                        // release the reservation to refund the budget.
                        let rollback_err = make_err!(
                            Code::ResourceExhausted,
                            "commit_reservation failed after assign: {commit_err}",
                        );
                        if let Err(rollback_fail) = state_manager
                            .assign_operation(&operation_id, Err(rollback_err))
                            .await
                        {
                            error!(
                                %operation_id,
                                ?rollback_fail,
                                "Failed to roll back assign_operation after commit failure"
                            );
                        }
                        workers.release_reservation(res).await;
                        Err(commit_err)
                            .err_tip(|| "Failed to commit reservation in SimpleScheduler::do_try_match")
                    }
                    Err((None, commit_err)) => {
                        // Commit failed DURING finalize_run (worker
                        // disconnected). `immediate_evict_worker` has already
                        // drained the worker's `running_action_infos` and
                        // requeued this op via `UpdateWithDisconnect` (no
                        // attempts bump). Nothing left to clean up here.
                        Err(commit_err)
                            .err_tip(|| "Failed to commit reservation in SimpleScheduler::do_try_match")
                    }
                }
            }
            Err(assign_err) if assign_err.code == Code::Aborted => {
                // Op was cancelled or already assigned elsewhere; the state
                // manager has already moved on. Release our reservation.
                workers.release_reservation(reservation).await;
                Ok(false)
            }
            Err(assign_err) => {
                // Assign itself failed. State is unchanged or already
                // updated by the state manager; either way we have nothing
                // to roll back. Just release the reservation.
                workers.release_reservation(reservation).await;
                Err(assign_err).err_tip(|| "Failed to assign operation in do_try_match")
            }
        }
    };

    info_span!("do_try_match")
        .in_scope(|| attach_fut)
        .with_context(ctx)
        .await
}

impl SimpleScheduler {
    pub fn new<A: AwaitedActionDb>(
        spec: &SimpleSpec,
        awaited_action_db: A,
        task_change_notify: Arc<Notify>,
        maybe_origin_event_tx: Option<mpsc::Sender<OriginEvent>>,
    ) -> (Arc<Self>, Arc<dyn WorkerScheduler>) {
        Self::new_with_callback(
            spec,
            awaited_action_db,
            || {
                // The cost of running `do_try_match()` is very high, but constant
                // in relation to the number of changes that have happened. This
                // means that grabbing this lock to process `do_try_match()` should
                // always yield to any other tasks that might want the lock. The
                // easiest and most fair way to do this is to sleep for a small
                // amount of time. Using something like tokio::task::yield_now()
                // does not yield as aggressively as we'd like if new futures are
                // scheduled within a future.
                tokio::time::sleep(Duration::from_millis(1))
            },
            task_change_notify,
            SystemTime::now,
            maybe_origin_event_tx,
        )
    }

    pub fn new_with_callback<
        Fut: Future<Output = ()> + Send,
        F: Fn() -> Fut + Send + Sync + 'static,
        A: AwaitedActionDb,
        I: InstantWrapper,
        NowFn: Fn() -> I + Clone + Send + Unpin + Sync + 'static,
    >(
        spec: &SimpleSpec,
        awaited_action_db: A,
        on_matching_engine_run: F,
        task_change_notify: Arc<Notify>,
        now_fn: NowFn,
        maybe_origin_event_tx: Option<mpsc::Sender<OriginEvent>>,
    ) -> (Arc<Self>, Arc<dyn WorkerScheduler>) {
        let platform_property_manager = Arc::new(PlatformPropertyManager::new(
            spec.supported_platform_properties
                .clone()
                .unwrap_or_default(),
        ));

        let mut worker_timeout_s = spec.worker_timeout_s;
        if worker_timeout_s == 0 {
            worker_timeout_s = DEFAULT_WORKER_TIMEOUT_S;
        }

        let mut client_action_timeout_s = spec.client_action_timeout_s;
        if client_action_timeout_s == 0 {
            client_action_timeout_s = DEFAULT_CLIENT_ACTION_TIMEOUT_S;
        }
        // This matches the value of CLIENT_KEEPALIVE_DURATION which means that
        // tasks are going to be dropped all over the place, this isn't a good
        // setting.
        if client_action_timeout_s <= CLIENT_KEEPALIVE_DURATION.as_secs() {
            error!(
                client_action_timeout_s,
                "Setting client_action_timeout_s to less than the client keep alive interval is going to cause issues, please set above {}.",
                CLIENT_KEEPALIVE_DURATION.as_secs()
            );
        }

        let max_job_retries = spec.max_job_retries;

        let max_concurrent_matches = spec
            .max_concurrent_matches
            .map(|v| v as usize)
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MAX_CONCURRENT_MATCHES);
        info!(
            max_concurrent_matches,
            "scheduler matcher concurrency resolved"
        );

        let safety_net_interval_s = spec
            .matcher_safety_net_interval_s
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MATCHER_SAFETY_NET_INTERVAL_S);
        let safety_net_interval = Duration::from_secs(safety_net_interval_s);
        info!(
            safety_net_interval_s,
            "matcher safety-net interval resolved"
        );

        let worker_change_notify = Arc::new(Notify::new());

        // Create shared worker registry for single heartbeat per worker.
        let worker_registry = Arc::new(WorkerRegistry::new());

        let state_manager = SimpleSchedulerStateManager::new(
            max_job_retries,
            Duration::from_secs(worker_timeout_s),
            Duration::from_secs(client_action_timeout_s),
            Duration::from_secs(spec.max_action_executing_timeout_s),
            awaited_action_db,
            now_fn,
            Some(worker_registry.clone()),
        );

        let worker_scheduler = ApiWorkerScheduler::new(
            state_manager.clone(),
            platform_property_manager.clone(),
            spec.allocation_strategy,
            worker_change_notify.clone(),
            worker_timeout_s,
            worker_registry,
        );

        let worker_scheduler_clone = worker_scheduler.clone();

        let action_scheduler = Arc::new_cyclic(move |weak_self| -> Self {
            let weak_inner = weak_self.clone();
            let task_worker_matching_spawn =
                spawn!("simple_scheduler_task_worker_matching", async move {
                    let mut last_match_successful = true;
                    let mut worker_match_logging_last: Option<Instant> = None;
                    // Safety-net interval: ensures the matcher re-runs at least
                    // every `safety_net_interval` even if neither
                    // `task_change_notify` nor `worker_change_notify` fires. Burn
                    // the immediate-fire first tick so startup doesn't trigger a
                    // bogus interval-driven cycle before any state exists.
                    let mut safety_net_interval_timer =
                        tokio::time::interval(safety_net_interval);
                    safety_net_interval_timer.set_missed_tick_behavior(
                        tokio::time::MissedTickBehavior::Delay,
                    );
                    safety_net_interval_timer.tick().await;
                    // Break out of the loop only when the inner is dropped.
                    loop {
                        let task_change_fut = task_change_notify.notified();
                        let worker_change_fut = worker_change_notify.notified();
                        tokio::pin!(task_change_fut);
                        tokio::pin!(worker_change_fut);
                        let state_changed = future::select(task_change_fut, worker_change_fut);
                        // `interval_driven` flips true iff the safety-net
                        // interval fired before either notify or the
                        // last-match-failed backoff sleep. Used to bump the
                        // `matcher_interval_kicks` metric and gate a one-line
                        // diagnostic log on non-empty queues — confirms the
                        // safety net is what woke us, not a notify.
                        let mut interval_driven = false;
                        if last_match_successful {
                            tokio::select! {
                                _ = state_changed => {}
                                _ = safety_net_interval_timer.tick() => {
                                    interval_driven = true;
                                }
                            }
                        } else {
                            // If the last match failed, then run again after a short sleep.
                            // This resolves issues where we tried to re-schedule a job to
                            // a disconnected worker.  The sleep ensures we don't enter a
                            // hard loop if there's something wrong inside do_try_match.
                            let sleep_fut = tokio::time::sleep(Duration::from_millis(100));
                            tokio::pin!(sleep_fut);
                            let backoff_or_change =
                                future::select(state_changed, sleep_fut);
                            tokio::select! {
                                _ = backoff_or_change => {}
                                _ = safety_net_interval_timer.tick() => {
                                    interval_driven = true;
                                }
                            }
                        }

                        let result = match weak_inner.upgrade() {
                            Some(scheduler) => {
                                let now = Instant::now();
                                let full_worker_logging = {
                                    match scheduler.worker_match_logging_interval {
                                        None => false,
                                        Some(duration) => match worker_match_logging_last {
                                            None => true,
                                            Some(when) => now.duration_since(when) >= duration,
                                        },
                                    }
                                };

                                let res = scheduler.do_try_match(full_worker_logging).await;
                                if full_worker_logging {
                                    let operations_stream = scheduler
                                        .matching_engine_state_manager
                                        .filter_operations(OperationFilter::default())
                                        .await
                                        .err_tip(|| "In action_scheduler getting filter result");

                                    let mut oldest_actions_in_state: HashMap<
                                        String,
                                        BTreeSet<Arc<ActionState>>,
                                    > = HashMap::new();
                                    let max_items = 5;

                                    match operations_stream {
                                        Ok(stream) => {
                                            let actions = stream
                                                .filter_map(|item| async move {
                                                    match item.as_ref().as_state().await {
                                                        Ok((action_state, _origin_metadata)) => {
                                                            Some(action_state)
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                ?e,
                                                                "Failed to get action state!"
                                                            );
                                                            None
                                                        }
                                                    }
                                                })
                                                .collect::<Vec<_>>()
                                                .await;
                                            for action_state in &actions {
                                                let name = action_state.stage.name();
                                                if let Some(values) =
                                                    oldest_actions_in_state.get_mut(&name)
                                                {
                                                    values.insert(action_state.clone());
                                                    if values.len() > max_items {
                                                        values.pop_first();
                                                    }
                                                } else {
                                                    let mut values = BTreeSet::new();
                                                    values.insert(action_state.clone());
                                                    oldest_actions_in_state.insert(name, values);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(?e, "Failed to get operations list!");
                                        }
                                    }

                                    for value in oldest_actions_in_state.values() {
                                        let mut items = vec![];
                                        for item in value {
                                            items.push(item.to_string());
                                        }
                                        info!(?items, "Oldest actions in state");
                                    }

                                    worker_match_logging_last.replace(now);
                                }
                                res
                            }
                            // If the inner went away it means the scheduler is shutting
                            // down, so we need to resolve our future.
                            None => return,
                        };
                        // `result` is `Result<DoTryMatchStats, Error>`. We
                        // examine the stats independently of error status
                        // because either branch is informative for telemetry.
                        let metrics = match weak_inner.upgrade() {
                            Some(scheduler) => Some(Arc::clone(
                                scheduler.worker_scheduler.get_metrics(),
                            )),
                            None => None,
                        };
                        match &result {
                            Ok(stats) => {
                                if interval_driven {
                                    if let Some(m) = metrics.as_ref() {
                                        m.matcher_interval_kicks
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    if stats.queued > 0 {
                                        info!(
                                            queued = stats.queued,
                                            dispatched = stats.dispatched,
                                            interval_s = safety_net_interval_s,
                                            "matcher safety-net tick on non-empty queue"
                                        );
                                    }
                                }
                                if stats.queued > 0 && stats.dispatched == 0 {
                                    if let Some(m) = metrics.as_ref() {
                                        m.do_try_match_starved_cycles
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(err) => {
                                error!(?err, "Error while running do_try_match");
                            }
                        }
                        last_match_successful = result.is_ok();

                        on_matching_engine_run().await;
                    }
                    // Unreachable.
                });

            let worker_match_logging_interval = match spec.worker_match_logging_interval_s {
                // -1 or 0 means disabled (0 used to cause expensive logging on every call)
                -1 | 0 => None,
                signed_secs => {
                    if let Ok(secs) = TryInto::<u64>::try_into(signed_secs) {
                        Some(Duration::from_secs(secs))
                    } else {
                        error!(
                            worker_match_logging_interval_s = spec.worker_match_logging_interval_s,
                            "Valid values for worker_match_logging_interval_s are -1, 0, or a positive integer, setting to disabled",
                        );
                        None
                    }
                }
            };
            Self {
                matching_engine_state_manager: state_manager.clone(),
                client_state_manager: state_manager.clone(),
                worker_scheduler,
                platform_property_manager,
                maybe_origin_event_tx,
                task_worker_matching_spawn,
                worker_match_logging_interval,
                max_concurrent_matches,
            }
        });
        (action_scheduler, worker_scheduler_clone)
    }
}

#[async_trait]
impl ClientStateManager for SimpleScheduler {
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
    ) -> Result<Box<dyn ActionStateResult>, Error> {
        self.inner_add_action(client_operation_id, action_info)
            .await
    }

    async fn filter_operations<'a>(
        &'a self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream<'a>, Error> {
        self.inner_filter_operations(filter).await
    }

    fn as_known_platform_property_provider(&self) -> Option<&dyn KnownPlatformPropertyProvider> {
        Some(self)
    }
}

#[async_trait]
impl KnownPlatformPropertyProvider for SimpleScheduler {
    async fn get_known_properties(&self, _instance_name: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .worker_scheduler
            .get_platform_property_manager()
            .get_known_properties()
            .keys()
            .cloned()
            .collect())
    }
}

#[async_trait]
impl WorkerScheduler for SimpleScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.worker_scheduler.get_platform_property_manager()
    }

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        self.worker_scheduler.add_worker(worker).await
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        operation_id: &OperationId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .update_action(worker_id, operation_id, update)
            .await
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        self.worker_scheduler
            .worker_keep_alive_received(worker_id, timestamp)
            .await
    }

    async fn remove_worker(&self, worker_id: &WorkerId, reason: Error) -> Result<(), Error> {
        self.worker_scheduler.remove_worker(worker_id, reason).await
    }

    async fn shutdown(&self, shutdown_guard: ShutdownGuard) {
        self.worker_scheduler.shutdown(shutdown_guard).await;
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        self.worker_scheduler
            .remove_timedout_workers(now_timestamp)
            .await
    }

    async fn set_drain_worker(&self, worker_id: &WorkerId, is_draining: bool) -> Result<(), Error> {
        self.worker_scheduler
            .set_drain_worker(worker_id, is_draining)
            .await
    }
}

impl RootMetricsComponent for SimpleScheduler {}
