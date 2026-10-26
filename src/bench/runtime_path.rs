//! Runtime-path benchmark driver.
//!
//! The runtime has no read API, so the single producer keeps a shadow
//! [`MatchingEngine`] current from each acknowledgement. The shadow is only
//! touched after both latency samples for a command have been taken, so
//! book-aware workload generation and bookkeeping never enter a timed window.
//!
//! One `--in-flight != 1` pipeline submits several commands before draining
//! acknowledgements. Because the shadow cannot see commands still in flight,
//! its book may lag by up to `in_flight - 1` commands in throughput mode; the
//! latency mode (`--in-flight 1`) keeps it exact.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use crate::durable::{
    EventLog, EventSnapshotStore, FileEventLog, FileEventSnapshotStore, InMemoryEventLog,
    InMemoryEventSnapshotStore,
};
use crate::engine::MatchingEngine;
use crate::runtime::{
    EngineHandle, EngineRuntime, RuntimeError, RuntimeMetrics, SubmitError, Ticket,
};
use crate::types::{EngineCommand, EngineConfig};

use super::config::{BenchConfig, BenchError, LogKind};
use super::report::{
    APPLIED_SLO_NS, BenchReport, CategoryStats, DURABLE_SLO_NS, EnvInfo, LatencyMetric,
    LatencyReport, RunStats, RuntimeReport, SloCheck, SloReport,
};
use super::run::{
    book_prices, command_from_action, count_depth, duration_ns, events_per_command, intended_time,
    ns_between, pace, record_workload, symbol_list,
};
use super::stats::Samples;
use super::workload::{Action, CommandKind, Workload};

pub(crate) fn run_runtime(config: &BenchConfig) -> Result<BenchReport, BenchError> {
    let symbols = symbol_list(config.symbols);
    let metrics = Arc::new(RuntimeMetrics::new());
    let (handle, runtime) = create_runtime(config, metrics.clone())?;

    // The runtime owns the real engine; this shadow is the producer's only way
    // to answer book-aware workload questions.
    let mut shadow = MatchingEngine::new(EngineConfig::default());
    let mut workload = Workload::new(config);

    let warmup_start = Instant::now();
    drive(
        config,
        &handle,
        &mut shadow,
        &mut workload,
        &symbols,
        config.warmup,
        false,
        warmup_start,
    )?;

    workload.start_measurement();

    let baseline = MetricsSnapshot::read(&metrics);
    let trades_before = shadow.trades().len();

    let measured_start = Instant::now();
    let mut stats = drive(
        config,
        &handle,
        &mut shadow,
        &mut workload,
        &symbols,
        config.events,
        true,
        measured_start,
    )?;
    let wall_time_ns = duration_ns(measured_start.elapsed());
    let delta = MetricsSnapshot::read(&metrics).delta(&baseline);

    // The runtime's own counter is the domain-event volume. The shadow still
    // drives the workload, but `MatchingEngine::execute` records the legacy
    // `EngineEvent` shape, which is not the event-sourced volume this report
    // must show. Ticket 09 removes that legacy path.
    let engine_events = delta.event_records;
    let trades = (shadow.trades().len() - trades_before) as u64;
    let final_depth_levels = count_depth(&shadow, &symbols);

    let applied_all = stats.applied_all.summary();
    let applied_limit = stats.applied_limit.summary();
    let applied_market = stats.applied_market.summary();
    let applied_cancel = stats.applied_cancel.summary();
    let durable_all = stats.durable.summary();
    let send_lag = stats.send_lag.summary();

    let total = CategoryStats {
        all: applied_all,
        limit: applied_limit,
        market: applied_market,
        cancel: applied_cancel,
    };
    // The direct report's `service` row is the time spent inside the engine.
    // The runtime cannot separate that from the applied wait without racing a
    // clock inside the worker, so it mirrors applied and the detailed apply
    // service metric is reported separately.
    let service = CategoryStats {
        all: applied_all,
        limit: applied_limit,
        market: applied_market,
        cancel: applied_cancel,
    };

    let achieved_rate = if wall_time_ns == 0 {
        0.0
    } else {
        config.events as f64 / (wall_time_ns as f64 / 1e9)
    };

    let runtime_report = RuntimeReport {
        applied_latency: applied_all,
        durable_latency: durable_all,
        queue_wait: LatencyMetric {
            mean_ns: mean(delta.queue_wait_ns_sum, delta.applied),
            max_ns: delta.queue_wait_ns_max,
        },
        apply_service: LatencyMetric {
            mean_ns: mean(delta.apply_ns_sum, delta.applied),
            max_ns: delta.apply_ns_max,
        },
        durable_wait: LatencyMetric {
            mean_ns: mean(delta.durable_wait_ns_sum, delta.durable),
            max_ns: delta.durable_wait_ns_max,
        },
        journal_append: LatencyMetric {
            mean_ns: mean(delta.journal_append_ns_sum, delta.journal_append_count),
            max_ns: delta.journal_append_ns_max,
        },
        batch_size_mean: if delta.batch_count == 0 {
            0.0
        } else {
            delta.batch_records_sum as f64 / delta.batch_count as f64
        },
        batch_size_max: delta.batch_records_max,
        batch_count: delta.batch_count,
        events_total: delta.event_records,
        events_per_command: events_per_command(delta.event_records, delta.applied),
        queue_depth_max: delta.queue_depth_max,
        queue_full: delta.queue_full,
        fsync_share: ratio(delta.journal_append_ns_sum, delta.durable_wait_ns_sum),
        applied_count: delta.applied,
        durable_count: delta.durable,
        slo: SloReport {
            applied: SloCheck::new(applied_all, APPLIED_SLO_NS, delta.queue_full),
            // The durable SLO is defined against FileEventLog on NVMe. An
            // in-memory log cannot exercise fsync, so reporting PASS/FAIL there
            // would be a false claim.
            durable: if config.log == LogKind::Memory {
                SloCheck::not_applicable(durable_all, DURABLE_SLO_NS, delta.queue_full)
            } else {
                SloCheck::new(durable_all, DURABLE_SLO_NS, delta.queue_full)
            },
        },
    };

    runtime
        .shutdown()
        .map_err(|error| BenchError::Io(format!("runtime shutdown failed: {error}")))?;

    Ok(BenchReport {
        config: config.clone(),
        run: RunStats {
            wall_time_ns,
            achieved_rate,
            commands: config.events,
            engine_events,
            events_per_command: events_per_command(engine_events, config.events),
            trades,
            rejections: workload.rejections(),
            final_open_orders: workload.open_len(),
            peak_open_orders: workload.peak_open_orders(),
            final_depth_levels,
        },
        latency: LatencyReport {
            total,
            service,
            send_lag,
        },
        runtime: Some(runtime_report),
        decide: None,
        env: EnvInfo::current(),
    })
}

fn create_runtime(
    config: &BenchConfig,
    metrics: Arc<RuntimeMetrics>,
) -> Result<(EngineHandle, EngineRuntime), BenchError> {
    let runtime_config = config.runtime_config(metrics);

    let (log, snapshots): (Box<dyn EventLog>, Box<dyn EventSnapshotStore>) = match config.log {
        LogKind::Memory => (
            Box::new(InMemoryEventLog::new()),
            Box::new(InMemoryEventSnapshotStore::new()),
        ),
        LogKind::File => {
            let dir = config
                .data_dir
                .as_ref()
                .ok_or_else(|| BenchError::usage("--log file requires --data-dir <PATH>"))?;
            std::fs::create_dir_all(dir).map_err(|error| {
                BenchError::Io(format!(
                    "cannot create data directory {}: {error}",
                    dir.display()
                ))
            })?;

            let log_path = dir.join("events.log");
            let snapshot_dir = dir.join("snapshots");
            let log = FileEventLog::open(&log_path).map_err(|error| {
                BenchError::Io(format!("cannot open {}: {error}", log_path.display()))
            })?;
            let snapshots = FileEventSnapshotStore::open(&snapshot_dir).map_err(|error| {
                BenchError::Io(format!(
                    "cannot open snapshot directory {}: {error}",
                    snapshot_dir.display()
                ))
            })?;
            (Box::new(log), Box::new(snapshots))
        }
    };

    EngineRuntime::create(runtime_config, EngineConfig::default(), log, snapshots)
        .map_err(|error| BenchError::Io(format!("runtime startup failed: {error}")))
}

struct Pending {
    submit_start: Instant,
    send_lag_ns: u64,
    ticket: Ticket,
    /// Kept so the shadow can apply the command after its timed window closes.
    command: EngineCommand,
    action: Action,
}

#[derive(Default)]
struct DriveStats {
    applied_all: Samples,
    applied_limit: Samples,
    applied_market: Samples,
    applied_cancel: Samples,
    durable: Samples,
    send_lag: Samples,
}

/// Drive one phase (warmup or measured) through the runtime.
#[allow(clippy::too_many_arguments)]
fn drive(
    config: &BenchConfig,
    handle: &EngineHandle,
    shadow: &mut MatchingEngine,
    workload: &mut Workload,
    symbols: &[String],
    commands: u64,
    measure: bool,
    phase_start: Instant,
) -> Result<DriveStats, BenchError> {
    let in_flight = config.in_flight.max(1);
    let mut stats = DriveStats::default();
    let mut pending = VecDeque::with_capacity(in_flight);

    for index in 0..commands {
        while pending.len() >= in_flight {
            drain_one(shadow, workload, &mut pending, &mut stats, measure)?;
        }

        let action =
            workload.next_action(|symbol_index| book_prices(shadow, symbols, symbol_index));
        let command = command_from_action(&action, symbols);
        let intended = if config.rate > 0 {
            Some(intended_time(phase_start, index, config.rate))
        } else {
            None
        };
        if let Some(intended) = intended {
            pace(intended);
        }

        let submit_start = Instant::now();
        let send_lag_ns = intended.map_or(0, |intended| ns_between(intended, submit_start));
        let ticket = submit_with_retry(handle, &command)?;
        pending.push_back(Pending {
            submit_start,
            send_lag_ns,
            ticket,
            command,
            action,
        });
    }

    while !pending.is_empty() {
        drain_one(shadow, workload, &mut pending, &mut stats, measure)?;
    }

    Ok(stats)
}

/// Drain the oldest acknowledgement, taking both latency samples before any
/// shadow bookkeeping runs.
fn drain_one(
    shadow: &mut MatchingEngine,
    workload: &mut Workload,
    pending: &mut VecDeque<Pending>,
    stats: &mut DriveStats,
    measure: bool,
) -> Result<(), BenchError> {
    let Pending {
        submit_start,
        send_lag_ns,
        ticket,
        command,
        action,
    } = pending
        .pop_front()
        .expect("the caller checked for pending work");

    let applied_receipt = ticket.wait_applied().map_err(wait_error)?;
    let applied_ns = ns_between(submit_start, Instant::now());
    let durable_receipt = ticket.wait_durable().map_err(wait_error)?;
    let durable_ns = ns_between(submit_start, Instant::now());

    debug_assert_eq!(
        applied_receipt.last_event_sequence, durable_receipt.last_event_sequence,
        "per-producer FIFO must preserve the event range across levels"
    );

    if measure {
        stats.applied_all.push(applied_ns);
        match action.kind {
            CommandKind::Limit => stats.applied_limit.push(applied_ns),
            CommandKind::Market => stats.applied_market.push(applied_ns),
            CommandKind::Cancel => stats.applied_cancel.push(applied_ns),
        }
        stats.durable.push(durable_ns);
        stats.send_lag.push(send_lag_ns);
    }

    let outcome = shadow.execute(command);
    record_workload(workload, &action, outcome, |id| shadow.is_open(id));
    Ok(())
}

fn submit_with_retry(handle: &EngineHandle, command: &EngineCommand) -> Result<Ticket, BenchError> {
    loop {
        match handle.try_submit(command.clone()) {
            Ok(ticket) => return Ok(ticket),
            // The queue and slot pool are bounded on purpose. The bench keeps
            // retrying so the workload completes, but every refusal is counted
            // in RuntimeMetrics and reported as queue_full.
            Err(SubmitError::QueueFull) => std::hint::spin_loop(),
            Err(SubmitError::Shutdown) => {
                return Err(BenchError::Io(
                    "runtime shut down before the workload completed".to_string(),
                ));
            }
            Err(SubmitError::EngineFailed(error)) => {
                return Err(BenchError::Io(format!("runtime failed: {error}")));
            }
        }
    }
}

fn wait_error(error: RuntimeError) -> BenchError {
    BenchError::Io(format!("runtime acknowledgement failed: {error}"))
}

fn mean(sum_ns: u64, count: u64) -> u64 {
    if count == 0 { 0 } else { sum_ns / count }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// One read of the runtime's atomic counters.
///
/// Sums and counts are differenced against a post-warmup baseline so the
/// reported means cover the measured phase only. Maxima cannot be differenced,
/// so they are the largest value observed for the whole process lifetime,
/// including warmup.
#[derive(Debug, Clone, Copy, Default)]
struct MetricsSnapshot {
    applied: u64,
    durable: u64,
    queue_full: u64,
    queue_wait_ns_sum: u64,
    queue_wait_ns_max: u64,
    apply_ns_sum: u64,
    apply_ns_max: u64,
    durable_wait_ns_sum: u64,
    durable_wait_ns_max: u64,
    journal_append_ns_sum: u64,
    journal_append_ns_max: u64,
    journal_append_count: u64,
    batch_records_sum: u64,
    batch_records_max: u64,
    batch_count: u64,
    event_records: u64,
    queue_depth_max: u64,
}

impl MetricsSnapshot {
    fn read(metrics: &RuntimeMetrics) -> Self {
        Self {
            applied: metrics.applied(),
            durable: metrics.durable(),
            queue_full: metrics.queue_full(),
            queue_wait_ns_sum: metrics.queue_wait_ns_sum(),
            queue_wait_ns_max: metrics.queue_wait_ns_max(),
            apply_ns_sum: metrics.apply_ns_sum(),
            apply_ns_max: metrics.apply_ns_max(),
            durable_wait_ns_sum: metrics.durable_wait_ns_sum(),
            durable_wait_ns_max: metrics.durable_wait_ns_max(),
            journal_append_ns_sum: metrics.journal_append_ns_sum(),
            journal_append_ns_max: metrics.journal_append_ns_max(),
            journal_append_count: metrics.journal_append_count(),
            batch_records_sum: metrics.batch_records_sum(),
            batch_records_max: metrics.batch_records_max(),
            batch_count: metrics.batch_count(),
            event_records: metrics.event_records(),
            queue_depth_max: metrics.queue_depth_max(),
        }
    }

    fn delta(self, before: &Self) -> Self {
        Self {
            applied: self.applied.saturating_sub(before.applied),
            durable: self.durable.saturating_sub(before.durable),
            queue_full: self.queue_full.saturating_sub(before.queue_full),
            queue_wait_ns_sum: self
                .queue_wait_ns_sum
                .saturating_sub(before.queue_wait_ns_sum),
            queue_wait_ns_max: self.queue_wait_ns_max,
            apply_ns_sum: self.apply_ns_sum.saturating_sub(before.apply_ns_sum),
            apply_ns_max: self.apply_ns_max,
            durable_wait_ns_sum: self
                .durable_wait_ns_sum
                .saturating_sub(before.durable_wait_ns_sum),
            durable_wait_ns_max: self.durable_wait_ns_max,
            journal_append_ns_sum: self
                .journal_append_ns_sum
                .saturating_sub(before.journal_append_ns_sum),
            journal_append_ns_max: self.journal_append_ns_max,
            journal_append_count: self
                .journal_append_count
                .saturating_sub(before.journal_append_count),
            batch_records_sum: self
                .batch_records_sum
                .saturating_sub(before.batch_records_sum),
            batch_records_max: self.batch_records_max,
            batch_count: self.batch_count.saturating_sub(before.batch_count),
            event_records: self.event_records.saturating_sub(before.event_records),
            queue_depth_max: self.queue_depth_max,
        }
    }
}
