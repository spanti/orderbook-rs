//! Benchmark orchestration: warmup, paced measurement, and latency collection.

use std::time::{Duration, Instant};

use crate::engine::MatchingEngine;
use crate::orderbook::OrderBook;
use crate::types::{
    CommandOutcome, EngineCommand, EngineConfig, OrderBookId, OrderRequest, OrderType, Side,
    TimeInForce,
};

use super::config::{BenchConfig, BenchError, BenchPath};
use super::report::{BenchReport, CategoryStats, DepthLevels, EnvInfo, LatencyReport, RunStats};
use super::stats::Samples;
use super::workload::{Action, CommandKind, SYMBOL_PREFIX, Workload};

const DEPTH_SCAN_CAP: u64 = 10_000;
const SPIN_THRESHOLD: Duration = Duration::from_millis(1);

pub fn run(config: &BenchConfig) -> Result<BenchReport, BenchError> {
    config.validate()?;
    match config.path {
        BenchPath::Direct => run_direct(config),
        BenchPath::Runtime => super::runtime_path::run_runtime(config),
    }
}

fn run_direct(config: &BenchConfig) -> Result<BenchReport, BenchError> {
    let symbols = symbol_list(config.symbols);
    let mut engine = MatchingEngine::new(EngineConfig::default());
    let mut workload = Workload::new(config);

    let warmup_start = Instant::now();
    for index in 0..config.warmup {
        let action =
            workload.next_action(|symbol_index| book_prices(&engine, &symbols, symbol_index));
        let command = command_from_action(&action, &symbols);
        if config.rate > 0 {
            pace(intended_time(warmup_start, index, config.rate));
        }
        let outcome = engine.execute(command);
        std::hint::black_box(&outcome);
        record_workload(&mut workload, &action, outcome, |id| engine.is_open(id));
    }

    let mut total_all = Samples::default();
    let mut total_limit = Samples::default();
    let mut total_market = Samples::default();
    let mut total_cancel = Samples::default();
    let mut service_all = Samples::default();
    let mut service_limit = Samples::default();
    let mut service_market = Samples::default();
    let mut service_cancel = Samples::default();
    let mut send_lag_samples = Samples::default();

    workload.start_measurement();

    let measured_start = Instant::now();
    let events_before = engine.events().len();
    let trades_before = engine.trades().len();

    for index in 0..config.events {
        let action =
            workload.next_action(|symbol_index| book_prices(&engine, &symbols, symbol_index));
        let command = command_from_action(&action, &symbols);
        let intended = if config.rate > 0 {
            Some(intended_time(measured_start, index, config.rate))
        } else {
            None
        };
        if let Some(intended) = intended {
            pace(intended);
        }

        let send = Instant::now();
        let outcome = engine.execute(command);
        let done = Instant::now();
        std::hint::black_box(&outcome);

        let service = ns_between(send, done);
        let (total, send_lag) = match intended {
            Some(intended) => (ns_between(intended, done), ns_between(intended, send)),
            None => (service, 0),
        };

        total_all.push(total);
        service_all.push(service);
        send_lag_samples.push(send_lag);
        match action.kind {
            CommandKind::Limit => {
                total_limit.push(total);
                service_limit.push(service);
            }
            CommandKind::Market => {
                total_market.push(total);
                service_market.push(service);
            }
            CommandKind::Cancel => {
                total_cancel.push(total);
                service_cancel.push(service);
            }
        }

        record_workload(&mut workload, &action, outcome, |id| engine.is_open(id));
    }

    let wall_time_ns = duration_ns(measured_start.elapsed());
    let engine_events = (engine.events().len() - events_before) as u64;
    let trades = (engine.trades().len() - trades_before) as u64;
    let final_depth_levels = count_depth(&engine, &symbols);

    let latency = LatencyReport {
        total: CategoryStats {
            all: total_all.summary(),
            limit: total_limit.summary(),
            market: total_market.summary(),
            cancel: total_cancel.summary(),
        },
        service: CategoryStats {
            all: service_all.summary(),
            limit: service_limit.summary(),
            market: service_market.summary(),
            cancel: service_cancel.summary(),
        },
        send_lag: send_lag_samples.summary(),
    };

    let achieved_rate = if wall_time_ns == 0 {
        0.0
    } else {
        config.events as f64 / (wall_time_ns as f64 / 1e9)
    };

    Ok(BenchReport {
        config: config.clone(),
        run: RunStats {
            wall_time_ns,
            achieved_rate,
            commands: config.events,
            engine_events,
            trades,
            rejections: workload.rejections(),
            final_open_orders: workload.open_len(),
            peak_open_orders: workload.peak_open_orders(),
            final_depth_levels,
        },
        latency,
        runtime: None,
        env: EnvInfo::current(),
    })
}

/// The bench's symbol names, in workload-index order.
pub(crate) fn symbol_list(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{SYMBOL_PREFIX}{index}"))
        .collect()
}

/// Adapt a workload action into the engine's command vocabulary.
///
/// Cancel actions carry placeholder side/price/quantity; those are ignored.
/// Submit actions set the same order type and time-in-force the old mapping
/// used: Limit+GTC, Market+IOC, never post-only.
pub(crate) fn command_from_action(action: &Action, symbols: &[String]) -> EngineCommand {
    let symbol = symbols[action.symbol_index].clone();
    match action.kind {
        CommandKind::Limit => EngineCommand::SubmitOrder {
            symbol,
            request: OrderRequest {
                side: action.side,
                price: action.price,
                quantity: action.quantity,
                timestamp: action.timestamp,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                post_only: false,
            },
        },
        CommandKind::Market => EngineCommand::SubmitOrder {
            symbol,
            request: OrderRequest {
                side: action.side,
                price: action.price,
                quantity: action.quantity,
                timestamp: action.timestamp,
                order_type: OrderType::Market,
                time_in_force: TimeInForce::IOC,
                post_only: false,
            },
        },
        CommandKind::Cancel => EngineCommand::CancelOrder {
            symbol,
            order_id: action
                .target_order_id
                .expect("cancel actions carry a target order id"),
        },
    }
}

/// Feed one command outcome back into the workload after the timed command
/// window has closed. The engine's lifecycle query is deliberately evaluated
/// here, not inside `send..done`, so bookkeeping never enters a latency sample.
pub(crate) fn record_workload(
    workload: &mut Workload,
    action: &Action,
    outcome: CommandOutcome,
    is_open: impl Fn(OrderBookId) -> bool,
) {
    match outcome {
        CommandOutcome::Accepted(report) => workload.on_submit_result(action, &Ok(report), is_open),
        CommandOutcome::Canceled(report) => workload.on_cancel_result(action, Some(&report)),
        CommandOutcome::Rejected(reason) => {
            if action.kind == CommandKind::Cancel {
                workload.on_cancel_result(action, None);
            } else {
                workload.on_submit_result(action, &Err(reason), is_open);
            }
        }
    }
}

pub(crate) fn book_prices(
    engine: &MatchingEngine,
    symbols: &[String],
    symbol_index: usize,
) -> (Option<u64>, Option<u64>) {
    engine
        .book(&symbols[symbol_index])
        .map_or((None, None), |book| (book.best_bid(), book.best_ask()))
}

pub(crate) fn intended_time(start: Instant, index: u64, rate: u64) -> Instant {
    let offset_ns = (index as u128 * 1_000_000_000u128 / rate as u128).min(u64::MAX as u128) as u64;
    start
        .checked_add(Duration::from_nanos(offset_ns))
        .unwrap_or(start)
}

pub(crate) fn pace(intended: Instant) {
    let now = Instant::now();
    if now >= intended {
        return;
    }

    let remaining = intended - now;
    if remaining > SPIN_THRESHOLD {
        std::thread::sleep(remaining - SPIN_THRESHOLD);
    }

    while Instant::now() < intended {
        std::hint::spin_loop();
    }
}

pub(crate) fn ns_between(start: Instant, end: Instant) -> u64 {
    duration_ns(end.saturating_duration_since(start))
}

pub(crate) fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

pub(crate) fn count_depth(engine: &MatchingEngine, symbols: &[String]) -> DepthLevels {
    let mut bids = 0;
    let mut asks = 0;
    let mut bids_capped = false;
    let mut asks_capped = false;

    for symbol in symbols {
        if let Some(book) = engine.book(symbol) {
            let (bid_levels, bid_capped) = scan_side(book, Side::Bid);
            let (ask_levels, ask_capped) = scan_side(book, Side::Ask);
            bids += bid_levels;
            asks += ask_levels;
            bids_capped |= bid_capped;
            asks_capped |= ask_capped;
        }
    }

    DepthLevels {
        bids,
        asks,
        bids_capped,
        asks_capped,
    }
}

fn scan_side(book: &OrderBook, side: Side) -> (u64, bool) {
    for level in 0..DEPTH_SCAN_CAP {
        if book.depth(side, level as usize).is_none() {
            return (level, false);
        }
    }
    (DEPTH_SCAN_CAP, true)
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::bench::config::{BenchPath, LogKind};
    use crate::bench::report::SloStatus;

    fn tiny_config() -> BenchConfig {
        let mut config = BenchConfig::default();
        config.events = 500;
        config.warmup = 50;
        config.rate = 0;
        config.symbols = 1;
        config.seed = 7;
        config
    }

    #[test]
    fn rate_zero_makes_total_equal_service_and_zero_send_lag() {
        let report = run(&tiny_config()).expect("benchmark run");
        assert_eq!(report.latency.total.all, report.latency.service.all);
        assert_eq!(report.latency.send_lag.count, 500);
        assert_eq!(report.latency.send_lag.max, 0);
        assert_eq!(report.latency.send_lag.mean, 0);
    }

    #[test]
    fn same_seed_reproduces_counts() {
        let config = tiny_config();
        let first = run(&config).expect("first run");
        let second = run(&config).expect("second run");

        assert_eq!(first.run.commands, second.run.commands);
        assert_eq!(first.run.engine_events, second.run.engine_events);
        assert_eq!(first.run.trades, second.run.trades);
        assert_eq!(first.run.final_open_orders, second.run.final_open_orders);
        assert_eq!(first.run.peak_open_orders, second.run.peak_open_orders);
    }

    #[test]
    fn default_workload_produces_no_rejections() {
        let report = run(&tiny_config()).expect("benchmark run");
        assert_eq!(report.run.rejections, 0);
    }

    #[test]
    fn peak_open_orders_respects_the_cap() {
        let mut config = tiny_config();
        config.events = 2_000;
        config.warmup = 200;
        config.max_open_orders = 10;
        let report = run(&config).expect("benchmark run");
        assert!(report.run.peak_open_orders <= 10);
        assert!(report.run.final_open_orders <= 10);
    }

    #[test]
    fn observed_mix_matches_configured_ratios() {
        let mut config = tiny_config();
        config.events = 10_000;
        config.warmup = 1_000;
        config.rate = 0;

        let report = run(&config).expect("benchmark run");
        let total = report.latency.total.all.count as f64;
        let limit = report.latency.total.limit.count as f64 / total;
        let market = report.latency.total.market.count as f64 / total;
        let cancel = report.latency.total.cancel.count as f64 / total;

        assert!((limit - 0.60).abs() < 0.02, "limit share was {limit}");
        assert!((market - 0.15).abs() < 0.02, "market share was {market}");
        assert!((cancel - 0.25).abs() < 0.02, "cancel share was {cancel}");
    }

    #[test]
    fn medium_runs_mark_p999_indicative() {
        let mut config = tiny_config();
        config.events = 20_000;
        config.warmup = 0;
        config.rate = 0;

        let report = run(&config).expect("benchmark run");
        let text = report.render_text();
        assert!(text.contains("p99.9 indicative"));
        assert!(!text.contains("p99.9 unreliable"));
    }

    #[test]
    fn empty_categories_are_serialized_with_zero_counts() {
        let mut config = tiny_config();
        config.events = 200;
        config.warmup = 0;
        config.rate = 0;
        config.limit_ratio = 1.0;
        config.market_ratio = 0.0;
        config.cancel_ratio = 0.0;
        config.cross_ratio = 0.0;

        let report = run(&config).expect("benchmark run");
        let json = report.to_json();
        assert!(json.contains("\"market\": {\n        \"count\": 0"));
        assert!(json.contains("\"cancel\": {\n        \"count\": 0"));
    }

    #[test]
    fn final_depth_is_reported() {
        let report = run(&tiny_config()).expect("benchmark run");
        let levels = report.run.final_depth_levels.bids + report.run.final_depth_levels.asks;

        assert!(report.run.final_open_orders <= report.run.peak_open_orders);
        if report.run.final_open_orders > 0 {
            assert!(levels > 0, "expected resting orders after the run");
        }
    }

    #[test]
    fn invalid_ratios_are_usage_errors() {
        let mut config = tiny_config();
        config.limit_ratio = 0.9;
        config.market_ratio = 0.9;
        let error = run(&config).expect_err("invalid ratios must fail");
        assert!(matches!(error, BenchError::Usage(_)));
    }

    fn tiny_runtime_config() -> BenchConfig {
        let mut config = tiny_config();
        config.path = BenchPath::Runtime;
        config.events = 100;
        config.warmup = 10;
        config.rate = 0;
        config
    }

    #[test]
    fn runtime_path_reports_applied_and_durable_latency_families() {
        let report = run(&tiny_runtime_config()).expect("runtime benchmark run");
        let runtime = report.runtime.as_ref().expect("runtime report section");

        assert_eq!(runtime.applied_latency.count, 100);
        assert_eq!(runtime.durable_latency.count, 100);
        assert_eq!(runtime.applied_count, 100);
        assert_eq!(runtime.durable_count, 100);
        assert_eq!(runtime.queue_full, 0);

        let text = report.render_text();
        assert!(text.contains("runtime latency (ns)"));
        assert!(text.contains("applied"));
        assert!(text.contains("durable"));
        assert!(text.contains("slo"));
        assert!(text.contains("target=10,000 ns"));
        assert!(text.contains("target=500,000 ns"));

        let json = report.to_json();
        assert!(json.contains("\"runtime\""));
        assert!(json.contains("\"applied_latency\""));
        assert!(json.contains("\"durable_latency\""));
        assert!(json.contains("\"queue_wait\""));
        assert!(json.contains("\"apply_service\""));
        assert!(json.contains("\"durable_wait\""));
        assert!(json.contains("\"journal_append\""));
        assert!(json.contains("\"queue_full\""));
        assert!(json.contains("\"fsync_share\""));
        assert!(json.contains("\"slo\""));
    }

    #[test]
    fn runtime_path_supports_in_flight_throughput_mode() {
        let mut config = tiny_runtime_config();
        config.in_flight = 4;

        let report = run(&config).expect("in-flight runtime benchmark run");
        let runtime = report.runtime.as_ref().expect("runtime report section");

        assert_eq!(runtime.applied_latency.count, 100);
        assert_eq!(runtime.durable_latency.count, 100);
        assert_eq!(report.run.commands, 100);
    }

    #[test]
    fn runtime_file_log_uses_the_file_adapters() {
        let dir = tempfile::tempdir().expect("temporary data dir");
        let mut config = tiny_runtime_config();
        config.events = 20;
        config.warmup = 0;
        config.log = LogKind::File;
        config.data_dir = Some(dir.path().to_path_buf());

        let report = run(&config).expect("file-backed runtime run");
        assert_eq!(
            report
                .runtime
                .as_ref()
                .expect("runtime report section")
                .applied_latency
                .count,
            20
        );
        assert!(dir.path().join("commands.log").exists());
        assert!(dir.path().join("snapshots").exists());
        assert_ne!(
            report.runtime.as_ref().unwrap().slo.durable.status,
            SloStatus::NotApplicable,
            "a file log makes the durable SLO judgeable"
        );
    }

    #[test]
    fn report_marks_the_durable_slo_not_applicable_with_the_memory_log() {
        let report = run(&tiny_runtime_config()).expect("runtime benchmark run");
        let runtime = report.runtime.as_ref().expect("runtime report section");

        assert_eq!(
            runtime.slo.durable.status,
            SloStatus::NotApplicable,
            "an in-memory log cannot judge the durable SLO"
        );
        assert_eq!(runtime.slo.durable.queue_full, runtime.queue_full);

        let text = report.render_text();
        assert!(text.contains("NOT APPLICABLE"));
        assert!(text.contains("Linux release"));
        assert!(text.contains("--idle spin"));
        assert!(text.contains("pinned matching core"));
        assert!(text.contains("NVMe"));
        assert!(text.contains("queue_full must be 0"));

        let json = report.to_json();
        assert!(json.contains("\"status\": \"not_applicable\""));
        assert!(json.contains("\"queue_full\": 0"));
    }

    #[test]
    fn direct_path_output_has_no_runtime_section() {
        let report = run(&tiny_config()).expect("direct benchmark run");

        assert!(report.runtime.is_none());
        assert!(!report.render_text().contains("runtime latency"));
        assert!(!report.render_text().contains("runtime path"));
        assert!(!report.to_json().contains("\"runtime\""));
    }

    #[test]
    fn report_renders_text_and_json_sections() {
        let report = run(&tiny_config()).expect("benchmark run");

        let text = report.render_text();
        assert!(text.contains("config   events="));
        assert!(text.contains("workload limit="));
        assert!(text.contains("run\n"));
        assert!(text.contains("latency (ns)"));
        assert!(text.contains("per command type"));
        assert!(text.contains("p99.9 unreliable"));

        let json = report.to_json();
        assert!(json.contains("\"config\""));
        assert!(json.contains("\"run\""));
        assert!(json.contains("\"latency_ns\""));
        assert!(json.contains("\"total\""));
        assert!(json.contains("\"service\""));
        assert!(json.contains("\"send_lag\""));
        assert!(json.contains("\"peak_open_orders\""));
        assert!(json.contains("\"env\""));
    }
}
