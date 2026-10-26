//! Structured benchmark results and their text/JSON renderings.

use std::fmt::Write;

use crate::runtime::IdleStrategy;

use super::config::{BenchConfig, LogKind};
use super::stats::Summary;

/// Acceptance target for `submit -> applied` p99, in nanoseconds.
pub const APPLIED_SLO_NS: u64 = 10_000;
/// Acceptance target for `submit -> durable` p99, in nanoseconds.
pub const DURABLE_SLO_NS: u64 = 500_000;

#[derive(Debug, Clone)]
pub struct BenchReport {
    pub config: BenchConfig,
    pub run: RunStats,
    pub latency: LatencyReport,
    /// Present only for the runtime path. Direct output is unchanged.
    pub runtime: Option<RuntimeReport>,
    pub env: EnvInfo,
}

/// In-process runtime measurements beyond the direct-path latency tables.
#[derive(Debug, Clone)]
pub struct RuntimeReport {
    /// Submit -> applied (`wait_applied`) per command.
    pub applied_latency: Summary,
    /// Submit -> durable (`wait_durable`) per command.
    pub durable_latency: Summary,
    pub queue_wait: LatencyMetric,
    pub apply_service: LatencyMetric,
    pub durable_wait: LatencyMetric,
    pub journal_append: LatencyMetric,
    /// Mean records per journal handoff batch.
    pub batch_size_mean: f64,
    pub batch_count: u64,
    pub queue_depth_max: u64,
    pub queue_full: u64,
    /// Journal append time / durable wait time, as a share of durable latency.
    pub fsync_share: f64,
    pub applied_count: u64,
    pub durable_count: u64,
    pub slo: SloReport,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LatencyMetric {
    pub mean_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct SloReport {
    pub applied: SloCheck,
    pub durable: SloCheck,
}

/// Whether an SLO can be judged in this run's setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SloStatus {
    /// p99 met the target and the run had no queue-full refusals.
    Pass,
    /// p99 missed the target, or the run observed at least one queue-full
    /// refusal.
    Fail,
    /// The measurement cannot judge this SLO in the current setup (for
    /// example, the durable SLO with an in-memory log).
    NotApplicable,
}

#[derive(Debug, Clone, Copy)]
pub struct SloCheck {
    pub target_ns: u64,
    pub p50: u64,
    pub p99: u64,
    pub p99_9: u64,
    /// Queue-full refusals over the same measured window.
    pub queue_full: u64,
    pub status: SloStatus,
}

impl SloCheck {
    /// Judge one SLO. `queue_full` must be zero for a PASS: a refusal means
    /// the producer could not sustain the offered rate, so the measured
    /// latency does not describe the acceptance workload.
    pub fn new(summary: Summary, target_ns: u64, queue_full: u64) -> Self {
        let status = if summary.p99 <= target_ns && queue_full == 0 {
            SloStatus::Pass
        } else {
            SloStatus::Fail
        };
        Self {
            target_ns,
            p50: summary.p50,
            p99: summary.p99,
            p99_9: summary.p99_9,
            queue_full,
            status,
        }
    }

    /// Mark the SLO as unjudgeable by this setup while still reporting the
    /// observed percentiles.
    pub fn not_applicable(summary: Summary, target_ns: u64, queue_full: u64) -> Self {
        Self {
            target_ns,
            p50: summary.p50,
            p99: summary.p99,
            p99_9: summary.p99_9,
            queue_full,
            status: SloStatus::NotApplicable,
        }
    }

    fn label(&self) -> &'static str {
        match self.status {
            SloStatus::Pass => "PASS",
            SloStatus::Fail => "FAIL",
            SloStatus::NotApplicable => "NOT APPLICABLE",
        }
    }

    fn status_json(&self) -> &'static str {
        match self.status {
            SloStatus::Pass => "pass",
            SloStatus::Fail => "fail",
            SloStatus::NotApplicable => "not_applicable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunStats {
    pub wall_time_ns: u64,
    pub achieved_rate: f64,
    pub commands: u64,
    pub engine_events: u64,
    pub trades: u64,
    pub rejections: u64,
    pub final_open_orders: u64,
    pub peak_open_orders: u64,
    pub final_depth_levels: DepthLevels,
}

#[derive(Debug, Clone, Copy)]
pub struct DepthLevels {
    pub bids: u64,
    pub asks: u64,
    pub bids_capped: bool,
    pub asks_capped: bool,
}

#[derive(Debug, Clone)]
pub struct LatencyReport {
    pub total: CategoryStats,
    pub service: CategoryStats,
    pub send_lag: Summary,
}

#[derive(Debug, Clone)]
pub struct CategoryStats {
    pub all: Summary,
    pub limit: Summary,
    pub market: Summary,
    pub cancel: Summary,
}

#[derive(Debug, Clone, Copy)]
pub struct EnvInfo {
    pub os: &'static str,
    pub arch: &'static str,
    pub profile: &'static str,
}

impl EnvInfo {
    pub fn current() -> Self {
        Self {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        }
    }
}

impl BenchReport {
    pub fn render_text(&self) -> String {
        let mut out = String::new();

        writeln!(out, "orderbook-rs bench").expect("writing to a String cannot fail");
        writeln!(
            out,
            "config   events={} warmup={} rate={} seed={} symbols={}",
            commas(self.config.events),
            commas(self.config.warmup),
            rate_label(self.config.rate),
            self.config.seed,
            self.config.symbols
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "workload limit={} market={} cancel={} cross={} band={} mid={} qty={}",
            pct(self.config.limit_ratio),
            pct(self.config.market_ratio),
            pct(self.config.cancel_ratio),
            pct(self.config.cross_ratio),
            self.config.price_band,
            self.config.mid,
            self.config.order_quantity
        )
        .expect("writing to a String cannot fail");
        if self.runtime.is_some() {
            writeln!(
                out,
                "runtime  queue={} journal_batches={} apply_batch={} coalesce={} flush_us={} idle={} affinity={} snapshots={} log={} in_flight={}",
                self.config.queue_capacity,
                self.config.journal_batch_capacity,
                self.config.apply_batch_max,
                self.config.journal_coalesce_max,
                self.config.journal_flush_us,
                idle_label(self.config.idle),
                affinity_label(self.config.affinity),
                self.config.snapshot_cadence,
                log_label(self.config.log),
                self.config.in_flight
            )
            .expect("writing to a String cannot fail");
        }
        writeln!(out).expect("writing to a String cannot fail");

        writeln!(out, "run").expect("writing to a String cannot fail");
        writeln!(
            out,
            "  wall time       {:.2} s",
            self.run.wall_time_ns as f64 / 1e9
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "  achieved rate   {} commands/s",
            commas(self.run.achieved_rate as u64)
        )
        .expect("writing to a String cannot fail");
        writeln!(out, "  commands        {}", commas(self.run.commands))
            .expect("writing to a String cannot fail");
        writeln!(
            out,
            "  engine events   {} ({}/s)",
            commas(self.run.engine_events),
            commas(per_second(self.run.engine_events, self.run.wall_time_ns))
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "  trades          {} ({}/s)",
            commas(self.run.trades),
            commas(per_second(self.run.trades, self.run.wall_time_ns))
        )
        .expect("writing to a String cannot fail");
        writeln!(out, "  rejections      {}", commas(self.run.rejections))
            .expect("writing to a String cannot fail");
        writeln!(
            out,
            "  final depth     open={} bids={} asks={} (peak open={})",
            commas(self.run.final_open_orders),
            depth_count(
                self.run.final_depth_levels.bids,
                self.run.final_depth_levels.bids_capped
            ),
            depth_count(
                self.run.final_depth_levels.asks,
                self.run.final_depth_levels.asks_capped
            ),
            commas(self.run.peak_open_orders)
        )
        .expect("writing to a String cannot fail");
        writeln!(out).expect("writing to a String cannot fail");

        writeln!(
            out,
            "latency (ns)      p50      p99     p99.9      mean       max"
        )
        .expect("writing to a String cannot fail");
        write_summary_row(&mut out, "total", self.latency.total.all);
        write_summary_row(&mut out, "service", self.latency.service.all);
        write_summary_row(&mut out, "send lag", self.latency.send_lag);

        if let Some(runtime) = &self.runtime {
            writeln!(out).expect("writing to a String cannot fail");
            writeln!(
                out,
                "runtime latency (ns)      p50      p99     p99.9      mean       max"
            )
            .expect("writing to a String cannot fail");
            write_summary_row(&mut out, "applied", runtime.applied_latency);
            write_summary_row(&mut out, "durable", runtime.durable_latency);
            writeln!(
                out,
                "note: applied and durable latency are measured from submit; the service row above mirrors applied"
            )
            .expect("writing to a String cannot fail");
        }

        writeln!(out).expect("writing to a String cannot fail");

        writeln!(out, "per command type").expect("writing to a String cannot fail");
        write_type_row(
            &mut out,
            "limit",
            self.latency.total.limit,
            self.run.wall_time_ns,
        );
        write_type_row(
            &mut out,
            "market",
            self.latency.total.market,
            self.run.wall_time_ns,
        );
        write_type_row(
            &mut out,
            "cancel",
            self.latency.total.cancel,
            self.run.wall_time_ns,
        );

        if let Some(runtime) = &self.runtime {
            writeln!(out).expect("writing to a String cannot fail");
            writeln!(out, "runtime metrics").expect("writing to a String cannot fail");
            write_metric_row(&mut out, "queue wait", runtime.queue_wait);
            write_metric_row(&mut out, "apply service", runtime.apply_service);
            write_metric_row(&mut out, "durable wait", runtime.durable_wait);
            write_metric_row(&mut out, "journal append", runtime.journal_append);
            writeln!(
                out,
                "  batch size       mean={:.2} records  batches={}",
                runtime.batch_size_mean,
                commas(runtime.batch_count)
            )
            .expect("writing to a String cannot fail");
            writeln!(
                out,
                "  queue depth max  {}",
                commas(runtime.queue_depth_max)
            )
            .expect("writing to a String cannot fail");
            writeln!(out, "  queue full       {}", commas(runtime.queue_full))
                .expect("writing to a String cannot fail");
            writeln!(
                out,
                "  fsync share      {:.1}% of durable wait",
                runtime.fsync_share * 100.0
            )
            .expect("writing to a String cannot fail");

            writeln!(out).expect("writing to a String cannot fail");
            writeln!(out, "slo").expect("writing to a String cannot fail");
            write_slo_row(&mut out, "submit -> applied", runtime.slo.applied);
            write_slo_row(&mut out, "submit -> durable", runtime.slo.durable);
            writeln!(
                out,
                "note: SLO setup requires a Linux release build, --idle spin, a pinned matching core, and FileCommandLog on NVMe for the durable level; queue_full must be 0 for a PASS"
            )
            .expect("writing to a String cannot fail");
            if self.config.log == LogKind::Memory {
                writeln!(
                    out,
                    "note: the durable SLO is NOT APPLICABLE with --log memory; rerun with --log file on NVMe to judge it"
                )
                .expect("writing to a String cannot fail");
            }
        }

        writeln!(
            out,
            "note: total latency is measured from intended send time"
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "note: engine audit logging is included; history grows with command count"
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "note: timings use the OS monotonic clock; Windows QPC typically ticks at ~100 ns, so do not interpret sub-tick differences"
        )
        .expect("writing to a String cannot fail");

        if self.config.rate == 0 {
            writeln!(out, "note: rate=unlimited; total = service, send lag = 0")
                .expect("writing to a String cannot fail");
        } else if self.run.achieved_rate < self.config.rate as f64 * 0.99 {
            writeln!(
                out,
                "warning: achieved rate {:.0} is below target {} commands/s",
                self.run.achieved_rate,
                commas(self.config.rate)
            )
            .expect("writing to a String cannot fail");
        }

        let samples = self.latency.total.all.count;
        if samples < 10_000 {
            writeln!(out, "warning: p99.9 unreliable (fewer than 10,000 samples)")
                .expect("writing to a String cannot fail");
        } else if samples < 100_000 {
            writeln!(out, "note: p99.9 indicative (fewer than 100,000 samples)")
                .expect("writing to a String cannot fail");
        }

        if self.run.final_depth_levels.bids_capped || self.run.final_depth_levels.asks_capped {
            writeln!(
                out,
                "note: depth level counts marked >= are lower bounds (scan cap reached)"
            )
            .expect("writing to a String cannot fail");
        }

        if self.run.rejections > 0 {
            writeln!(
                out,
                "warning: {} commands were rejected",
                commas(self.run.rejections)
            )
            .expect("writing to a String cannot fail");
        }

        out
    }

    pub fn to_json(&self) -> String {
        let mut out = String::new();

        writeln!(out, "{{").expect("writing to a String cannot fail");
        writeln!(out, "  \"config\": {{").expect("writing to a String cannot fail");
        writeln!(out, "    \"events\": {},", self.config.events)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"warmup\": {},", self.config.warmup)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"rate\": {},", self.config.rate)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"seed\": {},", self.config.seed)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"symbols\": {},", self.config.symbols)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"limit_ratio\": {},", self.config.limit_ratio)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"market_ratio\": {},", self.config.market_ratio)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"cancel_ratio\": {},", self.config.cancel_ratio)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"cross_ratio\": {},", self.config.cross_ratio)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"price_band\": {},", self.config.price_band)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"mid\": {},", self.config.mid)
            .expect("writing to a String cannot fail");
        writeln!(
            out,
            "    \"order_quantity\": {},",
            self.config.order_quantity
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "    \"max_open_orders\": {}",
            self.config.max_open_orders
        )
        .expect("writing to a String cannot fail");
        writeln!(out, "  }},").expect("writing to a String cannot fail");

        writeln!(out, "  \"run\": {{").expect("writing to a String cannot fail");
        writeln!(out, "    \"wall_time_ns\": {},", self.run.wall_time_ns)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"achieved_rate\": {:.3},", self.run.achieved_rate)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"commands\": {},", self.run.commands)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"engine_events\": {},", self.run.engine_events)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"trades\": {},", self.run.trades)
            .expect("writing to a String cannot fail");
        writeln!(out, "    \"rejections\": {},", self.run.rejections)
            .expect("writing to a String cannot fail");
        writeln!(
            out,
            "    \"final_open_orders\": {},",
            self.run.final_open_orders
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "    \"peak_open_orders\": {},",
            self.run.peak_open_orders
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "    \"final_depth_levels\": {{ \"bids\": {}, \"asks\": {} }}",
            self.run.final_depth_levels.bids, self.run.final_depth_levels.asks
        )
        .expect("writing to a String cannot fail");
        writeln!(out, "  }},").expect("writing to a String cannot fail");

        writeln!(out, "  \"latency_ns\": {{").expect("writing to a String cannot fail");
        writeln!(out, "    \"total\": {{").expect("writing to a String cannot fail");
        write_json_category(&mut out, "all", self.latency.total.all, true);
        write_json_category(&mut out, "limit", self.latency.total.limit, true);
        write_json_category(&mut out, "market", self.latency.total.market, true);
        write_json_category(&mut out, "cancel", self.latency.total.cancel, false);
        writeln!(out, "    }},").expect("writing to a String cannot fail");
        writeln!(out, "    \"service\": {{").expect("writing to a String cannot fail");
        write_json_category(&mut out, "all", self.latency.service.all, true);
        write_json_category(&mut out, "limit", self.latency.service.limit, true);
        write_json_category(&mut out, "market", self.latency.service.market, true);
        write_json_category(&mut out, "cancel", self.latency.service.cancel, false);
        writeln!(out, "    }},").expect("writing to a String cannot fail");
        writeln!(out, "    \"send_lag\": {{").expect("writing to a String cannot fail");
        write_json_summary_fields(&mut out, self.latency.send_lag, "      ", false);
        writeln!(out, "    }}").expect("writing to a String cannot fail");
        writeln!(out, "  }},").expect("writing to a String cannot fail");

        if let Some(runtime) = &self.runtime {
            writeln!(out, "  \"runtime\": {{").expect("writing to a String cannot fail");
            writeln!(out, "    \"config\": {{").expect("writing to a String cannot fail");
            writeln!(
                out,
                "      \"queue_capacity\": {}, \"journal_batch_capacity\": {}, \"apply_batch_max\": {},",
                self.config.queue_capacity,
                self.config.journal_batch_capacity,
                self.config.apply_batch_max
            )
            .expect("writing to a String cannot fail");
            writeln!(
                out,
                "      \"journal_coalesce_max\": {}, \"journal_flush_us\": {}, \"idle\": \"{}\",",
                self.config.journal_coalesce_max,
                self.config.journal_flush_us,
                idle_label(self.config.idle)
            )
            .expect("writing to a String cannot fail");
            match self.config.affinity {
                Some(core) => writeln!(out, "      \"affinity\": {core},")
                    .expect("writing to a String cannot fail"),
                None => writeln!(out, "      \"affinity\": null,")
                    .expect("writing to a String cannot fail"),
            }
            writeln!(
                out,
                "      \"snapshot_cadence\": {}, \"log\": \"{}\",",
                self.config.snapshot_cadence,
                log_label(self.config.log)
            )
            .expect("writing to a String cannot fail");
            match &self.config.data_dir {
                Some(dir) => writeln!(
                    out,
                    "      \"data_dir\": \"{}\", \"in_flight\": {}",
                    json_escape(&dir.to_string_lossy()),
                    self.config.in_flight
                )
                .expect("writing to a String cannot fail"),
                None => writeln!(
                    out,
                    "      \"data_dir\": null, \"in_flight\": {}",
                    self.config.in_flight
                )
                .expect("writing to a String cannot fail"),
            }
            writeln!(out, "    }},").expect("writing to a String cannot fail");

            writeln!(out, "    \"applied_latency\": {{").expect("writing to a String cannot fail");
            write_json_summary_fields(&mut out, runtime.applied_latency, "      ", false);
            writeln!(out, "    }},").expect("writing to a String cannot fail");
            writeln!(out, "    \"durable_latency\": {{").expect("writing to a String cannot fail");
            write_json_summary_fields(&mut out, runtime.durable_latency, "      ", false);
            writeln!(out, "    }},").expect("writing to a String cannot fail");

            write_json_metric(&mut out, "queue_wait", runtime.queue_wait, true);
            write_json_metric(&mut out, "apply_service", runtime.apply_service, true);
            write_json_metric(&mut out, "durable_wait", runtime.durable_wait, true);
            write_json_metric(&mut out, "journal_append", runtime.journal_append, true);
            writeln!(
                out,
                "    \"batch_size_mean\": {:.3},",
                runtime.batch_size_mean
            )
            .expect("writing to a String cannot fail");
            writeln!(out, "    \"batch_count\": {},", runtime.batch_count)
                .expect("writing to a String cannot fail");
            writeln!(out, "    \"queue_depth_max\": {},", runtime.queue_depth_max)
                .expect("writing to a String cannot fail");
            writeln!(out, "    \"queue_full\": {},", runtime.queue_full)
                .expect("writing to a String cannot fail");
            writeln!(out, "    \"fsync_share\": {:.6},", runtime.fsync_share)
                .expect("writing to a String cannot fail");
            writeln!(out, "    \"applied_count\": {},", runtime.applied_count)
                .expect("writing to a String cannot fail");
            writeln!(out, "    \"durable_count\": {},", runtime.durable_count)
                .expect("writing to a String cannot fail");

            writeln!(out, "    \"slo\": {{").expect("writing to a String cannot fail");
            writeln!(out, "      \"applied\": {{").expect("writing to a String cannot fail");
            write_json_slo_fields(&mut out, runtime.slo.applied, "        ");
            writeln!(out, "      }},").expect("writing to a String cannot fail");
            writeln!(out, "      \"durable\": {{").expect("writing to a String cannot fail");
            write_json_slo_fields(&mut out, runtime.slo.durable, "        ");
            writeln!(out, "      }}").expect("writing to a String cannot fail");
            writeln!(out, "    }}").expect("writing to a String cannot fail");
            writeln!(out, "  }},").expect("writing to a String cannot fail");
        }

        writeln!(
            out,
            "  \"env\": {{ \"os\": \"{}\", \"arch\": \"{}\", \"profile\": \"{}\" }}",
            json_escape(self.env.os),
            json_escape(self.env.arch),
            json_escape(self.env.profile)
        )
        .expect("writing to a String cannot fail");
        writeln!(out, "}}").expect("writing to a String cannot fail");

        out
    }
}

fn write_summary_row(out: &mut String, label: &str, summary: Summary) {
    writeln!(
        out,
        "  {:<10} {:>10} {:>8} {:>9} {:>9} {:>9}",
        label,
        commas(summary.p50),
        commas(summary.p99),
        commas(summary.p99_9),
        commas(summary.mean),
        commas(summary.max)
    )
    .expect("writing to a String cannot fail");
}

fn write_metric_row(out: &mut String, label: &str, metric: LatencyMetric) {
    writeln!(
        out,
        "  {:<16} mean={} ns  max={} ns",
        label,
        commas(metric.mean_ns),
        commas(metric.max_ns)
    )
    .expect("writing to a String cannot fail");
}

fn write_slo_row(out: &mut String, label: &str, check: SloCheck) {
    writeln!(
        out,
        "  {:<16} p50={} p99={} p99.9={} target={} ns  queue_full={}  {}",
        label,
        commas(check.p50),
        commas(check.p99),
        commas(check.p99_9),
        commas(check.target_ns),
        commas(check.queue_full),
        check.label()
    )
    .expect("writing to a String cannot fail");
}

fn idle_label(idle: IdleStrategy) -> &'static str {
    match idle {
        IdleStrategy::Spin => "spin",
        IdleStrategy::Backoff => "backoff",
    }
}

fn log_label(log: LogKind) -> &'static str {
    match log {
        LogKind::Memory => "memory",
        LogKind::File => "file",
    }
}

fn affinity_label(affinity: Option<usize>) -> String {
    affinity.map_or_else(|| "none".to_string(), |core| core.to_string())
}

fn write_type_row(out: &mut String, label: &str, summary: Summary, wall_time_ns: u64) {
    writeln!(
        out,
        "  {:<8} count={}  p50={}  p99={}  p99.9={}  throughput={}/s",
        label,
        commas(summary.count),
        commas(summary.p50),
        commas(summary.p99),
        commas(summary.p99_9),
        commas(per_second(summary.count, wall_time_ns))
    )
    .expect("writing to a String cannot fail");
}

fn write_json_category(out: &mut String, name: &str, summary: Summary, trailing_comma: bool) {
    writeln!(out, "      \"{name}\": {{").expect("writing to a String cannot fail");
    write_json_summary_fields(out, summary, "        ", false);
    if trailing_comma {
        writeln!(out, "      }},").expect("writing to a String cannot fail");
    } else {
        writeln!(out, "      }}").expect("writing to a String cannot fail");
    }
}

fn write_json_summary_fields(
    out: &mut String,
    summary: Summary,
    indent: &str,
    trailing_comma: bool,
) {
    writeln!(out, "{indent}\"count\": {},", summary.count)
        .expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"p50\": {},", summary.p50).expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"p99\": {},", summary.p99).expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"p99_9\": {},", summary.p99_9)
        .expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"mean\": {},", summary.mean).expect("writing to a String cannot fail");
    if trailing_comma {
        writeln!(out, "{indent}\"max\": {},", summary.max)
            .expect("writing to a String cannot fail");
    } else {
        writeln!(out, "{indent}\"max\": {}", summary.max).expect("writing to a String cannot fail");
    }
}

fn write_json_metric(out: &mut String, name: &str, metric: LatencyMetric, trailing_comma: bool) {
    writeln!(out, "    \"{name}\": {{").expect("writing to a String cannot fail");
    writeln!(out, "      \"mean_ns\": {},", metric.mean_ns)
        .expect("writing to a String cannot fail");
    if trailing_comma {
        writeln!(out, "      \"max_ns\": {}\n    }},", metric.max_ns)
            .expect("writing to a String cannot fail");
    } else {
        writeln!(out, "      \"max_ns\": {}\n    }}", metric.max_ns)
            .expect("writing to a String cannot fail");
    }
}

fn write_json_slo_fields(out: &mut String, check: SloCheck, indent: &str) {
    writeln!(out, "{indent}\"target_ns\": {},", check.target_ns)
        .expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"queue_full\": {},", check.queue_full)
        .expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"p50\": {},", check.p50).expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"p99\": {},", check.p99).expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"p99_9\": {},", check.p99_9).expect("writing to a String cannot fail");
    writeln!(out, "{indent}\"status\": \"{}\"", check.status_json())
        .expect("writing to a String cannot fail");
}

fn commas(value: u64) -> String {
    let text = value.to_string();
    let mut out = String::with_capacity(text.len() + text.len() / 3);
    for (index, byte) in text.bytes().enumerate() {
        if index > 0 && (text.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(byte as char);
    }
    out
}

fn per_second(count: u64, wall_time_ns: u64) -> u64 {
    if wall_time_ns == 0 {
        0
    } else {
        (count as f64 / (wall_time_ns as f64 / 1e9)).round() as u64
    }
}

fn rate_label(rate: u64) -> String {
    if rate == 0 {
        "unlimited".to_string()
    } else {
        format!("{}/s", commas(rate))
    }
}

fn pct(ratio: f64) -> String {
    format!("{:.0}%", ratio * 100.0)
}

fn depth_count(value: u64, capped: bool) -> String {
    if capped {
        format!(">={}", commas(value))
    } else {
        commas(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::stats::Summary;

    fn fast() -> Summary {
        Summary {
            count: 100,
            p50: 1,
            p99: 2,
            p99_9: 3,
            mean: 1,
            max: 3,
        }
    }

    #[test]
    fn slo_pass_requires_zero_queue_full() {
        assert_eq!(SloCheck::new(fast(), 10, 0).status, SloStatus::Pass);
        assert_eq!(
            SloCheck::new(fast(), 10, 1).status,
            SloStatus::Fail,
            "a queue-full refusal invalidates a latency PASS"
        );
        assert_eq!(
            SloCheck::new(fast(), 1, 0).status,
            SloStatus::Fail,
            "p99 above the target fails"
        );
        assert_eq!(
            SloCheck::not_applicable(fast(), 10, 0).status,
            SloStatus::NotApplicable
        );
    }

    #[test]
    fn slo_labels_render_pass_fail_and_not_applicable() {
        assert_eq!(SloCheck::new(fast(), 10, 0).label(), "PASS");
        assert_eq!(SloCheck::new(fast(), 10, 1).label(), "FAIL");
        assert_eq!(
            SloCheck::not_applicable(fast(), 10, 0).label(),
            "NOT APPLICABLE"
        );
        assert_eq!(SloCheck::new(fast(), 10, 0).status_json(), "pass");
        assert_eq!(SloCheck::new(fast(), 10, 1).status_json(), "fail");
        assert_eq!(
            SloCheck::not_applicable(fast(), 10, 0).status_json(),
            "not_applicable"
        );
    }
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            character if character.is_control() => {
                write!(out, "\\u{:04x}", character as u32)
                    .expect("writing to a String cannot fail");
            }
            character => out.push(character),
        }
    }
    out
}
