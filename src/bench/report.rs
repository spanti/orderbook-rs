//! Structured benchmark results and their text/JSON renderings.

use std::fmt::Write;

use serde::Serialize;

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
    /// Present only for the pure `decide` path.
    pub decide: Option<DecideReport>,
    pub env: EnvInfo,
}

/// Measurements for the pure `decide` path.
///
/// This is the pure decision baseline gated against the applied SLO: command
/// in, events out, running on borrowed `BookView` / `StopView` overlays with no
/// state mutation. If it cannot meet the applied target, that is a measured
/// shortfall, not a reason to move the runtime onto a different path.
#[derive(Debug, Clone, Serialize)]
pub struct DecideReport {
    pub latency: Summary,
    pub events_total: u64,
    #[serde(serialize_with = "serialize_round3")]
    pub events_per_command: f64,
    #[serde(serialize_with = "serialize_decide_slo")]
    pub slo: SloCheck,
}

/// In-process runtime measurements beyond the direct-path latency tables.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeReport {
    /// Submit -> applied (`wait_applied`) per command.
    pub applied_latency: Summary,
    /// Submit -> durable (`wait_durable`) per command.
    pub durable_latency: Summary,
    pub queue_wait: LatencyMetric,
    pub apply_service: LatencyMetric,
    pub durable_wait: LatencyMetric,
    pub journal_append: LatencyMetric,
    /// Mean event records per journal handoff batch.
    #[serde(serialize_with = "serialize_round3")]
    pub batch_size_mean: f64,
    /// Largest event-record count in one journal handoff batch.
    pub batch_size_max: u64,
    pub batch_count: u64,
    /// Event records applied in the measured window.
    pub events_total: u64,
    /// Mean event records per applied command.
    #[serde(serialize_with = "serialize_round3")]
    pub events_per_command: f64,
    pub queue_depth_max: u64,
    pub queue_full: u64,
    /// Journal append time / durable wait time, as a share of durable latency.
    #[serde(serialize_with = "serialize_round6")]
    pub fsync_share: f64,
    pub applied_count: u64,
    pub durable_count: u64,
    pub slo: SloReport,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct LatencyMetric {
    pub mean_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SloReport {
    pub applied: SloCheck,
    pub durable: SloCheck,
}

/// Whether an SLO can be judged in this run's setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Copy, Serialize)]
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
}

#[derive(Debug, Clone, Serialize)]
pub struct RunStats {
    pub wall_time_ns: u64,
    /// Denominator used for [`Self::achieved_rate`]. The runtime path uses the
    /// submission window (first to last submit), excluding the final durable
    /// drain; direct and decide use the measured loop.
    pub rate_window_ns: u64,
    #[serde(serialize_with = "serialize_round3")]
    pub achieved_rate: f64,
    pub commands: u64,
    pub engine_events: u64,
    /// Mean engine events per command. Event volume grows with fills and
    /// rejections, so this is reported next to the raw count.
    #[serde(serialize_with = "serialize_round3")]
    pub events_per_command: f64,
    pub trades: u64,
    pub rejections: u64,
    pub final_open_orders: u64,
    pub peak_open_orders: u64,
    pub final_depth_levels: DepthLevels,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct DepthLevels {
    pub bids: u64,
    pub asks: u64,
    #[serde(skip)]
    pub bids_capped: bool,
    #[serde(skip)]
    pub asks_capped: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyReport {
    pub total: CategoryStats,
    pub service: CategoryStats,
    pub send_lag: Summary,
}

#[derive(Debug, Clone, Serialize)]
pub struct CategoryStats {
    pub all: Summary,
    pub limit: Summary,
    pub market: Summary,
    pub cancel: Summary,
}

#[derive(Debug, Clone, Copy, Serialize)]
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
            "  engine events   {} ({}/s, {:.2}/command)",
            commas(self.run.engine_events),
            commas(per_second(self.run.engine_events, self.run.rate_window_ns)),
            self.run.events_per_command
        )
        .expect("writing to a String cannot fail");
        writeln!(
            out,
            "  trades          {} ({}/s)",
            commas(self.run.trades),
            commas(per_second(self.run.trades, self.run.rate_window_ns))
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
                "  event volume     events={}  events/command={:.2}",
                commas(runtime.events_total),
                runtime.events_per_command
            )
            .expect("writing to a String cannot fail");
            writeln!(
                out,
                "  batch size       mean={:.2} max={} records  batches={}",
                runtime.batch_size_mean,
                commas(runtime.batch_size_max),
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
                "note: SLO setup requires a Linux release build, --idle spin, a pinned matching core, and FileEventLog on NVMe for the durable level; queue_full must be 0 for a PASS"
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

        if let Some(decide) = &self.decide {
            writeln!(out).expect("writing to a String cannot fail");
            writeln!(out, "decide metrics").expect("writing to a String cannot fail");
            writeln!(
                out,
                "  {:<16} mean={} ns  max={} ns",
                "decide service",
                commas(decide.latency.mean),
                commas(decide.latency.max)
            )
            .expect("writing to a String cannot fail");
            writeln!(
                out,
                "  event volume     events={}  events/command={:.2}",
                commas(decide.events_total),
                decide.events_per_command
            )
            .expect("writing to a String cannot fail");

            writeln!(out).expect("writing to a String cannot fail");
            writeln!(out, "slo").expect("writing to a String cannot fail");
            write_slo_row(&mut out, "submit -> decide", decide.slo);
            writeln!(
                out,
                "note: the pure decide path is gated against the applied SLO; measure a Linux release build for a numeric judgement"
            )
            .expect("writing to a String cannot fail");
            if decide.slo.status == SloStatus::NotApplicable {
                writeln!(
                    out,
                    "note: the decide SLO is NOT APPLICABLE on this platform; Windows QPC tick resolution makes sub-tick numbers functional-only"
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
        } else {
            let achieved = self.run.achieved_rate as u64;
            if achieved < self.config.rate {
                writeln!(
                    out,
                    "warning: achieved rate {} is below target {} commands/s",
                    commas(achieved),
                    commas(self.config.rate)
                )
                .expect("writing to a String cannot fail");
            }
        }

        if self.runtime.is_some() {
            writeln!(
                out,
                "note: achieved rate uses the submission window (first to last submit); the final durable drain is excluded"
            )
            .expect("writing to a String cannot fail");
        }

        if self.runtime.is_some() && self.config.log == LogKind::File && self.config.rate > 0 {
            let in_flight = self.config.in_flight.max(1) as f64;
            let mean_floor_ns = in_flight / self.config.rate as f64 * 1e9;
            if mean_floor_ns > DURABLE_SLO_NS as f64 {
                let max_in_flight =
                    (self.config.rate as f64 * DURABLE_SLO_NS as f64 / 1e9).floor() as u64;
                writeln!(
                    out,
                    "note: in-flight {} at {} commands/s implies a mean latency floor of {:.0} us, above the {} us durable target; judge latency in a separate run with in-flight <= {}",
                    self.config.in_flight.max(1),
                    commas(self.config.rate),
                    mean_floor_ns / 1000.0,
                    DURABLE_SLO_NS / 1000,
                    commas(max_in_flight)
                )
                .expect("writing to a String cannot fail");
            }
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
        let view = BenchReportJson {
            config: ConfigJson::from(&self.config),
            run: &self.run,
            latency_ns: &self.latency,
            runtime: self.runtime.as_ref().map(|runtime| RuntimeJson {
                config: RuntimeConfigJson::from(&self.config),
                report: runtime,
            }),
            decide: self.decide.as_ref(),
            env: &self.env,
        };

        serde_json::to_string_pretty(&view).expect("serializing the bench report cannot fail")
            + "\n"
    }
}

/// Borrowed envelope for the JSON report.
///
/// The DTOs declare their own field lists once; this view only adds the JSON
/// names that differ from the Rust names and the config subsets the JSON
/// contract exposes.
#[derive(Serialize)]
struct BenchReportJson<'a> {
    config: ConfigJson,
    run: &'a RunStats,
    latency_ns: &'a LatencyReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<RuntimeJson<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decide: Option<&'a DecideReport>,
    env: &'a EnvInfo,
}

#[derive(Serialize)]
struct RuntimeJson<'a> {
    config: RuntimeConfigJson,
    #[serde(flatten)]
    report: &'a RuntimeReport,
}

/// The workload subset of [`BenchConfig`] exposed by the JSON contract.
#[derive(Serialize)]
struct ConfigJson {
    events: u64,
    warmup: u64,
    rate: u64,
    seed: u64,
    symbols: usize,
    limit_ratio: f64,
    market_ratio: f64,
    cancel_ratio: f64,
    cross_ratio: f64,
    price_band: u64,
    mid: u64,
    order_quantity: u64,
    max_open_orders: u64,
}

impl From<&BenchConfig> for ConfigJson {
    fn from(config: &BenchConfig) -> Self {
        Self {
            events: config.events,
            warmup: config.warmup,
            rate: config.rate,
            seed: config.seed,
            symbols: config.symbols,
            limit_ratio: config.limit_ratio,
            market_ratio: config.market_ratio,
            cancel_ratio: config.cancel_ratio,
            cross_ratio: config.cross_ratio,
            price_band: config.price_band,
            mid: config.mid,
            order_quantity: config.order_quantity,
            max_open_orders: config.max_open_orders,
        }
    }
}

/// The runtime-tuning subset of [`BenchConfig`] nested under `runtime.config`.
#[derive(Serialize)]
struct RuntimeConfigJson {
    queue_capacity: usize,
    journal_batch_capacity: usize,
    apply_batch_max: usize,
    journal_coalesce_max: usize,
    journal_flush_us: u64,
    idle: &'static str,
    affinity: Option<usize>,
    snapshot_cadence: u64,
    log: &'static str,
    data_dir: Option<String>,
    in_flight: usize,
}

impl From<&BenchConfig> for RuntimeConfigJson {
    fn from(config: &BenchConfig) -> Self {
        Self {
            queue_capacity: config.queue_capacity,
            journal_batch_capacity: config.journal_batch_capacity,
            apply_batch_max: config.apply_batch_max,
            journal_coalesce_max: config.journal_coalesce_max,
            journal_flush_us: config.journal_flush_us,
            idle: idle_label(config.idle),
            affinity: config.affinity,
            snapshot_cadence: config.snapshot_cadence,
            log: log_label(config.log),
            data_dir: config
                .data_dir
                .as_ref()
                .map(|dir| dir.to_string_lossy().into_owned()),
            in_flight: config.in_flight,
        }
    }
}

fn serialize_round3<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_f64((*value * 1_000.0).round() / 1_000.0)
}

fn serialize_round6<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_f64((*value * 1_000_000.0).round() / 1_000_000.0)
}

/// Nest the decide SLO under its `submit_to_decide` key.
fn serialize_decide_slo<S>(check: &SloCheck, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    #[derive(Serialize)]
    struct DecideSlo<'a> {
        submit_to_decide: &'a SloCheck,
    }

    serde::Serialize::serialize(
        &DecideSlo {
            submit_to_decide: check,
        },
        serializer,
    )
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
    }

    fn object_keys(value: &serde_json::Value) -> std::collections::BTreeSet<String> {
        value
            .as_object()
            .expect("expected a JSON object")
            .keys()
            .cloned()
            .collect()
    }

    fn expected_keys(names: &[&str]) -> std::collections::BTreeSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn base_report() -> BenchReport {
        let categories = || CategoryStats {
            all: fast(),
            limit: fast(),
            market: fast(),
            cancel: fast(),
        };
        BenchReport {
            config: BenchConfig::default(),
            run: RunStats {
                wall_time_ns: 1_000,
                rate_window_ns: 1_000,
                achieved_rate: 1234.5678,
                commands: 10,
                engine_events: 20,
                events_per_command: 2.0,
                trades: 0,
                rejections: 0,
                final_open_orders: 1,
                peak_open_orders: 2,
                final_depth_levels: DepthLevels {
                    bids: 1,
                    asks: 0,
                    bids_capped: true,
                    asks_capped: true,
                },
            },
            latency: LatencyReport {
                total: categories(),
                service: categories(),
                send_lag: fast(),
            },
            runtime: None,
            decide: None,
            env: EnvInfo {
                os: "test-os",
                arch: "test-arch",
                profile: "debug",
            },
        }
    }

    fn runtime_report() -> RuntimeReport {
        RuntimeReport {
            applied_latency: fast(),
            durable_latency: fast(),
            queue_wait: LatencyMetric {
                mean_ns: 1,
                max_ns: 2,
            },
            apply_service: LatencyMetric {
                mean_ns: 1,
                max_ns: 2,
            },
            durable_wait: LatencyMetric {
                mean_ns: 1,
                max_ns: 2,
            },
            journal_append: LatencyMetric {
                mean_ns: 1,
                max_ns: 2,
            },
            batch_size_mean: 1.2345,
            batch_size_max: 4,
            batch_count: 5,
            events_total: 20,
            events_per_command: 2.0,
            queue_depth_max: 3,
            queue_full: 0,
            fsync_share: 0.123_456_789,
            applied_count: 10,
            durable_count: 10,
            slo: SloReport {
                applied: SloCheck::new(fast(), APPLIED_SLO_NS, 0),
                durable: SloCheck::not_applicable(fast(), DURABLE_SLO_NS, 0),
            },
        }
    }

    fn decide_report() -> DecideReport {
        DecideReport {
            latency: fast(),
            events_total: 20,
            events_per_command: 2.0,
            slo: SloCheck::new(fast(), APPLIED_SLO_NS, 0),
        }
    }

    #[test]
    fn json_has_the_exact_top_level_contract_and_no_optional_sections() {
        let json: serde_json::Value =
            serde_json::from_str(&base_report().to_json()).expect("valid JSON");
        assert_eq!(
            object_keys(&json),
            expected_keys(&["config", "run", "latency_ns", "env"])
        );
        assert!(json.get("latency_ns").is_some());
        assert!(json.get("latency").is_none());
        assert!(json.get("runtime").is_none());
        assert!(json.get("decide").is_none());
    }

    #[test]
    fn json_config_is_the_workload_subset_only() {
        let json: serde_json::Value =
            serde_json::from_str(&base_report().to_json()).expect("valid JSON");
        assert_eq!(
            object_keys(&json["config"]),
            expected_keys(&[
                "events",
                "warmup",
                "rate",
                "seed",
                "symbols",
                "limit_ratio",
                "market_ratio",
                "cancel_ratio",
                "cross_ratio",
                "price_band",
                "mid",
                "order_quantity",
                "max_open_orders",
            ])
        );
    }

    #[test]
    fn json_runtime_config_has_the_exact_key_set_and_nesting() {
        let mut report = base_report();
        report.runtime = Some(runtime_report());
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).expect("valid JSON");

        assert!(json.get("decide").is_none());
        assert_eq!(
            object_keys(&json["runtime"]["config"]),
            expected_keys(&[
                "queue_capacity",
                "journal_batch_capacity",
                "apply_batch_max",
                "journal_coalesce_max",
                "journal_flush_us",
                "idle",
                "affinity",
                "snapshot_cadence",
                "log",
                "data_dir",
                "in_flight",
            ])
        );
        assert_eq!(json["runtime"]["config"]["idle"], "spin");
        assert_eq!(json["runtime"]["config"]["log"], "memory");
        assert!(json["runtime"]["config"]["affinity"].is_null());
        assert!(json["runtime"]["config"]["data_dir"].is_null());

        // RuntimeReport fields are flattened next to the nested config.
        assert_eq!(json["runtime"]["applied_latency"]["count"], 100);
        assert_eq!(json["runtime"]["durable_latency"]["count"], 100);
        assert_eq!(json["runtime"]["queue_full"], 0);
        assert_eq!(json["runtime"]["slo"]["applied"]["status"], "pass");
        assert_eq!(
            json["runtime"]["slo"]["durable"]["status"],
            "not_applicable"
        );
    }

    #[test]
    fn json_decide_section_is_nested_under_slo_submit_to_decide() {
        let mut report = base_report();
        report.decide = Some(decide_report());
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).expect("valid JSON");

        assert!(json.get("runtime").is_none());
        assert_eq!(json["decide"]["latency"]["count"], 100);
        assert_eq!(json["decide"]["events_total"], 20);
        assert_eq!(json["decide"]["slo"]["submit_to_decide"]["status"], "pass");
        assert_eq!(
            json["decide"]["slo"]["submit_to_decide"]["target_ns"],
            APPLIED_SLO_NS
        );
    }

    #[test]
    fn json_depth_levels_omit_the_scan_cap_flags() {
        let json: serde_json::Value =
            serde_json::from_str(&base_report().to_json()).expect("valid JSON");
        assert_eq!(
            object_keys(&json["run"]["final_depth_levels"]),
            expected_keys(&["bids", "asks"])
        );
        assert_eq!(json["run"]["final_depth_levels"]["bids"], 1);
        assert_eq!(json["run"]["final_depth_levels"]["asks"], 0);
    }

    #[test]
    fn json_rounds_the_contract_floats_and_keeps_them_numbers() {
        let mut report = base_report();
        report.runtime = Some(runtime_report());
        report.decide = Some(decide_report());
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).expect("valid JSON");

        assert_eq!(json["run"]["achieved_rate"], 1234.568);
        assert_eq!(json["run"]["events_per_command"], 2.0);
        assert_eq!(json["runtime"]["batch_size_mean"], 1.235);
        assert_eq!(json["runtime"]["fsync_share"], 0.123_457);
        assert_eq!(json["decide"]["events_per_command"], 2.0);
        assert!(json["runtime"]["fsync_share"].is_number());
        assert!(json["runtime"]["batch_size_mean"].is_number());
    }

    #[test]
    fn achieved_rate_warning_is_strict() {
        let mut report = base_report();
        report.config.rate = 100_000;
        report.run.achieved_rate = 99_999.0;

        let text = report.render_text();
        assert!(
            text.contains("warning: achieved rate 99,999 is below target 100,000 commands/s"),
            "a rate one command below target must warn:\n{text}"
        );

        report.run.achieved_rate = 100_000.0;
        let text = report.render_text();
        assert!(
            !text.contains("warning: achieved rate"),
            "the target itself must pass:\n{text}"
        );
    }

    #[test]
    fn runtime_report_notes_the_submission_window_and_littles_law() {
        let mut report = base_report();
        report.config.log = LogKind::File;
        report.config.rate = 100_000;
        report.config.in_flight = 128;
        report.runtime = Some(runtime_report());

        let text = report.render_text();
        assert!(
            text.contains("achieved rate uses the submission window"),
            "the rate denominator must be visible:\n{text}"
        );
        assert!(
            text.contains(
                "in-flight 128 at 100,000 commands/s implies a mean latency floor of 1280 us"
            ),
            "an unsatisfiable in-flight/rate combination must warn:\n{text}"
        );
        assert!(
            text.contains("in-flight <= 50"),
            "the warning must name the latency-run bound:\n{text}"
        );
    }
}
