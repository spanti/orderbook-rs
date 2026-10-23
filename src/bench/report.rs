//! Structured benchmark results and their text/JSON renderings.

use std::fmt::Write;

use super::config::BenchConfig;
use super::stats::Summary;

#[derive(Debug, Clone)]
pub struct BenchReport {
    pub config: BenchConfig,
    pub run: RunStats,
    pub latency: LatencyReport,
    pub env: EnvInfo,
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
            "note: timings use the OS monotonic clock; Windows QPC typically ticks at ~100 ns"
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
