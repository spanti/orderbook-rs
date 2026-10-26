//! Benchmark configuration, argument parsing, and validation.

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;

use crate::runtime::IdleStrategy;

pub const DEFAULT_EVENTS: u64 = 500_000;
pub const DEFAULT_RATE: u64 = 1_000_000;
pub const DEFAULT_SEED: u64 = 42;
pub const DEFAULT_SYMBOLS: usize = 1;
pub const DEFAULT_LIMIT_RATIO: f64 = 0.60;
pub const DEFAULT_MARKET_RATIO: f64 = 0.15;
pub const DEFAULT_CANCEL_RATIO: f64 = 0.25;
pub const DEFAULT_CROSS_RATIO: f64 = 0.10;
pub const DEFAULT_PRICE_BAND: u64 = 100;
pub const DEFAULT_MID: u64 = 10_000;
pub const DEFAULT_ORDER_QUANTITY: u64 = 10;
pub const DEFAULT_MAX_OPEN_ORDERS: u64 = 100_000;
pub const WARMUP_CAP: u64 = 100_000;
pub const RATIO_TOLERANCE: f64 = 1e-6;

pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;
pub const DEFAULT_JOURNAL_BATCH_CAPACITY: usize = 64;
pub const DEFAULT_APPLY_BATCH_MAX: usize = 64;
pub const DEFAULT_JOURNAL_COALESCE_MAX: usize = 1024;
pub const DEFAULT_JOURNAL_FLUSH_US: u64 = 100;
pub const DEFAULT_IN_FLIGHT: usize = 1;

/// Which execution path the benchmark drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BenchPath {
    /// Call `MatchingEngine::execute` directly on the producer thread.
    #[default]
    Direct,
    /// Submit through the matching runtime's bounded Command Queue.
    Runtime,
}

/// Storage adapters used by the runtime path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogKind {
    /// In-memory command log and snapshot store.
    #[default]
    Memory,
    /// File-backed command log and snapshot store under `--data-dir`.
    File,
}

pub const HELP: &str = "\
orderbook-rs bench

Usage: bench [OPTIONS]

Options:
  --events <N>             Measured input commands (default: 500000)
  --warmup <N>             Warmup input commands (default: min(events / 10, 100000))
  --rate <N>               Target commands/s; 0 = unlimited (default: 1000000)
  --seed <N>               Workload RNG seed (default: 42)
  --symbols <N>            Number of symbols, round-robin SYM0.. (default: 1)
  --limit-ratio <F>        Limit-order share (default: 0.60)
  --market-ratio <F>       Market-order share (default: 0.15)
  --cancel-ratio <F>       Cancel share (default: 0.25)
  --cross-ratio <F>        Share of limit orders that cross (default: 0.10)
  --price-band <TICKS>     Half-width of the price band (default: 100)
  --mid <PRICE>            Initial mid price (default: 10000)
  --order-quantity <Q>     Fixed quantity per order (default: 10)
  --max-open-orders <N>    Open-order cap; exceeding it forces cancels (default: 100000)
  --json-out <PATH>        Write a pretty JSON report to PATH
  --allow-debug            Allow running a debug build
  -h, --help               Print this help

Runtime path options:
  --path direct|runtime    Execution path (default: direct)
  --queue-capacity <N>     Ingress Command Queue capacity (default: 1024)
  --journal-batch-capacity <N>
                           Journal batches in flight before backpressure (default: 64)
  --apply-batch-max <N>    Maximum records per journal batch (default: 64)
  --journal-coalesce-max <N>
                           Maximum records per journal append/fsync (default: 1024)
  --journal-flush-us <N>   Upper bound on buffering before a flush, in us (default: 100)
  --idle spin|backoff      Matching-thread idle strategy; runtime default: spin
  --affinity <CORE>        Pin the matching thread to a core index
  --snapshot-cadence <N>   Automatic snapshots every N commands; 0 disables (default: 0)
  --log memory|file        Runtime storage adapters (default: memory)
  --data-dir <PATH>        Directory for file adapters; required with --log file
  --in-flight <N>          Commands in flight; 1 measures submit-to-ack latency (default: 1)

The benchmark must be built with --release for meaningful numbers.
";

#[derive(Debug, Clone, PartialEq)]
pub struct BenchConfig {
    pub events: u64,
    pub warmup: u64,
    pub rate: u64,
    pub seed: u64,
    pub symbols: usize,
    pub limit_ratio: f64,
    pub market_ratio: f64,
    pub cancel_ratio: f64,
    pub cross_ratio: f64,
    pub price_band: u64,
    pub mid: u64,
    pub order_quantity: u64,
    pub max_open_orders: u64,
    pub json_out: Option<PathBuf>,
    pub allow_debug: bool,
    pub path: BenchPath,
    pub queue_capacity: usize,
    pub journal_batch_capacity: usize,
    pub apply_batch_max: usize,
    pub journal_coalesce_max: usize,
    pub journal_flush_us: u64,
    pub idle: IdleStrategy,
    pub affinity: Option<usize>,
    pub snapshot_cadence: u64,
    pub log: LogKind,
    pub data_dir: Option<PathBuf>,
    pub in_flight: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            events: DEFAULT_EVENTS,
            warmup: default_warmup(DEFAULT_EVENTS),
            rate: DEFAULT_RATE,
            seed: DEFAULT_SEED,
            symbols: DEFAULT_SYMBOLS,
            limit_ratio: DEFAULT_LIMIT_RATIO,
            market_ratio: DEFAULT_MARKET_RATIO,
            cancel_ratio: DEFAULT_CANCEL_RATIO,
            cross_ratio: DEFAULT_CROSS_RATIO,
            price_band: DEFAULT_PRICE_BAND,
            mid: DEFAULT_MID,
            order_quantity: DEFAULT_ORDER_QUANTITY,
            max_open_orders: DEFAULT_MAX_OPEN_ORDERS,
            json_out: None,
            allow_debug: false,
            path: BenchPath::Direct,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            journal_batch_capacity: DEFAULT_JOURNAL_BATCH_CAPACITY,
            apply_batch_max: DEFAULT_APPLY_BATCH_MAX,
            journal_coalesce_max: DEFAULT_JOURNAL_COALESCE_MAX,
            journal_flush_us: DEFAULT_JOURNAL_FLUSH_US,
            // The bench measures the low-latency target, so spin is the
            // runtime default even though RuntimeConfig defaults to Backoff.
            idle: IdleStrategy::Spin,
            affinity: None,
            snapshot_cadence: 0,
            log: LogKind::Memory,
            data_dir: None,
            in_flight: DEFAULT_IN_FLIGHT,
        }
    }
}

pub fn default_warmup(events: u64) -> u64 {
    (events / 10).min(WARMUP_CAP)
}

impl BenchConfig {
    pub fn validate(&self) -> Result<(), BenchError> {
        if self.events == 0 {
            return Err(BenchError::usage("--events must be greater than 0"));
        }
        if self.symbols == 0 {
            return Err(BenchError::usage("--symbols must be greater than 0"));
        }
        if self.price_band == 0 {
            return Err(BenchError::usage("--price-band must be greater than 0"));
        }
        if self.mid == 0 {
            return Err(BenchError::usage("--mid must be greater than 0"));
        }
        if self.order_quantity == 0 {
            return Err(BenchError::usage("--order-quantity must be greater than 0"));
        }
        if self.max_open_orders == 0 {
            return Err(BenchError::usage(
                "--max-open-orders must be greater than 0",
            ));
        }

        for (name, ratio) in [
            ("--limit-ratio", self.limit_ratio),
            ("--market-ratio", self.market_ratio),
            ("--cancel-ratio", self.cancel_ratio),
            ("--cross-ratio", self.cross_ratio),
        ] {
            if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
                return Err(BenchError::usage(format!(
                    "{name} must be a finite number between 0 and 1"
                )));
            }
        }

        let sum = self.limit_ratio + self.market_ratio + self.cancel_ratio;
        if (sum - 1.0).abs() > RATIO_TOLERANCE {
            return Err(BenchError::usage(format!(
                "--limit-ratio + --market-ratio + --cancel-ratio must equal 1 (got {sum})"
            )));
        }

        for (name, value) in [
            ("--queue-capacity", self.queue_capacity),
            ("--journal-batch-capacity", self.journal_batch_capacity),
            ("--apply-batch-max", self.apply_batch_max),
            ("--journal-coalesce-max", self.journal_coalesce_max),
            ("--in-flight", self.in_flight),
        ] {
            if value == 0 {
                return Err(BenchError::usage(format!("{name} must be greater than 0")));
            }
        }
        if self.path == BenchPath::Runtime && self.log == LogKind::File && self.data_dir.is_none() {
            return Err(BenchError::usage("--log file requires --data-dir <PATH>"));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CliAction {
    Run(BenchConfig),
    Help,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BenchError {
    Usage(String),
    DebugBuild,
    Io(String),
}

impl BenchError {
    pub fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) | Self::DebugBuild => 2,
            Self::Io(_) => 1,
        }
    }
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => write!(f, "{message}"),
            Self::DebugBuild => write!(
                f,
                "benchmark must be built with --release; pass --allow-debug to run anyway"
            ),
            Self::Io(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for BenchError {}

/// Parses benchmark arguments (without the program name).
pub fn parse_args<I>(args: I) -> Result<CliAction, BenchError>
where
    I: IntoIterator<Item = String>,
{
    let args: Vec<String> = args.into_iter().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(CliAction::Help);
    }

    let mut seen = HashSet::new();
    let mut events = None;
    let mut warmup = None;
    let mut rate = None;
    let mut seed = None;
    let mut symbols = None;
    let mut limit_ratio = None;
    let mut market_ratio = None;
    let mut cancel_ratio = None;
    let mut cross_ratio = None;
    let mut price_band = None;
    let mut mid = None;
    let mut order_quantity = None;
    let mut max_open_orders = None;
    let mut json_out = None;
    let mut allow_debug = false;
    let mut path = None;
    let mut queue_capacity = None;
    let mut journal_batch_capacity = None;
    let mut apply_batch_max = None;
    let mut journal_coalesce_max = None;
    let mut journal_flush_us = None;
    let mut idle = None;
    let mut affinity = None;
    let mut snapshot_cadence = None;
    let mut log = None;
    let mut data_dir = None;
    let mut in_flight = None;

    let mut index = 0;
    while index < args.len() {
        let raw = &args[index];
        index += 1;

        let (flag, inline_value) = match raw.split_once('=') {
            Some((flag, value)) => (flag, Some(value.to_string())),
            None => (raw.as_str(), None),
        };

        if !seen.insert(flag.to_string()) {
            return Err(BenchError::usage(format!("duplicate flag: {flag}")));
        }

        if flag == "--allow-debug" {
            if inline_value.is_some() {
                return Err(BenchError::usage("--allow-debug does not take a value"));
            }
            allow_debug = true;
            continue;
        }

        if !takes_value(flag) {
            return Err(BenchError::usage(format!("unknown flag: {flag}")));
        }

        let value = match inline_value {
            Some(value) if !value.is_empty() => value,
            Some(_) => return Err(BenchError::usage(format!("empty value for {flag}"))),
            None => {
                let value = args
                    .get(index)
                    .cloned()
                    .ok_or_else(|| BenchError::usage(format!("missing value for {flag}")))?;
                index += 1;
                value
            }
        };

        match flag {
            "--events" => events = Some(parse_u64(flag, &value)?),
            "--warmup" => warmup = Some(parse_u64(flag, &value)?),
            "--rate" => rate = Some(parse_u64(flag, &value)?),
            "--seed" => seed = Some(parse_u64(flag, &value)?),
            "--symbols" => symbols = Some(parse_usize(flag, &value)?),
            "--limit-ratio" => limit_ratio = Some(parse_f64(flag, &value)?),
            "--market-ratio" => market_ratio = Some(parse_f64(flag, &value)?),
            "--cancel-ratio" => cancel_ratio = Some(parse_f64(flag, &value)?),
            "--cross-ratio" => cross_ratio = Some(parse_f64(flag, &value)?),
            "--price-band" => price_band = Some(parse_u64(flag, &value)?),
            "--mid" => mid = Some(parse_u64(flag, &value)?),
            "--order-quantity" => order_quantity = Some(parse_u64(flag, &value)?),
            "--max-open-orders" => max_open_orders = Some(parse_u64(flag, &value)?),
            "--json-out" => json_out = Some(PathBuf::from(value)),
            "--path" => path = Some(parse_path(&value)?),
            "--queue-capacity" => queue_capacity = Some(parse_usize(flag, &value)?),
            "--journal-batch-capacity" => journal_batch_capacity = Some(parse_usize(flag, &value)?),
            "--apply-batch-max" => apply_batch_max = Some(parse_usize(flag, &value)?),
            "--journal-coalesce-max" => journal_coalesce_max = Some(parse_usize(flag, &value)?),
            "--journal-flush-us" => journal_flush_us = Some(parse_u64(flag, &value)?),
            "--idle" => idle = Some(parse_idle(&value)?),
            "--affinity" => affinity = Some(parse_usize(flag, &value)?),
            "--snapshot-cadence" => snapshot_cadence = Some(parse_u64(flag, &value)?),
            "--log" => log = Some(parse_log(&value)?),
            "--data-dir" => data_dir = Some(PathBuf::from(value)),
            "--in-flight" => in_flight = Some(parse_usize(flag, &value)?),
            _ => unreachable!("takes_value accepted an unknown flag"),
        }
    }

    let events = events.unwrap_or(DEFAULT_EVENTS);
    let config = BenchConfig {
        events,
        warmup: warmup.unwrap_or_else(|| default_warmup(events)),
        rate: rate.unwrap_or(DEFAULT_RATE),
        seed: seed.unwrap_or(DEFAULT_SEED),
        symbols: symbols.unwrap_or(DEFAULT_SYMBOLS),
        limit_ratio: limit_ratio.unwrap_or(DEFAULT_LIMIT_RATIO),
        market_ratio: market_ratio.unwrap_or(DEFAULT_MARKET_RATIO),
        cancel_ratio: cancel_ratio.unwrap_or(DEFAULT_CANCEL_RATIO),
        cross_ratio: cross_ratio.unwrap_or(DEFAULT_CROSS_RATIO),
        price_band: price_band.unwrap_or(DEFAULT_PRICE_BAND),
        mid: mid.unwrap_or(DEFAULT_MID),
        order_quantity: order_quantity.unwrap_or(DEFAULT_ORDER_QUANTITY),
        max_open_orders: max_open_orders.unwrap_or(DEFAULT_MAX_OPEN_ORDERS),
        json_out,
        allow_debug,
        path: path.unwrap_or_default(),
        queue_capacity: queue_capacity.unwrap_or(DEFAULT_QUEUE_CAPACITY),
        journal_batch_capacity: journal_batch_capacity.unwrap_or(DEFAULT_JOURNAL_BATCH_CAPACITY),
        apply_batch_max: apply_batch_max.unwrap_or(DEFAULT_APPLY_BATCH_MAX),
        journal_coalesce_max: journal_coalesce_max.unwrap_or(DEFAULT_JOURNAL_COALESCE_MAX),
        journal_flush_us: journal_flush_us.unwrap_or(DEFAULT_JOURNAL_FLUSH_US),
        idle: idle.unwrap_or(IdleStrategy::Spin),
        affinity,
        snapshot_cadence: snapshot_cadence.unwrap_or(0),
        log: log.unwrap_or_default(),
        data_dir,
        in_flight: in_flight.unwrap_or(DEFAULT_IN_FLIGHT),
    };

    config.validate()?;
    Ok(CliAction::Run(config))
}

fn takes_value(flag: &str) -> bool {
    matches!(
        flag,
        "--events"
            | "--warmup"
            | "--rate"
            | "--seed"
            | "--symbols"
            | "--limit-ratio"
            | "--market-ratio"
            | "--cancel-ratio"
            | "--cross-ratio"
            | "--price-band"
            | "--mid"
            | "--order-quantity"
            | "--max-open-orders"
            | "--json-out"
            | "--path"
            | "--queue-capacity"
            | "--journal-batch-capacity"
            | "--apply-batch-max"
            | "--journal-coalesce-max"
            | "--journal-flush-us"
            | "--idle"
            | "--affinity"
            | "--snapshot-cadence"
            | "--log"
            | "--data-dir"
            | "--in-flight"
    )
}

fn parse_path(value: &str) -> Result<BenchPath, BenchError> {
    match value {
        "direct" => Ok(BenchPath::Direct),
        "runtime" => Ok(BenchPath::Runtime),
        _ => Err(BenchError::usage(format!(
            "invalid value for --path: {value} (expected direct or runtime)"
        ))),
    }
}

fn parse_idle(value: &str) -> Result<IdleStrategy, BenchError> {
    match value {
        "spin" => Ok(IdleStrategy::Spin),
        "backoff" => Ok(IdleStrategy::Backoff),
        _ => Err(BenchError::usage(format!(
            "invalid value for --idle: {value} (expected spin or backoff)"
        ))),
    }
}

fn parse_log(value: &str) -> Result<LogKind, BenchError> {
    match value {
        "memory" => Ok(LogKind::Memory),
        "file" => Ok(LogKind::File),
        _ => Err(BenchError::usage(format!(
            "invalid value for --log: {value} (expected memory or file)"
        ))),
    }
}

fn parse_u64(flag: &str, value: &str) -> Result<u64, BenchError> {
    value
        .parse::<u64>()
        .map_err(|_| BenchError::usage(format!("invalid value for {flag}: {value}")))
}

fn parse_usize(flag: &str, value: &str) -> Result<usize, BenchError> {
    let parsed = parse_u64(flag, value)?;
    usize::try_from(parsed)
        .map_err(|_| BenchError::usage(format!("value for {flag} is too large: {value}")))
}

fn parse_f64(flag: &str, value: &str) -> Result<f64, BenchError> {
    value
        .parse::<f64>()
        .map_err(|_| BenchError::usage(format!("invalid value for {flag}: {value}")))
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn warmup_default_is_ten_percent_capped() {
        assert_eq!(default_warmup(1), 0);
        assert_eq!(default_warmup(1_000), 100);
        assert_eq!(default_warmup(500_000), 50_000);
        assert_eq!(default_warmup(2_000_000), 100_000);
    }

    #[test]
    fn ratio_sum_uses_a_one_e_minus_six_tolerance() {
        let mut config = BenchConfig::default();
        config.limit_ratio = 0.6;
        config.market_ratio = 0.15;
        config.cancel_ratio = 0.249_999_5;
        assert!(config.validate().is_ok());

        config.cancel_ratio = 0.249_998;
        assert!(config.validate().is_err());
    }

    #[test]
    fn out_of_range_ratios_are_rejected() {
        let mut config = BenchConfig::default();
        config.limit_ratio = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn parse_supports_both_value_forms() {
        let action = parse_args([
            "--events=1000".to_string(),
            "--rate".to_string(),
            "0".to_string(),
        ])
        .expect("valid arguments");

        let CliAction::Run(config) = action else {
            panic!("expected a run action");
        };
        assert_eq!(config.events, 1000);
        assert_eq!(config.warmup, 100);
        assert_eq!(config.rate, 0);
    }

    #[test]
    fn parse_rejects_unknown_duplicate_and_missing_values() {
        assert!(parse_args(["--not-a-flag".to_string()]).is_err());
        assert!(
            parse_args([
                "--events".to_string(),
                "1".to_string(),
                "--events".to_string(),
                "2".to_string()
            ])
            .is_err()
        );
        assert!(parse_args(["--events".to_string()]).is_err());
    }

    #[test]
    fn help_short_circuits_parsing() {
        assert_eq!(
            parse_args(["--help".to_string()]).expect("help"),
            CliAction::Help
        );
        assert_eq!(
            parse_args(["-h".to_string()]).expect("help"),
            CliAction::Help
        );
    }

    #[test]
    fn path_defaults_to_direct_and_runtime_flags_have_bench_defaults() {
        let config = BenchConfig::default();
        assert_eq!(config.path, BenchPath::Direct);
        assert_eq!(config.queue_capacity, 1024);
        assert_eq!(config.journal_batch_capacity, 64);
        assert_eq!(config.apply_batch_max, 64);
        assert_eq!(config.journal_coalesce_max, 1024);
        assert_eq!(config.journal_flush_us, 100);
        assert_eq!(config.idle, crate::runtime::IdleStrategy::Spin);
        assert_eq!(config.affinity, None);
        assert_eq!(config.snapshot_cadence, 0);
        assert_eq!(config.log, LogKind::Memory);
        assert_eq!(config.data_dir, None);
        assert_eq!(config.in_flight, 1);
    }

    #[test]
    fn parses_the_runtime_flag_surface() {
        let action = parse_args([
            "--path".to_string(),
            "runtime".to_string(),
            "--queue-capacity".to_string(),
            "2048".to_string(),
            "--journal-batch-capacity".to_string(),
            "32".to_string(),
            "--apply-batch-max".to_string(),
            "16".to_string(),
            "--journal-coalesce-max".to_string(),
            "512".to_string(),
            "--journal-flush-us".to_string(),
            "50".to_string(),
            "--idle".to_string(),
            "backoff".to_string(),
            "--affinity".to_string(),
            "3".to_string(),
            "--snapshot-cadence".to_string(),
            "1000".to_string(),
            "--log".to_string(),
            "file".to_string(),
            "--data-dir".to_string(),
            "tmp/bench".to_string(),
            "--in-flight".to_string(),
            "8".to_string(),
        ])
        .expect("valid runtime arguments");

        let CliAction::Run(config) = action else {
            panic!("expected a run action");
        };
        assert_eq!(config.path, BenchPath::Runtime);
        assert_eq!(config.queue_capacity, 2048);
        assert_eq!(config.journal_batch_capacity, 32);
        assert_eq!(config.apply_batch_max, 16);
        assert_eq!(config.journal_coalesce_max, 512);
        assert_eq!(config.journal_flush_us, 50);
        assert_eq!(config.idle, crate::runtime::IdleStrategy::Backoff);
        assert_eq!(config.affinity, Some(3));
        assert_eq!(config.snapshot_cadence, 1000);
        assert_eq!(config.log, LogKind::File);
        assert_eq!(config.data_dir, Some(PathBuf::from("tmp/bench")));
        assert_eq!(config.in_flight, 8);
    }

    #[test]
    fn rejects_invalid_runtime_flag_values() {
        for args in [
            vec!["--path", "hybrid"],
            vec!["--idle", "busy"],
            vec!["--log", "disk"],
            vec!["--path", "runtime", "--in-flight", "0"],
            vec!["--path", "direct", "--in-flight", "0"],
            vec!["--path", "runtime", "--queue-capacity", "0"],
            vec!["--path", "runtime", "--journal-batch-capacity", "0"],
            vec!["--path", "runtime", "--apply-batch-max", "0"],
            vec!["--path", "runtime", "--journal-coalesce-max", "0"],
            vec!["--path", "runtime", "--log", "file"],
        ] {
            let parsed: Result<CliAction, BenchError> =
                parse_args(args.iter().map(|arg| arg.to_string()));
            assert!(parsed.is_err(), "expected a usage error for {args:?}");
        }
    }

    #[test]
    fn runtime_numeric_flags_must_be_positive() {
        let mut config = BenchConfig::default();
        config.path = BenchPath::Runtime;
        config.in_flight = 0;
        assert!(matches!(config.validate(), Err(BenchError::Usage(_))));

        config.in_flight = 1;
        config.queue_capacity = 0;
        assert!(matches!(config.validate(), Err(BenchError::Usage(_))));

        config.queue_capacity = 1024;
        config.journal_batch_capacity = 0;
        assert!(matches!(config.validate(), Err(BenchError::Usage(_))));

        config.journal_batch_capacity = 64;
        config.apply_batch_max = 0;
        assert!(matches!(config.validate(), Err(BenchError::Usage(_))));

        config.apply_batch_max = 64;
        config.journal_coalesce_max = 0;
        assert!(matches!(config.validate(), Err(BenchError::Usage(_))));
    }

    #[test]
    fn file_log_requires_a_data_dir() {
        let mut config = BenchConfig::default();
        config.path = BenchPath::Runtime;
        config.log = LogKind::File;
        assert!(matches!(config.validate(), Err(BenchError::Usage(_))));

        config.data_dir = Some(PathBuf::from("tmp/bench"));
        assert!(config.validate().is_ok());
    }
}
