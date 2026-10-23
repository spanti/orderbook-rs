//! Benchmark configuration, argument parsing, and validation.

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;

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
    )
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
