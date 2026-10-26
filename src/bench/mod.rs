//! Benchmark harness for the matching engine.

pub mod config;
pub mod report;
pub mod rng;
pub mod run;
pub mod runtime_path;
pub mod stats;
pub mod workload;

pub use config::{BenchConfig, BenchError, BenchPath, CliAction, HELP, LogKind, parse_args};
pub use report::BenchReport;
pub use run::run;
