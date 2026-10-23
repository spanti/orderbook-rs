use std::path::PathBuf;
use std::process::{Command, Output};

fn bench(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_bench"))
        .args(args)
        .output()
        .expect("failed to spawn bench binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn small_args() -> Vec<&'static str> {
    vec![
        "--allow-debug",
        "--events",
        "100",
        "--warmup",
        "10",
        "--rate",
        "0",
    ]
}

fn temp_path(name: &str) -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "orderbook-bench-{name}-{}-{unique}.json",
        std::process::id()
    ))
}

#[test]
fn help_exits_zero_and_lists_options() {
    let output = bench(&["--help"]);

    assert!(output.status.success());
    let text = stdout(&output);
    assert!(text.contains("--events"));
    assert!(text.contains("--warmup"));
    assert!(text.contains("--allow-debug"));
}

#[test]
fn unknown_flag_exits_two() {
    let output = bench(&["--not-a-flag"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("unknown flag"));
}

#[test]
fn missing_value_exits_two() {
    let output = bench(&["--allow-debug", "--events"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("missing value"));
}

#[test]
fn duplicate_flag_exits_two() {
    let output = bench(&["--allow-debug", "--events", "10", "--events", "20"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("duplicate flag"));
}

#[test]
fn invalid_ratio_sum_exits_two() {
    let output = bench(&[
        "--allow-debug",
        "--limit-ratio",
        "0.9",
        "--market-ratio",
        "0.9",
        "--cancel-ratio",
        "0.25",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("must equal 1"));
}

#[test]
fn debug_build_refuses_without_allow_debug() {
    if !cfg!(debug_assertions) {
        return;
    }

    let output = bench(&["--events", "10", "--warmup", "0", "--rate", "0"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("--allow-debug"));
}

#[test]
fn runs_and_prints_a_report() {
    let output = bench(&small_args());

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("latency (ns)"));
    assert!(text.contains("per command type"));
    assert!(text.contains("note: rate=unlimited"));
    assert!(text.contains("p99.9 unreliable"));
}

#[test]
fn equals_form_is_accepted() {
    let output = bench(&["--allow-debug", "--events=100", "--warmup=0", "--rate=0"]);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stdout(&output).contains("commands        100"));
}

#[test]
fn warmup_defaults_to_ten_percent() {
    let output = bench(&["--allow-debug", "--events", "1000", "--rate", "0"]);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stdout(&output).contains("warmup=100 "));
}

#[test]
fn json_out_writes_a_pretty_report() {
    let path = temp_path("ok");
    let path_text = path.to_string_lossy().into_owned();
    let args = vec![
        "--allow-debug",
        "--events",
        "100",
        "--warmup",
        "10",
        "--rate",
        "0",
        "--json-out",
        path_text.as_str(),
    ];

    let output = bench(&args);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let json = std::fs::read_to_string(&path).expect("json report exists");
    assert!(json.contains("\n  \"config\": {"));
    assert!(json.contains("\"latency_ns\""));
    assert!(json.contains("\"peak_open_orders\""));
    assert!(json.contains("\"env\""));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn json_out_missing_parent_exits_one() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let missing = std::env::temp_dir().join(format!(
        "orderbook-bench-missing-{}-{unique}",
        std::process::id()
    ));
    let path = missing.join("report.json");
    let path_text = path.to_string_lossy().into_owned();
    let args = vec![
        "--allow-debug",
        "--events",
        "100",
        "--warmup",
        "10",
        "--rate",
        "0",
        "--json-out",
        path_text.as_str(),
    ];

    let output = bench(&args);

    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("parent directory does not exist"));
}
