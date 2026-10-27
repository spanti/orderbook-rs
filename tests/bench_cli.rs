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
    assert!(text.contains("--path direct|runtime"));
    assert!(text.contains("--in-flight"));
    assert!(text.contains("--journal-coalesce-max"));
}

#[test]
fn runtime_path_runs_a_tiny_workload_and_prints_both_latency_families() {
    let output = bench(&[
        "--allow-debug",
        "--path",
        "runtime",
        "--events",
        "100",
        "--warmup",
        "10",
        "--rate",
        "0",
        "--in-flight",
        "1",
    ]);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("runtime latency (ns)"));
    assert!(text.contains("applied"));
    assert!(text.contains("durable"));
    assert!(text.contains("runtime metrics"));
    assert!(text.contains("queue wait"));
    assert!(text.contains("journal append"));
    assert!(text.contains("queue full"));
    assert!(text.contains("fsync share"));
    assert!(text.contains("slo"));
    assert!(text.contains("target=10,000 ns"));
    assert!(text.contains("target=500,000 ns"));
    assert!(text.contains("PASS") || text.contains("FAIL"));
}

#[test]
fn decide_path_reports_event_volume_and_the_applied_slo_gate() {
    let output = bench(&[
        "--allow-debug",
        "--path",
        "decide",
        "--events",
        "200",
        "--warmup",
        "10",
        "--rate",
        "0",
    ]);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("decide metrics"));
    assert!(text.contains("event volume"));
    assert!(text.contains("events/command"));
    assert!(text.contains("submit -> decide"));
    assert!(text.contains("target=10,000 ns"));
    // The decide path is gated against the applied SLO. On Windows the
    // tick-limited clock makes the status functional-only.
    assert!(text.contains("PASS") || text.contains("FAIL") || text.contains("NOT APPLICABLE"));
    if cfg!(target_os = "windows") {
        assert!(text.contains("NOT APPLICABLE"));
        assert!(text.contains("QPC"));
    }
}

#[test]
fn runtime_path_supports_in_flight_and_file_log() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "orderbook-bench-data-{}-{unique}",
        std::process::id()
    ));

    let output = bench(&[
        "--allow-debug",
        "--path",
        "runtime",
        "--log",
        "file",
        "--data-dir",
        dir.to_str().expect("temporary path is utf-8"),
        "--in-flight",
        "4",
        "--events",
        "50",
        "--warmup",
        "0",
        "--rate",
        "0",
    ]);

    let stderr_text = stderr(&output);
    let text = stdout(&output);
    assert!(output.status.success(), "stderr: {stderr_text}");
    assert!(text.contains("log=file"));
    assert!(text.contains("in_flight=4"));
    assert!(dir.join("events.log").exists());
    assert!(dir.join("snapshots").exists());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn invalid_runtime_flags_are_usage_errors() {
    for args in [
        vec!["--path", "hybrid"],
        vec!["--idle", "busy"],
        vec!["--log", "disk"],
        vec!["--path", "runtime", "--in-flight", "0"],
        vec!["--path", "runtime", "--queue-capacity", "0"],
        vec!["--path", "runtime", "--log", "file"],
    ] {
        let output = bench(&args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "expected a usage exit for {args:?}; stderr: {}",
            stderr(&output)
        );
        assert!(
            stderr(&output).contains("invalid value")
                || stderr(&output).contains("must be greater than 0")
                || stderr(&output).contains("requires --data-dir"),
            "unexpected stderr for {args:?}: {}",
            stderr(&output)
        );
    }
}

#[test]
fn direct_output_stays_runtime_free() {
    let output = bench(&small_args());

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let text = stdout(&output);
    assert!(!text.contains("runtime latency"));
    assert!(!text.contains("runtime metrics"));
    assert!(!text.contains("fsync share"));
    assert!(!text.contains("slo"));
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
    assert!(json.contains("\"events\": 100"));
    assert!(json.contains("\"rate\": 0"));
    assert!(json.contains("\"latency_ns\""));
    assert!(json.contains("\"total\": {"));
    assert!(json.contains("\"service\": {"));
    assert!(json.contains("\"send_lag\": {"));
    assert!(json.contains("\"final_depth_levels\""));
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
