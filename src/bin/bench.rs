use std::path::Path;

use orderbook_rs::bench::{BenchError, CliAction, HELP, parse_args, run};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match parse_args(args) {
        Ok(CliAction::Help) => {
            print!("{HELP}");
        }
        Ok(CliAction::Run(config)) => {
            if cfg!(debug_assertions) && !config.allow_debug {
                let error = BenchError::DebugBuild;
                eprintln!("error: {error}");
                std::process::exit(error.exit_code());
            }

            match run(&config) {
                Ok(report) => {
                    if let Some(path) = &config.json_out
                        && let Err(error) = write_json(path, &report.to_json())
                    {
                        eprintln!("error: {error}");
                        std::process::exit(error.exit_code());
                    }
                    print!("{}", report.render_text());
                }
                Err(error) => {
                    eprintln!("error: {error}");
                    std::process::exit(error.exit_code());
                }
            }
        }
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(error.exit_code());
        }
    }
}

fn write_json(path: &Path, json: &str) -> Result<(), BenchError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        return Err(BenchError::Io(format!(
            "parent directory does not exist: {}",
            parent.display()
        )));
    }

    std::fs::write(path, json)
        .map_err(|error| BenchError::Io(format!("failed to write {}: {error}", path.display())))
}
