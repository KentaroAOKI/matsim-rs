use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use matsim_core::{explain_person_score, run_single_iteration, write_outputs};
use matsim_io::load_scenario;
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "matsim-rs")]
#[command(about = "Rust entrypoint for incremental MATSim core reimplementation")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Compare {
        #[arg(long)]
        left: PathBuf,
        #[arg(long)]
        right: PathBuf,
    },
    Explain {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        person_id: String,
    },
}

#[derive(Debug, Error)]
enum CliError {
    #[error(transparent)]
    Io(#[from] matsim_io::IoError),
    #[error(transparent)]
    Core(#[from] matsim_core::CoreError),
    #[error("failed to read {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("person `{0}` not found")]
    PersonNotFound(String),
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run { config } => run_command(&config),
        Command::Compare { left, right } => compare_command(&left, &right),
        Command::Explain { config, person_id } => explain_command(&config, &person_id),
    }
}

fn run_command(config_path: &Path) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let output = run_single_iteration(&scenario);
    let output_dir = resolve_output_dir(config_path, &scenario.config.output_directory);
    write_outputs(&output_dir, &output)?;

    println!("random_seed={}", scenario.config.random_seed);
    println!("persons={}", scenario.population.persons.len());
    println!("network_links={}", scenario.network.links.len());
    println!("output_dir={}", output_dir.display());
    Ok(())
}

fn resolve_output_dir(config_path: &Path, configured_output: &str) -> PathBuf {
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let candidate = Path::new(configured_output);
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        config_dir.join(candidate)
    }
}

fn compare_command(left: &Path, right: &Path) -> Result<(), CliError> {
    for name in ["scorestats.csv", "modestats.csv", "traveldistancestats.csv"] {
        let left_path = left.join(name);
        let right_path = right.join(name);
        let left_text = fs::read_to_string(&left_path).map_err(|source| CliError::ReadFile {
            path: left_path.display().to_string(),
            source,
        })?;
        let right_text = fs::read_to_string(&right_path).map_err(|source| CliError::ReadFile {
            path: right_path.display().to_string(),
            source,
        })?;

        println!("== {name} ==");
        if left_text == right_text {
            println!("identical");
            continue;
        }

        let left_lines: Vec<_> = left_text.lines().collect();
        let right_lines: Vec<_> = right_text.lines().collect();
        let max_len = left_lines.len().max(right_lines.len());
        for index in 0..max_len {
            let left_line = left_lines.get(index).copied().unwrap_or("<missing>");
            let right_line = right_lines.get(index).copied().unwrap_or("<missing>");
            if left_line != right_line {
                println!("line {}:", index + 1);
                println!("  left : {left_line}");
                println!("  right: {right_line}");
            }
        }
    }
    Ok(())
}

fn explain_command(config_path: &Path, person_id: &str) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let breakdown =
        explain_person_score(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", breakdown.person_id);
    println!("total_score={:.6}", breakdown.total_score);
    for item in breakdown.items {
        println!(
            "{} start={} end={} score={:.6}",
            item.label, item.start_time_seconds, item.end_time_seconds, item.score
        );
    }
    Ok(())
}
