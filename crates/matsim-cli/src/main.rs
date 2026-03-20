use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use matsim_core::{explain_person_plans, explain_person_reroute, explain_person_score, run_iterations, write_outputs};
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
    ExplainReroute {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        person_id: String,
    },
    ExplainPlans {
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
        Command::ExplainReroute { config, person_id } => explain_reroute_command(&config, &person_id),
        Command::ExplainPlans { config, person_id } => explain_plans_command(&config, &person_id),
    }
}

fn run_command(config_path: &Path) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let output = run_iterations(&scenario);
    let output_dir = resolve_output_dir(config_path, &scenario.config.output_directory);
    write_outputs(&output_dir, &output)?;

    println!("random_seed={}", scenario.config.random_seed);
    println!("persons={}", scenario.population.persons.len());
    println!("network_links={}", scenario.network.links.len());
    println!("iterations={}", output.iterations.len());
    println!("strategies={}", scenario.config.replanning.strategies.len());
    if let Some(last) = output.iterations.last() {
        println!("last_iteration_score={:.6}", last.score_stats.avg_executed);
        println!(
            "last_iteration_plans={{avg_per_person:{:.6},max_per_person:{},selected_share:{:.6}}}",
            last.plan_memory_stats.avg_plans_per_person,
            last.plan_memory_stats.max_plans_per_person,
            last.plan_memory_stats.selected_plan_share
        );
        println!(
            "last_iteration_replanning={{strategies_considered:{},persons_replanned:{}}}",
            last.replanning_summary.strategies_considered, last.replanning_summary.persons_replanned
        );
        let mut bottlenecks = last.observed_link_costs.clone();
        bottlenecks.sort_by(|left, right| right.travel_time_seconds.total_cmp(&left.travel_time_seconds));
        for stat in bottlenecks.into_iter().take(3) {
            println!("last_iteration_link_cost[{}]={:.6}", stat.link_id, stat.travel_time_seconds);
        }
    }
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
    for name in [
        "scorestats.csv",
        "planstats.csv",
        "modestats.csv",
        "traveldistancestats.csv",
        "observed_link_costs.csv",
    ] {
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

fn explain_reroute_command(config_path: &Path, person_id: &str) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let explanation =
        explain_person_reroute(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", explanation.person_id);
    for leg in explanation.legs {
        println!(
            "leg={} mode={} current_cost={:.6} rerouted_cost={:.6}",
            leg.leg_index, leg.mode, leg.current_cost_seconds, leg.rerouted_cost_seconds
        );
        println!("  current_links={}", leg.current_link_ids.join(","));
        println!("  rerouted_nodes={}", leg.rerouted_node_ids.join(","));
        println!("  rerouted_links={}", leg.rerouted_link_ids.join(","));
    }
    Ok(())
}

fn explain_plans_command(config_path: &Path, person_id: &str) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let explanation =
        explain_person_plans(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", explanation.person_id);
    println!("selected_plan_index={}", explanation.selected_plan_index);
    println!("plans={}", explanation.plans.len());
    for plan in explanation.plans {
        println!(
            "plan={} selected={} score={} legs={} activities={}",
            plan.index,
            plan.selected,
            plan.score
                .map(|score| format!("{score:.6}"))
                .unwrap_or_else(|| "None".to_string()),
            plan.leg_count,
            plan.activity_count
        );
    }
    Ok(())
}
