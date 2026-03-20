use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{Parser, Subcommand};
use matsim_core::{
    analyze_event_groups, analyze_events, analyze_link_event_groups, explain_person_plans,
    explain_person_reroute, explain_person_reroute_score, explain_person_score,
    run_iterations_with_state, write_outputs,
};
use matsim_io::{load_events, load_scenario, write_population};
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
        #[arg(long)]
        iteration: Option<u32>,
    },
    ExplainLink {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        link_id: String,
        #[arg(long)]
        iteration: Option<u32>,
    },
    ExplainReroute {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        person_id: String,
        #[arg(long)]
        iteration: Option<u32>,
    },
    ExplainRerouteScore {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        person_id: String,
        #[arg(long)]
        iteration: Option<u32>,
    },
    ExplainPlans {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        person_id: String,
        #[arg(long)]
        iteration: Option<u32>,
    },
    InspectPerson {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        person_id: String,
        #[arg(long)]
        iteration: Option<u32>,
    },
    InspectPopulation {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        iteration: Option<u32>,
        #[arg(long, default_value = "id")]
        sort_by: PopulationSortKey,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = 0.0)]
        min_reroute_gain: f64,
        #[arg(long)]
        csv: bool,
        #[arg(long)]
        markdown: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    InspectNetwork {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        iteration: Option<u32>,
        #[arg(long, default_value = "delay")]
        sort_by: NetworkSortKey,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = 0.0)]
        min_delay: f64,
        #[arg(long)]
        csv: bool,
        #[arg(long)]
        markdown: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    InspectReroutes {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        iteration: Option<u32>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = 0.0)]
        min_score_delta: f64,
        #[arg(long)]
        csv: bool,
        #[arg(long)]
        markdown: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    InspectRerouteScores {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        iteration: Option<u32>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        markdown: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    InspectBottleneck {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        link_id: String,
        #[arg(long)]
        iteration: Option<u32>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        csv: bool,
        #[arg(long)]
        markdown: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    InspectQueueChain {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        from_iteration: u32,
        #[arg(long)]
        from_link_id: String,
        #[arg(long)]
        to_iteration: u32,
        #[arg(long)]
        to_link_id: String,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        csv: bool,
        #[arg(long)]
        markdown: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    AnalyzeEvents {
        #[arg(long)]
        config: PathBuf,
    },
    AnalyzeEventsFile {
        #[arg(long)]
        events: PathBuf,
    },
    AnalyzeLinkEvents {
        #[arg(long)]
        config: PathBuf,
    },
    AnalyzeLinkEventsFile {
        #[arg(long)]
        events: PathBuf,
    },
}

#[derive(Debug, Clone, Copy)]
enum PopulationSortKey {
    Id,
    Score,
    RerouteGain,
    Plans,
}

#[derive(Debug, Clone, Copy)]
enum NetworkSortKey {
    Id,
    Delay,
    TravelTime,
    Traversals,
}

impl FromStr for NetworkSortKey {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "id" => Ok(Self::Id),
            "delay" => Ok(Self::Delay),
            "travel-time" => Ok(Self::TravelTime),
            "traversals" => Ok(Self::Traversals),
            _ => Err(format!("unsupported sort key `{value}`")),
        }
    }
}

impl FromStr for PopulationSortKey {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "id" => Ok(Self::Id),
            "score" => Ok(Self::Score),
            "reroute-gain" => Ok(Self::RerouteGain),
            "plans" => Ok(Self::Plans),
            _ => Err(format!("unsupported sort key `{value}`")),
        }
    }
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
    #[error("link `{0}` not found")]
    LinkNotFound(String),
    #[error("iteration `{requested}` not found; available through `{last_available}`")]
    IterationNotFound { requested: u32, last_available: u32 },
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
        Command::Explain {
            config,
            person_id,
            iteration,
        } => explain_command(&config, &person_id, iteration),
        Command::ExplainLink {
            config,
            link_id,
            iteration,
        } => explain_link_command(&config, &link_id, iteration),
        Command::ExplainReroute {
            config,
            person_id,
            iteration,
        } => explain_reroute_command(&config, &person_id, iteration),
        Command::ExplainRerouteScore {
            config,
            person_id,
            iteration,
        } => explain_reroute_score_command(&config, &person_id, iteration),
        Command::ExplainPlans {
            config,
            person_id,
            iteration,
        } => explain_plans_command(&config, &person_id, iteration),
        Command::InspectPerson {
            config,
            person_id,
            iteration,
        } => inspect_person_command(&config, &person_id, iteration),
        Command::InspectPopulation {
            config,
            iteration,
            sort_by,
            limit,
            min_reroute_gain,
            csv,
            markdown,
            output,
        } => inspect_population_command(
            &config,
            iteration,
            sort_by,
            limit,
            min_reroute_gain,
            csv,
            markdown,
            output,
        ),
        Command::InspectNetwork {
            config,
            iteration,
            sort_by,
            limit,
            min_delay,
            csv,
            markdown,
            output,
        } => inspect_network_command(
            &config, iteration, sort_by, limit, min_delay, csv, markdown, output,
        ),
        Command::InspectReroutes {
            config,
            iteration,
            limit,
            min_score_delta,
            csv,
            markdown,
            output,
        } => inspect_reroutes_command(
            &config,
            iteration,
            limit,
            min_score_delta,
            csv,
            markdown,
            output,
        ),
        Command::InspectRerouteScores {
            config,
            iteration,
            limit,
            markdown,
            output,
        } => inspect_reroute_scores_command(&config, iteration, limit, markdown, output),
        Command::InspectBottleneck {
            config,
            link_id,
            iteration,
            limit,
            csv,
            markdown,
            output,
        } => inspect_bottleneck_command(&config, &link_id, iteration, limit, csv, markdown, output),
        Command::InspectQueueChain {
            config,
            from_iteration,
            from_link_id,
            to_iteration,
            to_link_id,
            limit,
            csv,
            markdown,
            output,
        } => inspect_queue_chain_command(
            &config,
            from_iteration,
            &from_link_id,
            to_iteration,
            &to_link_id,
            limit,
            csv,
            markdown,
            output,
        ),
        Command::AnalyzeEvents { config } => analyze_events_command(&config),
        Command::AnalyzeEventsFile { events } => analyze_events_file_command(&events),
        Command::AnalyzeLinkEvents { config } => analyze_link_events_command(&config),
        Command::AnalyzeLinkEventsFile { events } => analyze_link_events_file_command(&events),
    }
}

fn run_command(config_path: &Path) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (output, final_state) = run_iterations_with_state(&scenario);
    let output_dir = resolve_output_dir(config_path, &scenario.config.output_directory);
    write_outputs(&output_dir, &output)?;
    write_population(
        &output_dir.join("output_plans.xml"),
        &final_state.population,
    )?;

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
            "last_iteration_replanning={{strategies_considered:{},persons_replanned:{},plan_delta:{}}}",
            last.replanning_summary.strategies_considered,
            last.replanning_summary.persons_replanned,
            last.replanning_summary.plan_delta
        );
        for stat in &last.replanning_summary.strategy_stats {
            println!(
                "last_iteration_strategy[{}]={{sampled:{},applied:{}}}",
                stat.strategy_name, stat.sampled, stat.applied
            );
        }
        let mut bottlenecks = last.observed_link_costs.clone();
        bottlenecks.sort_by(|left, right| {
            right
                .travel_time_seconds
                .total_cmp(&left.travel_time_seconds)
        });
        for stat in bottlenecks.into_iter().take(3) {
            println!(
                "last_iteration_link_cost[{}]={:.6}",
                stat.link_id, stat.travel_time_seconds
            );
        }
    }
    println!("output_dir={}", output_dir.display());
    Ok(())
}

fn analyze_events_command(config_path: &Path) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (output, _) = run_iterations_with_state(&scenario);
    let analyses = analyze_events(&output);

    println!("iteration;avg_leg_travel_time_seconds;avg_activity_duration_seconds;departures;arrivals;link_enters;link_leaves;activity_starts;activity_ends");
    for analysis in analyses {
        println!(
            "{};{:.6};{:.6};{};{};{};{};{};{}",
            analysis.iteration,
            analysis.avg_leg_travel_time_seconds,
            analysis.avg_activity_duration_seconds,
            analysis.departures,
            analysis.arrivals,
            analysis.link_enters,
            analysis.link_leaves,
            analysis.activity_starts,
            analysis.activity_ends
        );
    }
    Ok(())
}

fn analyze_events_file_command(events_path: &Path) -> Result<(), CliError> {
    let grouped = load_events(events_path)?;
    let analyses = analyze_event_groups(&grouped);

    println!("iteration;avg_leg_travel_time_seconds;avg_activity_duration_seconds;departures;arrivals;link_enters;link_leaves;activity_starts;activity_ends");
    for analysis in analyses {
        println!(
            "{};{:.6};{:.6};{};{};{};{};{};{}",
            analysis.iteration,
            analysis.avg_leg_travel_time_seconds,
            analysis.avg_activity_duration_seconds,
            analysis.departures,
            analysis.arrivals,
            analysis.link_enters,
            analysis.link_leaves,
            analysis.activity_starts,
            analysis.activity_ends
        );
    }
    Ok(())
}

fn analyze_link_events_command(config_path: &Path) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (output, _) = run_iterations_with_state(&scenario);
    let grouped = output
        .iterations
        .iter()
        .map(|iteration| (iteration.iteration, iteration.events.clone()))
        .collect::<Vec<_>>();
    let analyses = analyze_link_event_groups(&grouped);

    println!("iteration;link_id;avg_travel_time_seconds;traversals");
    for analysis in analyses {
        println!(
            "{};{};{:.6};{}",
            analysis.iteration,
            analysis.link_id,
            analysis.avg_travel_time_seconds,
            analysis.traversals
        );
    }
    Ok(())
}

fn analyze_link_events_file_command(events_path: &Path) -> Result<(), CliError> {
    let grouped = load_events(events_path)?;
    let analyses = analyze_link_event_groups(&grouped);

    println!("iteration;link_id;avg_travel_time_seconds;traversals");
    for analysis in analyses {
        println!(
            "{};{};{:.6};{}",
            analysis.iteration,
            analysis.link_id,
            analysis.avg_travel_time_seconds,
            analysis.traversals
        );
    }
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
        "observed_link_profiles.csv",
        "eventstats.csv",
        "link_eventstats.csv",
        "replanningstats.csv",
        "reroutestats.csv",
    ] {
        let left_path = left.join(name);
        let right_path = right.join(name);
        let left_text = match fs::read_to_string(&left_path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                println!("== {name} ==");
                println!("skipped: missing left file {}", left_path.display());
                continue;
            }
            Err(source) => {
                return Err(CliError::ReadFile {
                    path: left_path.display().to_string(),
                    source,
                })
            }
        };
        let right_text = match fs::read_to_string(&right_path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                println!("== {name} ==");
                println!("skipped: missing right file {}", right_path.display());
                continue;
            }
            Err(source) => {
                return Err(CliError::ReadFile {
                    path: right_path.display().to_string(),
                    source,
                })
            }
        };

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

fn explain_command(
    config_path: &Path,
    person_id: &str,
    iteration: Option<u32>,
) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let breakdown = explain_person_score(&scenario, person_id)
        .ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", breakdown.person_id);
    if let Some(iteration) = iteration {
        println!("iteration={iteration}");
    }
    println!("total_score={:.6}", breakdown.total_score);
    for item in breakdown.items {
        println!(
            "{} start={} end={} score={:.6}",
            item.label, item.start_time_seconds, item.end_time_seconds, item.score
        );
    }
    Ok(())
}

fn explain_link_command(
    config_path: &Path,
    link_id: &str,
    iteration: Option<u32>,
) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (output, _) = run_iterations_with_state(&scenario);
    let target_iteration = iteration.unwrap_or(output.last_iteration);
    let selected = output
        .iterations
        .iter()
        .find(|candidate| candidate.iteration == target_iteration)
        .ok_or(CliError::IterationNotFound {
            requested: target_iteration,
            last_available: output.last_iteration,
        })?;
    let link = scenario
        .network
        .links
        .get(link_id)
        .ok_or_else(|| CliError::LinkNotFound(link_id.to_string()))?;
    let event_groups = vec![(selected.iteration, selected.events.clone())];
    let event_stat = analyze_link_event_groups(&event_groups)
        .into_iter()
        .find(|candidate| candidate.link_id == link_id);
    let observed_travel_time = selected
        .observed_link_costs
        .iter()
        .find(|candidate| candidate.link_id == link_id)
        .map(|candidate| candidate.travel_time_seconds)
        .unwrap_or(link.length_m / link.freespeed_mps);
    let freespeed_travel_time = link.length_m / link.freespeed_mps;

    println!("link_id={link_id}");
    println!("iteration={target_iteration}");
    println!("freespeed_travel_time_seconds={:.6}", freespeed_travel_time);
    println!("observed_travel_time_seconds={:.6}", observed_travel_time);
    if let Some(event_stat) = event_stat {
        println!(
            "event_travel_time_seconds={:.6}",
            event_stat.avg_travel_time_seconds
        );
        println!("traversals={}", event_stat.traversals);
    } else {
        println!("event_travel_time_seconds={:.6}", freespeed_travel_time);
        println!("traversals=0");
    }
    println!(
        "avg_delay_seconds={:.6}",
        (observed_travel_time - freespeed_travel_time).max(0.0)
    );
    Ok(())
}

fn explain_reroute_command(
    config_path: &Path,
    person_id: &str,
    iteration: Option<u32>,
) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let explanation = explain_person_reroute(&scenario, person_id)
        .ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", explanation.person_id);
    if let Some(iteration) = iteration {
        println!("iteration={iteration}");
    }
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

fn explain_reroute_score_command(
    config_path: &Path,
    person_id: &str,
    iteration: Option<u32>,
) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let breakdown = explain_person_reroute_score(&scenario, person_id)
        .ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", breakdown.person_id);
    if let Some(iteration) = iteration {
        println!("iteration={iteration}");
    }
    println!("current_total_score={:.6}", breakdown.current_total_score);
    println!("rerouted_total_score={:.6}", breakdown.rerouted_total_score);
    println!(
        "delta={:.6}",
        breakdown.rerouted_total_score - breakdown.current_total_score
    );
    for item in breakdown.items {
        println!(
            "{} current={:.6} rerouted={:.6} delta={:.6}",
            item.label, item.current_score, item.rerouted_score, item.delta
        );
    }
    Ok(())
}

fn explain_plans_command(
    config_path: &Path,
    person_id: &str,
    iteration: Option<u32>,
) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let explanation = explain_person_plans(&scenario, person_id)
        .ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", explanation.person_id);
    if let Some(iteration) = iteration {
        println!("iteration={iteration}");
    }
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

fn inspect_person_command(
    config_path: &Path,
    person_id: &str,
    iteration: Option<u32>,
) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let plans = explain_person_plans(&scenario, person_id)
        .ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;
    let reroute = explain_person_reroute(&scenario, person_id)
        .ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;
    let score = explain_person_score(&scenario, person_id)
        .ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

    println!("person_id={}", person_id);
    if let Some(iteration) = iteration {
        println!("iteration={iteration}");
    }

    println!("\n[plans]");
    println!("selected_plan_index={}", plans.selected_plan_index);
    println!("plans={}", plans.plans.len());
    for plan in plans.plans {
        println!(
            "plan={} selected={} score={} legs={} activities={}",
            plan.index,
            plan.selected,
            plan.score
                .map(|value| format!("{value:.6}"))
                .unwrap_or_else(|| "None".to_string()),
            plan.leg_count,
            plan.activity_count
        );
    }

    println!("\n[reroute]");
    for leg in reroute.legs {
        println!(
            "leg={} mode={} current_cost={:.6} rerouted_cost={:.6}",
            leg.leg_index, leg.mode, leg.current_cost_seconds, leg.rerouted_cost_seconds
        );
        println!("  current_links={}", leg.current_link_ids.join(","));
        println!("  rerouted_nodes={}", leg.rerouted_node_ids.join(","));
        println!("  rerouted_links={}", leg.rerouted_link_ids.join(","));
    }

    println!("\n[score]");
    println!("total_score={:.6}", score.total_score);
    for item in score.items {
        println!(
            "{} start={} end={} score={:.6}",
            item.label, item.start_time_seconds, item.end_time_seconds, item.score
        );
    }

    Ok(())
}

fn inspect_population_command(
    config_path: &Path,
    iteration: Option<u32>,
    sort_by: PopulationSortKey,
    limit: Option<usize>,
    min_reroute_gain: f64,
    csv: bool,
    markdown: bool,
    output: Option<PathBuf>,
) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let mut rows = Vec::new();

    for person in &scenario.population.persons {
        let plans = explain_person_plans(&scenario, &person.id)
            .ok_or_else(|| CliError::PersonNotFound(person.id.clone()))?;
        let reroute = explain_person_reroute(&scenario, &person.id)
            .ok_or_else(|| CliError::PersonNotFound(person.id.clone()))?;
        let score = explain_person_score(&scenario, &person.id)
            .ok_or_else(|| CliError::PersonNotFound(person.id.clone()))?;
        let reroute_gain = reroute
            .legs
            .iter()
            .map(|leg| (leg.current_cost_seconds - leg.rerouted_cost_seconds).max(0.0))
            .sum::<f64>();
        let current_links = reroute
            .legs
            .iter()
            .flat_map(|leg| leg.current_link_ids.iter().cloned())
            .collect::<Vec<_>>()
            .join(",");

        rows.push((
            person.id.clone(),
            plans.selected_plan_index,
            plans.plans.len(),
            score.total_score,
            reroute_gain,
            current_links,
        ));
    }

    rows.retain(|row| row.4 >= min_reroute_gain);

    match sort_by {
        PopulationSortKey::Id => rows.sort_by(|left, right| left.0.cmp(&right.0)),
        PopulationSortKey::Score => rows.sort_by(|left, right| {
            right
                .3
                .total_cmp(&left.3)
                .then_with(|| left.0.cmp(&right.0))
        }),
        PopulationSortKey::RerouteGain => rows.sort_by(|left, right| {
            right
                .4
                .total_cmp(&left.4)
                .then_with(|| left.0.cmp(&right.0))
        }),
        PopulationSortKey::Plans => {
            rows.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)))
        }
    }

    if let Some(limit) = limit {
        rows.truncate(limit);
    }

    let mut text = String::new();
    if csv {
        text.push_str(
            "person_id;selected_plan_index;plans;selected_score;reroute_gain;current_links\n",
        );
        for row in rows {
            text.push_str(&format!(
                "{};{};{};{:.6};{:.6};{}\n",
                row.0, row.1, row.2, row.3, row.4, row.5
            ));
        }
        return emit_text(&text, output);
    }

    if markdown {
        text.push_str("# Population Inspection\n");
        if let Some(iteration) = iteration {
            text.push_str(&format!("\n- iteration: {iteration}\n"));
        }
        text.push_str(&format!(
            "- persons: {}\n",
            scenario.population.persons.len()
        ));
        text.push_str(&format!(
            "- sort_by: {}\n",
            match sort_by {
                PopulationSortKey::Id => "id",
                PopulationSortKey::Score => "score",
                PopulationSortKey::RerouteGain => "reroute-gain",
                PopulationSortKey::Plans => "plans",
            }
        ));
        text.push_str(&format!("- min_reroute_gain: {:.6}\n", min_reroute_gain));
        if let Some(limit) = limit {
            text.push_str(&format!("- limit: {limit}\n"));
        }
        text.push_str(
            "\n| person_id | selected_plan_index | plans | selected_score | reroute_gain | current_links |\n",
        );
        text.push_str("| --- | ---: | ---: | ---: | ---: | --- |\n");
        for row in rows {
            text.push_str(&format!(
                "| {} | {} | {} | {:.6} | {:.6} | {} |",
                row.0, row.1, row.2, row.3, row.4, row.5
            ));
            text.push('\n');
        }
        return emit_text(&text, output);
    }

    if let Some(iteration) = iteration {
        text.push_str(&format!("iteration={iteration}\n"));
    }
    text.push_str(&format!("persons={}\n", scenario.population.persons.len()));
    text.push_str(&format!(
        "sort_by={}\n",
        match sort_by {
            PopulationSortKey::Id => "id",
            PopulationSortKey::Score => "score",
            PopulationSortKey::RerouteGain => "reroute-gain",
            PopulationSortKey::Plans => "plans",
        }
    ));
    if let Some(limit) = limit {
        text.push_str(&format!("limit={limit}\n"));
    }
    text.push_str(&format!("min_reroute_gain={min_reroute_gain:.6}\n"));
    text.push_str(
        "person_id;selected_plan_index;plans;selected_score;reroute_gain;current_links\n",
    );
    for row in rows {
        text.push_str(&format!(
            "{};{};{};{:.6};{:.6};{}\n",
            row.0, row.1, row.2, row.3, row.4, row.5
        ));
    }

    emit_text(&text, output)
}

fn inspect_network_command(
    config_path: &Path,
    iteration: Option<u32>,
    sort_by: NetworkSortKey,
    limit: Option<usize>,
    min_delay: f64,
    csv: bool,
    markdown: bool,
    output: Option<PathBuf>,
) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (run_output, _) = run_iterations_with_state(&scenario);
    let target_iteration = iteration.unwrap_or(run_output.last_iteration);
    let selected = run_output
        .iterations
        .iter()
        .find(|candidate| candidate.iteration == target_iteration)
        .ok_or(CliError::IterationNotFound {
            requested: target_iteration,
            last_available: run_output.last_iteration,
        })?;
    let grouped = vec![(selected.iteration, selected.events.clone())];
    let event_stats = analyze_link_event_groups(&grouped)
        .into_iter()
        .map(|stat| (stat.link_id.clone(), stat))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut rows = scenario
        .network
        .links
        .values()
        .map(|link| {
            let freespeed = link.length_m / link.freespeed_mps;
            let observed = selected
                .observed_link_costs
                .iter()
                .find(|candidate| candidate.link_id == link.id)
                .map(|candidate| candidate.travel_time_seconds)
                .unwrap_or(freespeed);
            let event_stat = event_stats.get(&link.id);
            let traversals = event_stat.map(|stat| stat.traversals).unwrap_or(0);
            let event_time = event_stat
                .map(|stat| stat.avg_travel_time_seconds)
                .unwrap_or(freespeed);
            let delay = (observed - freespeed).max(0.0);
            (
                link.id.clone(),
                freespeed,
                observed,
                event_time,
                delay,
                traversals,
            )
        })
        .collect::<Vec<_>>();

    rows.retain(|row| row.4 >= min_delay);

    match sort_by {
        NetworkSortKey::Id => rows.sort_by(|left, right| left.0.cmp(&right.0)),
        NetworkSortKey::Delay => rows.sort_by(|left, right| {
            right
                .4
                .total_cmp(&left.4)
                .then_with(|| left.0.cmp(&right.0))
        }),
        NetworkSortKey::TravelTime => rows.sort_by(|left, right| {
            right
                .2
                .total_cmp(&left.2)
                .then_with(|| left.0.cmp(&right.0))
        }),
        NetworkSortKey::Traversals => {
            rows.sort_by(|left, right| right.5.cmp(&left.5).then_with(|| left.0.cmp(&right.0)))
        }
    }

    if let Some(limit) = limit {
        rows.truncate(limit);
    }

    let mut text = String::new();
    if csv {
        text.push_str("link_id;freespeed_travel_time;observed_travel_time;event_travel_time;avg_delay;traversals\n");
        for row in rows {
            text.push_str(&format!(
                "{};{:.6};{:.6};{:.6};{:.6};{}\n",
                row.0, row.1, row.2, row.3, row.4, row.5
            ));
        }
        return emit_text(&text, output);
    }

    if markdown {
        text.push_str("# Network Inspection\n");
        text.push_str(&format!("\n- iteration: {target_iteration}\n"));
        text.push_str(&format!("- links: {}\n", scenario.network.links.len()));
        text.push_str(&format!(
            "- sort_by: {}\n",
            match sort_by {
                NetworkSortKey::Id => "id",
                NetworkSortKey::Delay => "delay",
                NetworkSortKey::TravelTime => "travel-time",
                NetworkSortKey::Traversals => "traversals",
            }
        ));
        text.push_str(&format!("- min_delay: {:.6}\n", min_delay));
        if let Some(limit) = limit {
            text.push_str(&format!("- limit: {limit}\n"));
        }
        text.push_str(
            "\n| link_id | freespeed_tt | observed_tt | event_tt | avg_delay | traversals |\n",
        );
        text.push_str("|---|---:|---:|---:|---:|---:|\n");
        for row in rows {
            text.push_str(&format!(
                "| {} | {:.6} | {:.6} | {:.6} | {:.6} | {} |\n",
                row.0, row.1, row.2, row.3, row.4, row.5
            ));
        }
        return emit_text(&text, output);
    }

    text.push_str(&format!("iteration={target_iteration}\n"));
    text.push_str(&format!("links={}\n", scenario.network.links.len()));
    text.push_str(&format!(
        "sort_by={}\n",
        match sort_by {
            NetworkSortKey::Id => "id",
            NetworkSortKey::Delay => "delay",
            NetworkSortKey::TravelTime => "travel-time",
            NetworkSortKey::Traversals => "traversals",
        }
    ));
    text.push_str(&format!("min_delay={:.6}\n", min_delay));
    if let Some(limit) = limit {
        text.push_str(&format!("limit={limit}\n"));
    }
    for row in rows {
        text.push_str(&format!(
            "link_id={} freespeed_tt={:.6} observed_tt={:.6} event_tt={:.6} avg_delay={:.6} traversals={}\n",
            row.0, row.1, row.2, row.3, row.4, row.5
        ));
    }
    emit_text(&text, output)
}

fn inspect_reroutes_command(
    config_path: &Path,
    iteration: Option<u32>,
    limit: Option<usize>,
    min_score_delta: f64,
    csv: bool,
    markdown: bool,
    output: Option<PathBuf>,
) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (run_output, _) = run_iterations_with_state(&scenario);
    let target_iteration = iteration.unwrap_or(run_output.last_iteration);
    let selected = run_output
        .iterations
        .iter()
        .find(|candidate| candidate.iteration == target_iteration)
        .ok_or(CliError::IterationNotFound {
            requested: target_iteration,
            last_available: run_output.last_iteration,
        })?;

    let delayed_links = selected
        .observed_link_profiles
        .iter()
        .filter(|stat| stat.delay_seconds > 0.0)
        .map(|stat| stat.link_id.clone())
        .collect::<std::collections::BTreeSet<_>>();

    let mut rows = selected
        .replanning_summary
        .reroute_details
        .iter()
        .map(|detail| {
            let previous_delay_links = detail
                .previous_links
                .split(['|', ','])
                .filter(|link_id| !link_id.is_empty() && delayed_links.contains(*link_id))
                .count();
            let rerouted_delay_links = detail
                .rerouted_links
                .split(['|', ','])
                .filter(|link_id| !link_id.is_empty() && delayed_links.contains(*link_id))
                .count();
            (
                detail.person_id.clone(),
                detail.previous_score,
                detail.estimated_rerouted_score,
                detail.estimated_rerouted_score - detail.previous_score,
                previous_delay_links,
                rerouted_delay_links,
                detail.previous_links.clone(),
                detail.rerouted_links.clone(),
            )
        })
        .filter(|row| row.3 >= min_score_delta)
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        right
            .3
            .total_cmp(&left.3)
            .then_with(|| left.0.cmp(&right.0))
    });
    if let Some(limit) = limit {
        rows.truncate(limit);
    }

    let mut text = String::new();
    if csv {
        text.push_str("person_id;previous_score;estimated_rerouted_score;estimated_score_delta;previous_delay_links;rerouted_delay_links;previous_links;rerouted_links\n");
        for row in rows {
            text.push_str(&format!(
                "{};{:.6};{:.6};{:.6};{};{};{};{}\n",
                row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7
            ));
        }
        return emit_text(&text, output);
    }

    if markdown {
        text.push_str("# Reroute Inspection\n");
        text.push_str(&format!("\n- iteration: {target_iteration}\n"));
        text.push_str(&format!("- delayed_links: {}\n", delayed_links.len()));
        text.push_str(&format!("- min_score_delta: {:.6}\n", min_score_delta));
        if let Some(limit) = limit {
            text.push_str(&format!("- limit: {limit}\n"));
        }
        text.push_str("\n| person_id | prev_score | rerouted_score | score_delta | prev_delay_links | rerouted_delay_links |\n");
        text.push_str("|---|---:|---:|---:|---:|---:|\n");
        for row in rows {
            text.push_str(&format!(
                "| {} | {:.6} | {:.6} | {:.6} | {} | {} |\n",
                row.0, row.1, row.2, row.3, row.4, row.5
            ));
        }
        return emit_text(&text, output);
    }

    text.push_str(&format!("iteration={target_iteration}\n"));
    text.push_str(&format!("delayed_links={}\n", delayed_links.len()));
    text.push_str(&format!("min_score_delta={min_score_delta:.6}\n"));
    if let Some(limit) = limit {
        text.push_str(&format!("limit={limit}\n"));
    }
    text.push_str("person_id;previous_score;estimated_rerouted_score;estimated_score_delta;previous_delay_links;rerouted_delay_links;previous_links;rerouted_links\n");
    for row in rows {
        text.push_str(&format!(
            "{};{:.6};{:.6};{:.6};{};{};{};{}\n",
            row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7
        ));
    }
    emit_text(&text, output)
}

fn inspect_reroute_scores_command(
    config_path: &Path,
    iteration: Option<u32>,
    limit: Option<usize>,
    markdown: bool,
    output: Option<PathBuf>,
) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (run_output, _) = run_iterations_with_state(&scenario);
    let target_iteration = iteration.unwrap_or(run_output.last_iteration);
    let selected = run_output
        .iterations
        .iter()
        .find(|candidate| candidate.iteration == target_iteration)
        .ok_or(CliError::IterationNotFound {
            requested: target_iteration,
            last_available: run_output.last_iteration,
        })?;
    let scenario_before = resolve_scenario_before_iteration(config_path, target_iteration)?;

    let mut aggregates = std::collections::BTreeMap::<String, (String, f64, usize, usize)>::new();
    for detail in &selected.replanning_summary.reroute_details {
        let breakdown = explain_person_reroute_score(&scenario_before, &detail.person_id)
            .ok_or_else(|| CliError::PersonNotFound(detail.person_id.clone()))?;
        for (component_index, item) in breakdown.items.into_iter().enumerate() {
            let key = format!("{:02}:{}", component_index, item.label);
            let entry = aggregates.entry(key).or_insert((item.label, 0.0, 0, 0));
            entry.1 += item.delta;
            entry.2 += 1;
            if item.delta > 0.0 {
                entry.3 += 1;
            }
        }
    }

    let mut rows = aggregates
        .into_iter()
        .map(|(component, (label, total_delta, count, positive_count))| {
            (
                component,
                label,
                total_delta,
                if count > 0 {
                    total_delta / count as f64
                } else {
                    0.0
                },
                count,
                positive_count,
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .2
            .total_cmp(&left.2)
            .then_with(|| left.0.cmp(&right.0))
    });
    if let Some(limit) = limit {
        rows.truncate(limit);
    }

    let mut text = String::new();
    if markdown {
        text.push_str("# Reroute Score Components\n");
        text.push_str(&format!("\n- iteration: {target_iteration}\n"));
        text.push_str(&format!(
            "- rerouted_persons: {}\n",
            selected.replanning_summary.reroute_details.len()
        ));
        if let Some(limit) = limit {
            text.push_str(&format!("- limit: {limit}\n"));
        }
        text.push_str(
            "\n| component | label | total_delta | avg_delta | count | positive_count |\n",
        );
        text.push_str("|---|---|---:|---:|---:|---:|\n");
        for row in rows {
            text.push_str(&format!(
                "| {} | {} | {:.6} | {:.6} | {} | {} |\n",
                row.0, row.1, row.2, row.3, row.4, row.5
            ));
        }
        return emit_text(&text, output);
    }

    text.push_str(&format!("iteration={target_iteration}\n"));
    text.push_str(&format!(
        "rerouted_persons={}\n",
        selected.replanning_summary.reroute_details.len()
    ));
    if let Some(limit) = limit {
        text.push_str(&format!("limit={limit}\n"));
    }
    text.push_str("component;label;total_delta;avg_delta;count;positive_count\n");
    for row in rows {
        text.push_str(&format!(
            "{};{};{:.6};{:.6};{};{}\n",
            row.0, row.1, row.2, row.3, row.4, row.5
        ));
    }
    emit_text(&text, output)
}

fn inspect_bottleneck_command(
    config_path: &Path,
    link_id: &str,
    iteration: Option<u32>,
    limit: Option<usize>,
    csv: bool,
    markdown: bool,
    output: Option<PathBuf>,
) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (run_output, _) = run_iterations_with_state(&scenario);
    let target_iteration = iteration.unwrap_or(run_output.last_iteration);
    let selected = run_output
        .iterations
        .iter()
        .find(|candidate| candidate.iteration == target_iteration)
        .ok_or(CliError::IterationNotFound {
            requested: target_iteration,
            last_available: run_output.last_iteration,
        })?;

    let person_scores = selected
        .person_score_stats
        .iter()
        .map(|stat| (stat.person_id.clone(), stat))
        .collect::<std::collections::BTreeMap<_, _>>();
    let reroute_scores = selected
        .replanning_summary
        .reroute_details
        .iter()
        .map(|detail| {
            (
                detail.person_id.clone(),
                detail.estimated_rerouted_score - detail.previous_score,
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut rows = selected
        .link_traversals
        .iter()
        .filter(|traversal| traversal.link_id == link_id)
        .map(|traversal| {
            let person_score = person_scores.get(&traversal.person_id);
            let queue_delay = (traversal.queue_exit_time_seconds
                - traversal.free_speed_exit_time_seconds)
                .max(0.0);
            let reroute_delta = reroute_scores
                .get(&traversal.person_id)
                .copied()
                .unwrap_or(0.0);
            (
                traversal.same_enter_rank,
                traversal.person_id.clone(),
                traversal.leg_index,
                traversal.enter_time_seconds,
                traversal.same_enter_group_size,
                queue_delay,
                person_score.map(|value| value.executed).unwrap_or(0.0),
                person_score.map(|value| value.worst).unwrap_or(0.0),
                person_score.map(|value| value.average).unwrap_or(0.0),
                person_score.map(|value| value.best).unwrap_or(0.0),
                reroute_delta,
            )
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    if let Some(limit) = limit {
        rows.truncate(limit);
    }

    let mut text = String::new();
    if csv {
        text.push_str("same_enter_rank;person_id;leg_index;enter_time_seconds;same_enter_group_size;queue_delay_seconds;executed;worst;average;best;estimated_reroute_score_delta\n");
        for row in rows {
            text.push_str(&format!(
                "{};{};{};{:.6};{};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6}\n",
                row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10
            ));
        }
        return emit_text(&text, output);
    }

    if markdown {
        text.push_str("# Bottleneck Inspection\n");
        text.push_str(&format!("\n- iteration: {target_iteration}\n"));
        text.push_str(&format!("- link_id: {link_id}\n"));
        text.push_str(&format!("- traversals: {}\n", rows.len()));
        if let Some(limit) = limit {
            text.push_str(&format!("- limit: {limit}\n"));
        }
        text.push_str(
            "\n| rank | person_id | leg | enter_time | group_size | queue_delay | executed | worst | average | best | reroute_delta |\n",
        );
        text.push_str("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for row in rows {
            text.push_str(&format!(
                "| {} | {} | {} | {:.6} | {} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} |\n",
                row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10
            ));
        }
        return emit_text(&text, output);
    }

    text.push_str(&format!("iteration={target_iteration}\n"));
    text.push_str(&format!("link_id={link_id}\n"));
    if let Some(limit) = limit {
        text.push_str(&format!("limit={limit}\n"));
    }
    text.push_str(
        "same_enter_rank;person_id;leg_index;enter_time_seconds;same_enter_group_size;queue_delay_seconds;executed;worst;average;best;estimated_reroute_score_delta\n",
    );
    for row in rows {
        text.push_str(&format!(
            "{};{};{};{:.6};{};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6}\n",
            row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10
        ));
    }
    emit_text(&text, output)
}

fn inspect_queue_chain_command(
    config_path: &Path,
    from_iteration: u32,
    from_link_id: &str,
    to_iteration: u32,
    to_link_id: &str,
    limit: Option<usize>,
    csv: bool,
    markdown: bool,
    output: Option<PathBuf>,
) -> Result<(), CliError> {
    let scenario = load_scenario(config_path)?;
    let (run_output, _) = run_iterations_with_state(&scenario);
    let from_selected = run_output
        .iterations
        .iter()
        .find(|candidate| candidate.iteration == from_iteration)
        .ok_or(CliError::IterationNotFound {
            requested: from_iteration,
            last_available: run_output.last_iteration,
        })?;
    let to_selected = run_output
        .iterations
        .iter()
        .find(|candidate| candidate.iteration == to_iteration)
        .ok_or(CliError::IterationNotFound {
            requested: to_iteration,
            last_available: run_output.last_iteration,
        })?;

    let from_scores = from_selected
        .person_score_stats
        .iter()
        .map(|stat| (stat.person_id.clone(), stat))
        .collect::<std::collections::BTreeMap<_, _>>();
    let to_scores = to_selected
        .person_score_stats
        .iter()
        .map(|stat| (stat.person_id.clone(), stat))
        .collect::<std::collections::BTreeMap<_, _>>();
    let to_reroute_scores = to_selected
        .replanning_summary
        .reroute_details
        .iter()
        .map(|detail| {
            (
                detail.person_id.clone(),
                detail.estimated_rerouted_score - detail.previous_score,
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let to_traversals = to_selected
        .link_traversals
        .iter()
        .filter(|traversal| traversal.link_id == to_link_id)
        .map(|traversal| (traversal.person_id.clone(), traversal))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut rows = from_selected
        .link_traversals
        .iter()
        .filter(|traversal| traversal.link_id == from_link_id)
        .filter_map(|from_traversal| {
            let to_traversal = to_traversals.get(&from_traversal.person_id)?;
            let from_queue_delay = (from_traversal.queue_exit_time_seconds
                - from_traversal.free_speed_exit_time_seconds)
                .max(0.0);
            let to_queue_delay = (to_traversal.queue_exit_time_seconds
                - to_traversal.free_speed_exit_time_seconds)
                .max(0.0);
            let from_score = from_scores.get(&from_traversal.person_id);
            let to_score = to_scores.get(&from_traversal.person_id);
            Some((
                from_traversal.same_enter_rank,
                from_traversal.person_id.clone(),
                from_queue_delay,
                to_queue_delay,
                from_traversal.queue_exit_time_seconds,
                to_traversal.enter_time_seconds,
                from_score.map(|value| value.executed).unwrap_or(0.0),
                to_score.map(|value| value.executed).unwrap_or(0.0),
                to_score.map(|value| value.worst).unwrap_or(0.0),
                to_score.map(|value| value.average).unwrap_or(0.0),
                to_score.map(|value| value.best).unwrap_or(0.0),
                to_reroute_scores
                    .get(&from_traversal.person_id)
                    .copied()
                    .unwrap_or(0.0),
            ))
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    if let Some(limit) = limit {
        rows.truncate(limit);
    }

    let mut text = String::new();
    if csv {
        text.push_str("from_rank;person_id;from_queue_delay_seconds;to_queue_delay_seconds;from_queue_exit_time_seconds;to_enter_time_seconds;from_executed;to_executed;to_worst;to_average;to_best;to_estimated_reroute_score_delta\n");
        for row in rows {
            text.push_str(&format!(
                "{};{};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6}\n",
                row.0,
                row.1,
                row.2,
                row.3,
                row.4,
                row.5,
                row.6,
                row.7,
                row.8,
                row.9,
                row.10,
                row.11
            ));
        }
        return emit_text(&text, output);
    }

    if markdown {
        text.push_str("# Queue Chain Inspection\n");
        text.push_str(&format!(
            "\n- from: iteration {from_iteration} link {from_link_id}\n"
        ));
        text.push_str(&format!(
            "- to: iteration {to_iteration} link {to_link_id}\n"
        ));
        text.push_str(&format!("- matched_persons: {}\n", rows.len()));
        if let Some(limit) = limit {
            text.push_str(&format!("- limit: {limit}\n"));
        }
        text.push_str("\n| from_rank | person_id | from_delay | to_delay | from_exit | to_enter | from_executed | to_executed | to_worst | to_average | to_best | reroute_delta |\n");
        text.push_str("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for row in rows {
            text.push_str(&format!(
                "| {} | {} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} |\n",
                row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10, row.11
            ));
        }
        return emit_text(&text, output);
    }

    text.push_str(&format!("from_iteration={from_iteration}\n"));
    text.push_str(&format!("from_link_id={from_link_id}\n"));
    text.push_str(&format!("to_iteration={to_iteration}\n"));
    text.push_str(&format!("to_link_id={to_link_id}\n"));
    if let Some(limit) = limit {
        text.push_str(&format!("limit={limit}\n"));
    }
    text.push_str("from_rank;person_id;from_queue_delay_seconds;to_queue_delay_seconds;from_queue_exit_time_seconds;to_enter_time_seconds;from_executed;to_executed;to_worst;to_average;to_best;to_estimated_reroute_score_delta\n");
    for row in rows {
        text.push_str(&format!(
            "{};{};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6}\n",
            row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10, row.11
        ));
    }
    emit_text(&text, output)
}

fn emit_text(text: &str, output: Option<PathBuf>) -> Result<(), CliError> {
    if let Some(path) = output {
        fs::write(&path, text).map_err(|source| CliError::ReadFile {
            path: path.display().to_string(),
            source,
        })?;
    } else {
        print!("{text}");
    }
    Ok(())
}

fn resolve_scenario_for_iteration(
    config_path: &Path,
    iteration: Option<u32>,
) -> Result<matsim_core::Scenario, CliError> {
    let scenario = load_scenario(config_path)?;
    if let Some(iteration) = iteration {
        let mut scenario_for_run = scenario.clone();
        scenario_for_run.config.last_iteration =
            iteration.min(scenario_for_run.config.last_iteration);
        let (_, final_state) = run_iterations_with_state(&scenario_for_run);
        Ok(final_state)
    } else {
        Ok(scenario)
    }
}

fn resolve_scenario_before_iteration(
    config_path: &Path,
    iteration: u32,
) -> Result<matsim_core::Scenario, CliError> {
    if iteration == 0 {
        return load_scenario(config_path).map_err(CliError::from);
    }
    let scenario = load_scenario(config_path)?;
    let mut scenario_for_run = scenario.clone();
    scenario_for_run.config.last_iteration =
        (iteration - 1).min(scenario_for_run.config.last_iteration);
    let (_, final_state) = run_iterations_with_state(&scenario_for_run);
    Ok(final_state)
}
