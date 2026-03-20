use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{Parser, Subcommand};
use matsim_core::{
    analyze_event_groups, analyze_events, analyze_link_event_groups, explain_person_plans, explain_person_reroute,
    explain_person_score, run_iterations_with_state, write_outputs,
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
    ExplainReroute {
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
        Command::ExplainReroute {
            config,
            person_id,
            iteration,
        } => explain_reroute_command(&config, &person_id, iteration),
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
        } => inspect_population_command(&config, iteration, sort_by, limit, min_reroute_gain, csv, markdown, output),
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
    write_population(&output_dir.join("output_plans.xml"), &final_state.population)?;

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
            analysis.iteration, analysis.link_id, analysis.avg_travel_time_seconds, analysis.traversals
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
            analysis.iteration, analysis.link_id, analysis.avg_travel_time_seconds, analysis.traversals
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
        "eventstats.csv",
        "link_eventstats.csv",
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

fn explain_command(config_path: &Path, person_id: &str, iteration: Option<u32>) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let breakdown =
        explain_person_score(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

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

fn explain_reroute_command(config_path: &Path, person_id: &str, iteration: Option<u32>) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let explanation =
        explain_person_reroute(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

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

fn explain_plans_command(config_path: &Path, person_id: &str, iteration: Option<u32>) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let explanation =
        explain_person_plans(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

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

fn inspect_person_command(config_path: &Path, person_id: &str, iteration: Option<u32>) -> Result<(), CliError> {
    let scenario = resolve_scenario_for_iteration(config_path, iteration)?;
    let plans =
        explain_person_plans(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;
    let reroute =
        explain_person_reroute(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;
    let score =
        explain_person_score(&scenario, person_id).ok_or_else(|| CliError::PersonNotFound(person_id.to_string()))?;

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
        let plans =
            explain_person_plans(&scenario, &person.id).ok_or_else(|| CliError::PersonNotFound(person.id.clone()))?;
        let reroute = explain_person_reroute(&scenario, &person.id)
            .ok_or_else(|| CliError::PersonNotFound(person.id.clone()))?;
        let score =
            explain_person_score(&scenario, &person.id).ok_or_else(|| CliError::PersonNotFound(person.id.clone()))?;
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
        PopulationSortKey::Score => rows.sort_by(|left, right| right.3.total_cmp(&left.3).then_with(|| left.0.cmp(&right.0))),
        PopulationSortKey::RerouteGain => {
            rows.sort_by(|left, right| right.4.total_cmp(&left.4).then_with(|| left.0.cmp(&right.0)))
        }
        PopulationSortKey::Plans => rows.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0))),
    }

    if let Some(limit) = limit {
        rows.truncate(limit);
    }

    let mut text = String::new();
    if csv {
        text.push_str("person_id;selected_plan_index;plans;selected_score;reroute_gain;current_links\n");
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
        text.push_str(&format!("- persons: {}\n", scenario.population.persons.len()));
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
    text.push_str(&format!("sort_by={}\n", match sort_by {
        PopulationSortKey::Id => "id",
        PopulationSortKey::Score => "score",
        PopulationSortKey::RerouteGain => "reroute-gain",
        PopulationSortKey::Plans => "plans",
    }));
    if let Some(limit) = limit {
        text.push_str(&format!("limit={limit}\n"));
    }
    text.push_str(&format!("min_reroute_gain={min_reroute_gain:.6}\n"));
    text.push_str("person_id;selected_plan_index;plans;selected_score;reroute_gain;current_links\n");
    for row in rows {
        text.push_str(&format!(
            "{};{};{};{:.6};{:.6};{}\n",
            row.0, row.1, row.2, row.3, row.4, row.5
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

fn resolve_scenario_for_iteration(config_path: &Path, iteration: Option<u32>) -> Result<matsim_core::Scenario, CliError> {
    let scenario = load_scenario(config_path)?;
    if let Some(iteration) = iteration {
        let mut scenario_for_run = scenario.clone();
        scenario_for_run.config.last_iteration = iteration.min(scenario_for_run.config.last_iteration);
        let (_, final_state) = run_iterations_with_state(&scenario_for_run);
        Ok(final_state)
    } else {
        Ok(scenario)
    }
}
