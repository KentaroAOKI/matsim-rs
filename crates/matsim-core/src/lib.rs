use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct MatsimConfig {
    pub random_seed: u64,
    pub network_path: String,
    pub plans_path: String,
    pub output_directory: String,
    pub last_iteration: u32,
    pub scoring: ScoringConfig,
    pub replanning: ReplanningConfig,
}

#[derive(Debug, Clone, Default)]
pub struct ScoringConfig {
    pub performing_utils_per_hour: f64,
    pub late_arrival_utils_per_hour: f64,
    pub early_departure_utils_per_hour: f64,
    pub waiting_utils_per_hour: f64,
    pub activity_params: BTreeMap<String, ActivityScoringParameters>,
    pub mode_params: BTreeMap<String, ModeScoringParameters>,
}

#[derive(Debug, Clone, Default)]
pub struct ReplanningConfig {
    pub strategies: Vec<StrategySetting>,
}

#[derive(Debug, Clone)]
pub struct StrategySetting {
    pub name: String,
    pub weight: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ActivityScoringParameters {
    pub typical_duration_seconds: f64,
    pub opening_time_seconds: Option<f64>,
    pub closing_time_seconds: Option<f64>,
    pub latest_start_time_seconds: Option<f64>,
    pub earliest_end_time_seconds: Option<f64>,
    pub minimal_duration_seconds: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct ModeScoringParameters {
    pub marginal_utility_of_traveling_utils_per_hour: f64,
    pub marginal_utility_of_distance_utils_per_meter: f64,
    pub monetary_distance_rate: f64,
    pub constant: f64,
    pub daily_monetary_constant: f64,
    pub daily_utility_constant: f64,
}

impl Default for ModeScoringParameters {
    fn default() -> Self {
        Self {
            marginal_utility_of_traveling_utils_per_hour: -6.0,
            marginal_utility_of_distance_utils_per_meter: 0.0,
            monetary_distance_rate: 0.0,
            constant: 0.0,
            daily_monetary_constant: 0.0,
            daily_utility_constant: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scenario {
    pub config: MatsimConfig,
    pub network: Network,
    pub population: Population,
}

#[derive(Debug, Clone, Default)]
pub struct Network {
    pub links: BTreeMap<String, Link>,
}

#[derive(Debug, Clone)]
pub struct Link {
    pub id: String,
    pub from_node_id: String,
    pub to_node_id: String,
    pub length_m: f64,
    pub freespeed_mps: f64,
    pub capacity_veh_per_hour: f64,
}

#[derive(Debug, Clone, Default)]
pub struct Population {
    pub persons: Vec<Person>,
}

#[derive(Debug, Clone)]
pub struct Person {
    pub id: String,
    pub plans: Vec<Plan>,
    pub selected_plan_index: usize,
}

impl Person {
    pub fn selected_plan(&self) -> &Plan {
        &self.plans[self.selected_plan_index]
    }

    pub fn selected_plan_mut(&mut self) -> &mut Plan {
        &mut self.plans[self.selected_plan_index]
    }
}

#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub score: Option<f64>,
    pub elements: Vec<PlanElement>,
}

#[derive(Debug, Clone)]
pub enum PlanElement {
    Activity(Activity),
    Leg(Leg),
}

#[derive(Debug, Clone)]
pub struct Activity {
    pub activity_type: String,
    pub link_id: Option<String>,
    pub end_time_seconds: Option<f64>,
    pub duration_seconds: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct Leg {
    pub mode: String,
    pub route_node_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RunOutput {
    pub last_iteration: u32,
    pub iterations: Vec<IterationOutput>,
}

#[derive(Debug, Clone)]
pub struct IterationOutput {
    pub iteration: u32,
    pub mode_stats: Vec<ModeStat>,
    pub travel_distance_stats: TravelDistanceStats,
    pub score_stats: ScoreStats,
    pub replanning_summary: ReplanningSummary,
}

#[derive(Debug, Clone, Default)]
pub struct ReplanningSummary {
    pub strategies_considered: usize,
    pub persons_replanned: usize,
}

#[derive(Debug, Clone)]
struct SimulationState {
    person_stats: Vec<PersonScoreStats>,
}

#[derive(Debug, Clone, Copy)]
struct PersonScoreStats {
    last_executed: f64,
    best: f64,
    worst: f64,
    sum: f64,
    count: u32,
}

#[derive(Debug, Clone)]
pub struct ModeStat {
    pub mode: String,
    pub share: f64,
}

#[derive(Debug, Clone)]
pub struct TravelDistanceStats {
    pub avg_leg_distance_per_plan_m: f64,
    pub avg_leg_distance_per_person_m: f64,
    pub avg_trip_distance_per_plan_m: f64,
    pub avg_trip_distance_per_person_m: f64,
}

#[derive(Debug, Clone)]
pub struct ScoreStats {
    pub avg_executed: f64,
    pub avg_worst: f64,
    pub avg_average: f64,
    pub avg_best: f64,
}

#[derive(Debug, Clone)]
pub struct PersonScoreBreakdown {
    pub person_id: String,
    pub total_score: f64,
    pub items: Vec<ScoreBreakdownItem>,
}

#[derive(Debug, Clone)]
pub struct ScoreBreakdownItem {
    pub label: String,
    pub start_time_seconds: f64,
    pub end_time_seconds: f64,
    pub score: f64,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("failed to create output directory {path}: {source}")]
    CreateOutputDirectory {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write output file {path}: {source}")]
    WriteOutputFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

use thiserror::Error;

pub fn run_single_iteration(scenario: &Scenario) -> IterationOutput {
    let mut scenario = scenario.clone();
    let mut state = SimulationState::new(scenario.population.persons.len());
    run_iteration(&mut scenario, &mut state, 0)
}

pub fn run_iterations(scenario: &Scenario) -> RunOutput {
    let mut scenario = scenario.clone();
    let mut state = SimulationState::new(scenario.population.persons.len());
    let iterations = (0..=scenario.config.last_iteration)
        .map(|iteration| run_iteration(&mut scenario, &mut state, iteration))
        .collect();

    RunOutput {
        last_iteration: scenario.config.last_iteration,
        iterations,
    }
}

impl SimulationState {
    fn new(person_count: usize) -> Self {
        Self {
            person_stats: vec![
                PersonScoreStats {
                    last_executed: 0.0,
                    best: f64::NEG_INFINITY,
                    worst: f64::INFINITY,
                    sum: 0.0,
                    count: 0,
                };
                person_count
            ],
        }
    }
}

fn run_iteration(scenario: &mut Scenario, state: &mut SimulationState, iteration: u32) -> IterationOutput {
    let simulated_leg_times = simulate_leg_travel_times(&scenario.population, &scenario.network);
    let mut mode_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total_legs = 0usize;
    let mut total_leg_distance_m = 0.0_f64;
    let mut total_plan_distance_m = 0.0_f64;

    for person in &scenario.population.persons {
        let mut person_distance_m = 0.0_f64;
        let mut last_activity: Option<&Activity> = None;

        for element in &person.selected_plan().elements {
            match element {
                PlanElement::Activity(activity) => last_activity = Some(activity),
                PlanElement::Leg(leg) => {
                    total_legs += 1;
                    *mode_counts.entry(leg.mode.clone()).or_default() += 1;

                    let distance_m = leg_distance_m(leg, last_activity, next_activity(&person.selected_plan(), leg), &scenario.network);
                    total_leg_distance_m += distance_m;
                    person_distance_m += distance_m;
                }
            }
        }

        total_plan_distance_m += person_distance_m;
    }

    let person_count_usize = scenario.population.persons.len();
    let person_count = person_count_usize as f64;
    let leg_count = total_legs as f64;
    let executed_scores: Vec<f64> = scenario
        .population
        .persons
        .iter()
        .zip(simulated_leg_times.iter())
        .map(|(person, leg_times)| {
            score_plan(
                &person.selected_plan(),
                &scenario.config.scoring,
                &scenario.network,
                leg_times,
            )
        })
        .collect();

    for (person_stats, executed_score) in state.person_stats.iter_mut().zip(executed_scores.iter().copied()) {
        person_stats.last_executed = executed_score;
        person_stats.best = person_stats.best.max(executed_score);
        person_stats.worst = person_stats.worst.min(executed_score);
        person_stats.sum += executed_score;
        person_stats.count += 1;
    }

    let avg_executed = if person_count > 0.0 {
        state.person_stats.iter().map(|stats| stats.last_executed).sum::<f64>() / person_count
    } else {
        0.0
    };
    let avg_worst = if person_count > 0.0 {
        state.person_stats.iter().map(|stats| stats.worst).sum::<f64>() / person_count
    } else {
        0.0
    };
    let avg_average = if person_count > 0.0 {
        state
            .person_stats
            .iter()
            .map(|stats| stats.sum / stats.count as f64)
            .sum::<f64>()
            / person_count
    } else {
        0.0
    };
    let avg_best = if person_count > 0.0 {
        state.person_stats.iter().map(|stats| stats.best).sum::<f64>() / person_count
    } else {
        0.0
    };

    let mode_stats = mode_counts
        .into_iter()
        .map(|(mode, count)| ModeStat {
            mode,
            share: if leg_count > 0.0 { count as f64 / leg_count } else { 0.0 },
        })
        .collect();

    let travel_distance_stats = TravelDistanceStats {
        avg_leg_distance_per_plan_m: if leg_count > 0.0 { total_leg_distance_m / leg_count } else { 0.0 },
        avg_leg_distance_per_person_m: if person_count > 0.0 { total_plan_distance_m / person_count } else { 0.0 },
        avg_trip_distance_per_plan_m: if leg_count > 0.0 { total_leg_distance_m / leg_count } else { 0.0 },
        avg_trip_distance_per_person_m: if person_count > 0.0 { total_plan_distance_m / person_count } else { 0.0 },
    };

    let score_stats = ScoreStats {
        avg_executed,
        avg_worst,
        avg_average,
        avg_best,
    };
    let replanning_summary = apply_replanning_hook(scenario, &executed_scores, iteration);

    IterationOutput {
        iteration,
        mode_stats,
        travel_distance_stats,
        score_stats,
        replanning_summary,
    }
}

fn apply_replanning_hook(scenario: &mut Scenario, executed_scores: &[f64], iteration: u32) -> ReplanningSummary {
    for (person, executed_score) in scenario
        .population
        .persons
        .iter_mut()
        .zip(executed_scores.iter().copied())
    {
        person.selected_plan_mut().score = Some(executed_score);
    }

    if iteration >= scenario.config.last_iteration {
        return ReplanningSummary {
            strategies_considered: scenario.config.replanning.strategies.len(),
            persons_replanned: 0,
        };
    }

    let mut persons_replanned = 0usize;
    if scenario
        .config
        .replanning
        .strategies
        .iter()
        .any(|strategy| strategy.name == "BestScore")
    {
        for person in &mut scenario.population.persons {
            let current_index = person.selected_plan_index;
            let best_index = person
                .plans
                .iter()
                .enumerate()
                .filter_map(|(index, plan)| plan.score.map(|score| (index, score)))
                .max_by(|left, right| left.1.total_cmp(&right.1))
                .map(|(index, _)| index)
                .unwrap_or(current_index);
            if best_index != current_index {
                person.selected_plan_index = best_index;
                persons_replanned += 1;
            }
        }
    }

    ReplanningSummary {
        strategies_considered: scenario.config.replanning.strategies.len(),
        persons_replanned,
    }
}

pub fn write_outputs(output_dir: &Path, output: &RunOutput) -> Result<(), CoreError> {
    fs::create_dir_all(output_dir).map_err(|source| CoreError::CreateOutputDirectory {
        path: output_dir.display().to_string(),
        source,
    })?;

    write_scorestats(&output_dir.join("scorestats.csv"), output)?;
    write_modestats(&output_dir.join("modestats.csv"), output)?;
    write_traveldistancestats(&output_dir.join("traveldistancestats.csv"), output)?;
    Ok(())
}

pub fn explain_person_score(scenario: &Scenario, person_id: &str) -> Option<PersonScoreBreakdown> {
    let person = scenario.population.persons.iter().find(|person| person.id == person_id)?;
    let simulated_leg_times = simulate_leg_travel_times(&scenario.population, &scenario.network);
    let person_index = scenario
        .population
        .persons
        .iter()
        .position(|candidate| candidate.id == person_id)?;
    Some(score_plan_breakdown(
        person,
        &scenario.config.scoring,
        &scenario.network,
        &simulated_leg_times[person_index],
    ))
}

fn write_scorestats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;avg_executed;avg_worst;avg_average;avg_best"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        writeln!(
            writer,
            "{};{:.6};{:.6};{:.6};{:.6}",
            iteration.iteration,
            iteration.score_stats.avg_executed,
            iteration.score_stats.avg_worst,
            iteration.score_stats.avg_average,
            iteration.score_stats.avg_best
        )
        .map_err(|source| write_error(path, source))?;
    }
    Ok(())
}

fn write_modestats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    write!(writer, "iteration").map_err(|source| write_error(path, source))?;
    let Some(first_iteration) = output.iterations.first() else {
        writeln!(writer).map_err(|source| write_error(path, source))?;
        return Ok(());
    };
    for stat in &first_iteration.mode_stats {
        write!(writer, ";{}", stat.mode).map_err(|source| write_error(path, source))?;
    }
    writeln!(writer).map_err(|source| write_error(path, source))?;

    for iteration in &output.iterations {
        write!(writer, "{}", iteration.iteration).map_err(|source| write_error(path, source))?;
        for stat in &iteration.mode_stats {
            write!(writer, ";{:.1}", stat.share).map_err(|source| write_error(path, source))?;
        }
        writeln!(writer).map_err(|source| write_error(path, source))?;
    }
    Ok(())
}

fn write_traveldistancestats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "ITERATION;avg. Average Leg distance;avg. Average Trip distance"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        writeln!(
            writer,
            "{};{};{}",
            iteration.iteration,
            iteration.travel_distance_stats.avg_leg_distance_per_plan_m,
            iteration.travel_distance_stats.avg_trip_distance_per_plan_m
        )
        .map_err(|source| write_error(path, source))?;
    }
    Ok(())
}

fn csv_writer(path: &Path) -> Result<BufWriter<File>, CoreError> {
    let file = File::create(path).map_err(|source| write_error(path, source))?;
    Ok(BufWriter::new(file))
}

fn write_error(path: &Path, source: std::io::Error) -> CoreError {
    CoreError::WriteOutputFile {
        path: path.display().to_string(),
        source,
    }
}

fn next_activity<'a>(plan: &'a Plan, leg: &Leg) -> Option<&'a Activity> {
    let mut found_leg = false;
    for element in &plan.elements {
        match element {
            PlanElement::Leg(candidate) if std::ptr::eq(candidate, leg) => found_leg = true,
            PlanElement::Activity(activity) if found_leg => return Some(activity),
            _ => {}
        }
    }
    None
}

fn leg_distance_m(leg: &Leg, previous_activity: Option<&Activity>, next_activity: Option<&Activity>, network: &Network) -> f64 {
    let previous_link = previous_activity.and_then(|activity| activity.link_id.as_deref());
    let next_link = next_activity.and_then(|activity| activity.link_id.as_deref());
    if leg.route_node_ids.is_empty() && previous_link.is_some() && previous_link == next_link {
        return 0.0;
    }

    route_link_sequence(leg, previous_activity, next_activity, network)
        .into_iter()
        .filter_map(|link_id| network.links.get(link_id))
        .map(|link| link.length_m)
        .sum()
}

fn simulate_leg_travel_times(population: &Population, network: &Network) -> Vec<Vec<f64>> {
    let mut travel_times = population
        .persons
        .iter()
        .map(|person| {
            let leg_count = person
                .selected_plan()
                .elements
                .iter()
                .filter(|element| matches!(element, PlanElement::Leg(_)))
                .count();
            vec![0.0; leg_count]
        })
        .collect::<Vec<_>>();

    let mut pending = BinaryHeap::new();
    for (person_index, person) in population.persons.iter().enumerate() {
        if let Some((leg_index, departure_time_s)) = first_leg_departure(&person.selected_plan()) {
            pending.push(PendingLeg {
                departure_time_ms: to_millis(departure_time_s),
                departure_time_s,
                person_index,
                person_id: person.id.clone(),
                plan_element_index: leg_index,
            });
        }
    }

    let mut next_link_exit_time_s = BTreeMap::<String, f64>::new();

    while let Some(pending_leg) = pending.pop() {
        let person = &population.persons[pending_leg.person_index];
        let Some(PlanElement::Leg(leg)) = person.selected_plan().elements.get(pending_leg.plan_element_index) else {
            continue;
        };
        let previous_activity = previous_activity_at(&person.selected_plan(), pending_leg.plan_element_index);
        let next_activity = next_activity_at(&person.selected_plan(), pending_leg.plan_element_index);
        let route_links = route_link_sequence(leg, previous_activity, next_activity, network);

        let mut current_time_s = pending_leg.departure_time_s;
        for link_id in route_links {
            let Some(link) = network.links.get(link_id) else {
                continue;
            };
            let free_speed_exit_s = current_time_s + link.length_m / link.freespeed_mps;
            let queue_exit_s = next_link_exit_time_s.get(link_id).copied().unwrap_or(0.0);
            let exit_time_s = free_speed_exit_s.max(queue_exit_s);
            let headway_s = if link.capacity_veh_per_hour.is_finite() && link.capacity_veh_per_hour > 0.0 {
                3600.0 / link.capacity_veh_per_hour
            } else {
                0.0
            };
            next_link_exit_time_s.insert(link_id.to_string(), exit_time_s + headway_s);
            current_time_s = exit_time_s;
        }

        let travel_time_s = (current_time_s - pending_leg.departure_time_s).max(0.0);
        let leg_order = leg_order_for_element(&person.selected_plan(), pending_leg.plan_element_index);
        if let Some(slot) = travel_times[pending_leg.person_index].get_mut(leg_order) {
            *slot = travel_time_s;
        }

        if let Some((next_leg_index, next_departure_s)) = next_leg_departure(
            &person.selected_plan(),
            pending_leg.plan_element_index,
            pending_leg.departure_time_s + travel_time_s,
        ) {
            pending.push(PendingLeg {
                departure_time_ms: to_millis(next_departure_s),
                departure_time_s: next_departure_s,
                person_index: pending_leg.person_index,
                person_id: person.id.clone(),
                plan_element_index: next_leg_index,
            });
        }
    }

    travel_times
}

fn first_leg_departure(plan: &Plan) -> Option<(usize, f64)> {
    let mut current_time_s = 0.0;
    for (index, element) in plan.elements.iter().enumerate() {
        match element {
            PlanElement::Activity(activity) => current_time_s = activity_departure_time(activity, current_time_s),
            PlanElement::Leg(_) => return Some((index, current_time_s)),
        }
    }
    None
}

fn next_leg_departure(plan: &Plan, leg_index: usize, arrival_time_s: f64) -> Option<(usize, f64)> {
    let PlanElement::Activity(activity) = plan.elements.get(leg_index + 1)? else {
        return None;
    };
    let departure_time_s = activity_departure_time(activity, arrival_time_s);
    for index in (leg_index + 2)..plan.elements.len() {
        if matches!(plan.elements[index], PlanElement::Leg(_)) {
            return Some((index, departure_time_s));
        }
    }
    None
}

fn activity_departure_time(activity: &Activity, arrival_time_s: f64) -> f64 {
    if let Some(end_time_s) = activity.end_time_seconds {
        arrival_time_s.max(end_time_s)
    } else if let Some(duration_s) = activity.duration_seconds {
        arrival_time_s + duration_s
    } else {
        arrival_time_s
    }
}

fn previous_activity_at(plan: &Plan, leg_index: usize) -> Option<&Activity> {
    plan.elements[..leg_index].iter().rev().find_map(|element| match element {
        PlanElement::Activity(activity) => Some(activity),
        _ => None,
    })
}

fn next_activity_at(plan: &Plan, leg_index: usize) -> Option<&Activity> {
    plan.elements.iter().skip(leg_index + 1).find_map(|element| match element {
        PlanElement::Activity(activity) => Some(activity),
        _ => None,
    })
}

fn leg_order_for_element(plan: &Plan, leg_index: usize) -> usize {
    plan.elements[..leg_index]
        .iter()
        .filter(|element| matches!(element, PlanElement::Leg(_)))
        .count()
}

fn to_millis(time_s: f64) -> i64 {
    (time_s * 1000.0).round() as i64
}

fn score_plan(plan: &Plan, scoring: &ScoringConfig, network: &Network, leg_travel_times: &[f64]) -> f64 {
    score_plan_internal(plan, scoring, network, leg_travel_times).total_score
}

fn score_plan_breakdown(
    person: &Person,
    scoring: &ScoringConfig,
    network: &Network,
    leg_travel_times: &[f64],
) -> PersonScoreBreakdown {
    let breakdown = score_plan_internal(&person.selected_plan(), scoring, network, leg_travel_times);
    PersonScoreBreakdown {
        person_id: person.id.clone(),
        total_score: breakdown.total_score,
        items: breakdown.items,
    }
}

struct PlanScoreBreakdown {
    total_score: f64,
    items: Vec<ScoreBreakdownItem>,
}

#[derive(Clone, Debug)]
struct PendingLeg {
    departure_time_ms: i64,
    departure_time_s: f64,
    person_index: usize,
    person_id: String,
    plan_element_index: usize,
}

impl Eq for PendingLeg {}

impl PartialEq for PendingLeg {
    fn eq(&self, other: &Self) -> bool {
        self.departure_time_ms == other.departure_time_ms
            && self.person_index == other.person_index
            && self.plan_element_index == other.plan_element_index
    }
}

impl Ord for PendingLeg {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .departure_time_ms
            .cmp(&self.departure_time_ms)
            .then_with(|| self.person_id.cmp(&other.person_id))
            .then_with(|| other.plan_element_index.cmp(&self.plan_element_index))
    }
}

impl PartialOrd for PendingLeg {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn score_plan_internal(
    plan: &Plan,
    scoring: &ScoringConfig,
    network: &Network,
    leg_travel_times: &[f64],
) -> PlanScoreBreakdown {
    let activities: Vec<&Activity> = plan
        .elements
        .iter()
        .filter_map(|element| match element {
            PlanElement::Activity(activity) => Some(activity),
            _ => None,
        })
        .collect();
    if activities.is_empty() {
        return PlanScoreBreakdown {
            total_score: 0.0,
            items: Vec::new(),
        };
    }

    let mut score = 0.0_f64;
    let mut items = Vec::<ScoreBreakdownItem>::new();
    let mut current_time = 0.0_f64;
    let mut activity_windows: Vec<(usize, f64, f64)> = Vec::with_capacity(activities.len());
    let mut seen_modes = BTreeMap::<String, ()>::new();
    let mut last_activity: Option<&Activity> = None;
    let mut leg_index = 0usize;

    for element in &plan.elements {
        match element {
            PlanElement::Activity(activity) => {
                let start = current_time;
                let end = activity_departure_time(activity, start);
                let activity_index = activity_windows.len();
                activity_windows.push((activity_index, start, end));
                current_time = end;
                last_activity = Some(activity);
            }
            PlanElement::Leg(leg) => {
                let travel_time = leg_travel_times.get(leg_index).copied().unwrap_or_else(|| {
                    leg_travel_time_seconds(leg, last_activity, next_activity(plan, leg), network)
                });
                let leg_score = score_leg(leg, scoring, network, travel_time, &mut seen_modes);
                score += leg_score;
                items.push(ScoreBreakdownItem {
                    label: format!("leg:{}", leg.mode),
                    start_time_seconds: current_time,
                    end_time_seconds: current_time + travel_time,
                    score: leg_score,
                });
                current_time += travel_time;
                leg_index += 1;
            }
        }
    }

    if let Some(last) = activities.last() {
        if let Some((last_index, last_start, last_end)) = activity_windows.last_mut() {
            if *last_end == *last_start && last.duration_seconds.is_none() && last.end_time_seconds.is_none() {
                *last_end = 24.0 * 3600.0;
            }
            *last_index = activities.len() - 1;
        }
    }

    if activities.len() >= 2 && activities.first().map(|a| a.activity_type.as_str()) == activities.last().map(|a| a.activity_type.as_str()) {
        let last = activities.last().unwrap();
        let (_, _, first_end) = activity_windows[0];
        let (_, last_start, _) = activity_windows[activity_windows.len() - 1];
        let overnight_score = score_activity(last, scoring, last_start, Some(first_end + 24.0 * 3600.0));
        score += overnight_score;
        items.push(ScoreBreakdownItem {
            label: format!("activity:{}(overnight)", last.activity_type),
            start_time_seconds: last_start,
            end_time_seconds: first_end + 24.0 * 3600.0,
            score: overnight_score,
        });
        for (index, start, end) in activity_windows.iter().copied().skip(1).take(activity_windows.len().saturating_sub(2)) {
            let activity_score = score_activity(activities[index], scoring, start, Some(end));
            score += activity_score;
            items.push(ScoreBreakdownItem {
                label: format!("activity:{}", activities[index].activity_type),
                start_time_seconds: start,
                end_time_seconds: end,
                score: activity_score,
            });
        }
        return PlanScoreBreakdown {
            total_score: score,
            items,
        };
    }

    for (index, start, end) in activity_windows {
        let activity_score = score_activity(activities[index], scoring, start, Some(end));
        score += activity_score;
        items.push(ScoreBreakdownItem {
            label: format!("activity:{}", activities[index].activity_type),
            start_time_seconds: start,
            end_time_seconds: end,
            score: activity_score,
        });
    }
    PlanScoreBreakdown {
        total_score: score,
        items,
    }
}

fn score_activity(
    activity: &Activity,
    scoring: &ScoringConfig,
    arrival_time: f64,
    departure_time: Option<f64>,
) -> f64 {
    let Some(params) = scoring.activity_params.get(&activity.activity_type) else {
        return 0.0;
    };
    let departure_time = departure_time.unwrap_or(arrival_time);

    let mut activity_start = arrival_time;
    let mut activity_end = departure_time;

    if let Some(opening) = params.opening_time_seconds {
        if arrival_time < opening {
            activity_start = opening;
        }
    }
    if let Some(closing) = params.closing_time_seconds {
        if closing < departure_time {
            activity_end = closing;
        }
    }
    if let (Some(opening), Some(closing)) = (params.opening_time_seconds, params.closing_time_seconds) {
        if opening > departure_time || closing < arrival_time {
            activity_start = departure_time;
            activity_end = departure_time;
        }
    }

    let duration = (activity_end - activity_start).max(0.0);
    let mut score = 0.0_f64;
    let marginal_utility_of_performing_s = scoring.performing_utils_per_hour / 3600.0;
    let marginal_utility_of_waiting_s = scoring.waiting_utils_per_hour / 3600.0;
    let marginal_utility_of_late_arrival_s = scoring.late_arrival_utils_per_hour / 3600.0;
    let marginal_utility_of_early_departure_s = scoring.early_departure_utils_per_hour / 3600.0;

    if arrival_time < activity_start {
        score += marginal_utility_of_waiting_s * (activity_start - arrival_time);
    }
    if let Some(latest_start) = params.latest_start_time_seconds {
        if activity_start > latest_start {
            score += marginal_utility_of_late_arrival_s * (activity_start - latest_start);
        }
    }

    if params.typical_duration_seconds > 0.0 {
        let zero_utility_duration_h =
            (params.typical_duration_seconds * (-1.0_f64).exp()) / 3600.0;
        let zero_utility_duration_s = zero_utility_duration_h * 3600.0;
        if duration >= zero_utility_duration_s {
            score += marginal_utility_of_performing_s
                * params.typical_duration_seconds
                * ((duration / 3600.0) / zero_utility_duration_h).ln();
        } else {
            let slope_at_zero =
                marginal_utility_of_performing_s * params.typical_duration_seconds / zero_utility_duration_s;
            score -= slope_at_zero * (zero_utility_duration_s - duration);
        }
    }

    if let Some(earliest_end) = params.earliest_end_time_seconds {
        if activity_end < earliest_end {
            score += marginal_utility_of_early_departure_s * (earliest_end - activity_end);
        }
    }
    if let Some(minimal_duration) = params.minimal_duration_seconds {
        if duration < minimal_duration {
            score += marginal_utility_of_early_departure_s * (minimal_duration - duration);
        }
    }

    score
}

fn score_leg(
    leg: &Leg,
    scoring: &ScoringConfig,
    _network: &Network,
    travel_time_seconds: f64,
    seen_modes: &mut BTreeMap<String, ()>,
) -> f64 {
    let params = scoring.mode_params.get(&leg.mode).cloned().unwrap_or_default();
    let distance_m = 0.0;
    let first_mode_use = seen_modes.insert(leg.mode.clone(), ()).is_none();

    travel_time_seconds * params.marginal_utility_of_traveling_utils_per_hour / 3600.0
        + distance_m * params.marginal_utility_of_distance_utils_per_meter
        + if first_mode_use { params.constant + params.daily_utility_constant } else { 0.0 }
}

fn leg_travel_time_seconds(
    leg: &Leg,
    previous_activity: Option<&Activity>,
    next_activity: Option<&Activity>,
    network: &Network,
) -> f64 {
    let previous_link = previous_activity.and_then(|activity| activity.link_id.as_deref());
    let next_link = next_activity.and_then(|activity| activity.link_id.as_deref());
    if leg.route_node_ids.is_empty() && previous_link.is_some() && previous_link == next_link {
        return 0.0;
    }

    route_link_sequence(leg, previous_activity, next_activity, network)
        .into_iter()
        .filter_map(|link_id| network.links.get(link_id))
        .map(|link| link.length_m / link.freespeed_mps)
        .sum()
}

fn route_link_sequence<'a>(
    leg: &'a Leg,
    previous_activity: Option<&'a Activity>,
    next_activity: Option<&'a Activity>,
    network: &'a Network,
) -> Vec<&'a str> {
    let previous_link_id = previous_activity.and_then(|activity| activity.link_id.as_deref());
    let next_link_id = next_activity.and_then(|activity| activity.link_id.as_deref());

    let Some(previous_link_id) = previous_link_id else {
        return Vec::new();
    };
    let Some(next_link_id) = next_link_id else {
        return Vec::new();
    };

    if leg.route_node_ids.is_empty() && previous_link_id == next_link_id {
        return Vec::new();
    }

    let Some(previous_link) = network.links.get(previous_link_id) else {
        return Vec::new();
    };
    let Some(next_link) = network.links.get(next_link_id) else {
        return Vec::new();
    };

    let mut current_node_id = previous_link.to_node_id.as_str();
    let mut route_nodes = leg.route_node_ids.iter().map(String::as_str).peekable();
    let mut links = Vec::new();

    if route_nodes.peek().copied() == Some(current_node_id) {
        route_nodes.next();
    }

    for node_id in route_nodes {
        let Some(link_id) = find_link_between_nodes(network, current_node_id, node_id) else {
            return links;
        };
        links.push(link_id);
        current_node_id = node_id;
    }

    if current_node_id == next_link.from_node_id {
        links.push(next_link_id);
    }

    links
}

fn find_link_between_nodes<'a>(network: &'a Network, from_node_id: &str, to_node_id: &str) -> Option<&'a str> {
    network
        .links
        .values()
        .find(|link| link.from_node_id == from_node_id && link.to_node_id == to_node_id)
        .map(|link| link.id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leg_distance_uses_route_nodes_and_end_link() {
        let mut network = Network::default();
        for (id, from_node_id, to_node_id, length) in [
            ("1", "n0", "n1", 10.0),
            ("2", "n1", "n2", 20.0),
            ("3", "n2", "n3", 30.0),
        ] {
            network.links.insert(
                id.to_string(),
                Link {
                    id: id.to_string(),
                    from_node_id: from_node_id.to_string(),
                    to_node_id: to_node_id.to_string(),
                    length_m: length,
                    freespeed_mps: 1.0,
                    capacity_veh_per_hour: 3600.0,
                },
            );
        }

        let previous = Activity {
            activity_type: "h".to_string(),
            link_id: Some("1".to_string()),
            end_time_seconds: None,
            duration_seconds: None,
        };
        let next = Activity {
            activity_type: "w".to_string(),
            link_id: Some("3".to_string()),
            end_time_seconds: None,
            duration_seconds: None,
        };
        let leg = Leg {
            mode: "car".to_string(),
            route_node_ids: vec!["n1".to_string(), "n2".to_string()],
        };

        assert_eq!(leg_distance_m(&leg, Some(&previous), Some(&next), &network), 50.0);
    }

    #[test]
    fn empty_route_on_same_link_is_zero_distance() {
        let mut network = Network::default();
        network.links.insert(
            "1".to_string(),
            Link {
                id: "1".to_string(),
                from_node_id: "n0".to_string(),
                to_node_id: "n1".to_string(),
                length_m: 10.0,
                freespeed_mps: 1.0,
                capacity_veh_per_hour: 3600.0,
            },
        );

        let previous = Activity {
            activity_type: "w".to_string(),
            link_id: Some("1".to_string()),
            end_time_seconds: None,
            duration_seconds: None,
        };
        let next = Activity {
            activity_type: "w".to_string(),
            link_id: Some("1".to_string()),
            end_time_seconds: None,
            duration_seconds: None,
        };
        let leg = Leg {
            mode: "car".to_string(),
            route_node_ids: vec![],
        };

        assert_eq!(leg_distance_m(&leg, Some(&previous), Some(&next), &network), 0.0);
    }

    #[test]
    fn best_score_replanning_selects_highest_scored_plan() {
        let mut scenario = Scenario {
            config: MatsimConfig {
                random_seed: 1,
                network_path: String::new(),
                plans_path: String::new(),
                output_directory: String::new(),
                last_iteration: 1,
                scoring: ScoringConfig::default(),
                replanning: ReplanningConfig {
                    strategies: vec![StrategySetting {
                        name: "BestScore".to_string(),
                        weight: 1.0,
                    }],
                },
            },
            network: Network::default(),
            population: Population {
                persons: vec![Person {
                    id: "1".to_string(),
                    plans: vec![
                        Plan {
                            score: Some(1.0),
                            elements: Vec::new(),
                        },
                        Plan {
                            score: Some(5.0),
                            elements: Vec::new(),
                        },
                    ],
                    selected_plan_index: 0,
                }],
            },
        };

        let summary = apply_replanning_hook(&mut scenario, &[1.0], 0);

        assert_eq!(summary.persons_replanned, 1);
        assert_eq!(scenario.population.persons[0].selected_plan_index, 1);
        assert_eq!(scenario.population.persons[0].plans[0].score, Some(1.0));
    }
}
