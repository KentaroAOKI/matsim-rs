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

#[derive(Debug, Clone)]
pub struct ReplanningConfig {
    pub strategies: Vec<StrategySetting>,
    pub max_agent_plan_memory_size: Option<usize>,
}

impl Default for ReplanningConfig {
    fn default() -> Self {
        Self {
            strategies: Vec::new(),
            max_agent_plan_memory_size: Some(5),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StrategySetting {
    pub name: String,
    pub weight: f64,
    pub disable_after_fraction: Option<f64>,
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
    pub plan_memory_stats: PlanMemoryStats,
    pub observed_link_costs: Vec<LinkCostStat>,
    pub observed_link_profiles: Vec<LinkProfileStat>,
    pub events: Vec<EventRecord>,
    pub replanning_summary: ReplanningSummary,
}

#[derive(Debug, Clone, Default)]
pub struct ReplanningSummary {
    pub strategies_considered: usize,
    pub persons_replanned: usize,
    pub plan_delta: isize,
    pub strategy_stats: Vec<StrategyStat>,
    pub reroute_details: Vec<RerouteStat>,
}

#[derive(Debug, Clone, Default)]
pub struct StrategyStat {
    pub strategy_name: String,
    pub sampled: usize,
    pub applied: usize,
}

#[derive(Debug, Clone, Default)]
pub struct RerouteStat {
    pub person_id: String,
    pub previous_links: String,
    pub rerouted_links: String,
    pub previous_score: f64,
    pub estimated_rerouted_score: f64,
    pub score_components: Vec<RerouteScoreComponentStat>,
    pub leg_stats: Vec<RerouteLegStat>,
}

#[derive(Debug, Clone, Default)]
pub struct RerouteScoreComponentStat {
    pub component: String,
    pub label: String,
    pub current_score: f64,
    pub rerouted_score: f64,
    pub delta: f64,
}

#[derive(Debug, Clone, Default)]
pub struct RerouteLegStat {
    pub leg_index: usize,
    pub mode: String,
    pub departure_time_seconds: f64,
    pub current_cost_seconds: f64,
    pub rerouted_cost_seconds: f64,
    pub current_arrival_time_seconds: f64,
    pub rerouted_arrival_time_seconds: f64,
    pub current_links: String,
    pub rerouted_links: String,
}

#[derive(Debug, Clone)]
struct SimulationState {
    person_stats: Vec<PersonScoreStats>,
}

#[derive(Debug, Clone, Default)]
struct SimulationSnapshot {
    leg_times: Vec<Vec<f64>>,
    observed_link_costs: BTreeMap<String, f64>,
    observed_link_time_profiles: BTreeMap<String, BTreeMap<u32, f64>>,
    events: Vec<EventRecord>,
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
pub struct PlanMemoryStats {
    pub avg_plans_per_person: f64,
    pub max_plans_per_person: usize,
    pub selected_plan_share: f64,
}

#[derive(Debug, Clone)]
pub struct LinkCostStat {
    pub link_id: String,
    pub travel_time_seconds: f64,
}

#[derive(Debug, Clone)]
pub struct LinkProfileStat {
    pub link_id: String,
    pub hour_bucket: u32,
    pub travel_time_seconds: f64,
    pub delay_seconds: f64,
}

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub time_seconds: f64,
    pub person_id: String,
    pub event_type: String,
    pub link_id: Option<String>,
    pub leg_index: usize,
}

#[derive(Debug, Clone)]
pub struct EventAnalysis {
    pub iteration: u32,
    pub avg_leg_travel_time_seconds: f64,
    pub avg_activity_duration_seconds: f64,
    pub departures: usize,
    pub arrivals: usize,
    pub link_enters: usize,
    pub link_leaves: usize,
    pub activity_starts: usize,
    pub activity_ends: usize,
}

#[derive(Debug, Clone)]
pub struct LinkEventAnalysis {
    pub iteration: u32,
    pub link_id: String,
    pub avg_travel_time_seconds: f64,
    pub traversals: usize,
}

#[derive(Debug, Clone)]
pub struct PersonScoreBreakdown {
    pub person_id: String,
    pub total_score: f64,
    pub items: Vec<ScoreBreakdownItem>,
}

#[derive(Debug, Clone)]
pub struct PersonRerouteScoreBreakdown {
    pub person_id: String,
    pub current_total_score: f64,
    pub rerouted_total_score: f64,
    pub items: Vec<RerouteScoreItem>,
}

#[derive(Debug, Clone)]
pub struct PersonRerouteExplanation {
    pub person_id: String,
    pub legs: Vec<RerouteLegExplanation>,
}

#[derive(Debug, Clone)]
pub struct PersonPlansExplanation {
    pub person_id: String,
    pub selected_plan_index: usize,
    pub plans: Vec<PlanExplanation>,
}

#[derive(Debug, Clone)]
pub struct PlanExplanation {
    pub index: usize,
    pub score: Option<f64>,
    pub selected: bool,
    pub leg_count: usize,
    pub activity_count: usize,
}

#[derive(Debug, Clone)]
pub struct RerouteLegExplanation {
    pub leg_index: usize,
    pub mode: String,
    pub current_link_ids: Vec<String>,
    pub current_cost_seconds: f64,
    pub rerouted_node_ids: Vec<String>,
    pub rerouted_link_ids: Vec<String>,
    pub rerouted_cost_seconds: f64,
}

#[derive(Debug, Clone)]
pub struct ScoreBreakdownItem {
    pub label: String,
    pub start_time_seconds: f64,
    pub end_time_seconds: f64,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct RerouteScoreItem {
    pub label: String,
    pub current_score: f64,
    pub rerouted_score: f64,
    pub delta: f64,
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
    run_iterations_with_state(scenario).0
}

pub fn run_iterations_with_state(scenario: &Scenario) -> (RunOutput, Scenario) {
    let mut scenario = scenario.clone();
    let mut state = SimulationState::new(scenario.population.persons.len());
    let iterations = (0..=scenario.config.last_iteration)
        .map(|iteration| run_iteration(&mut scenario, &mut state, iteration))
        .collect();

    (
        RunOutput {
            last_iteration: scenario.config.last_iteration,
            iterations,
        },
        scenario,
    )
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
    let simulation = simulate_traffic(&scenario.population, &scenario.network);
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
        .zip(simulation.leg_times.iter())
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
    let total_plans = scenario
        .population
        .persons
        .iter()
        .map(|person| person.plans.len())
        .sum::<usize>();
    let max_plans_per_person = scenario
        .population
        .persons
        .iter()
        .map(|person| person.plans.len())
        .max()
        .unwrap_or(0);
    let plan_memory_stats = PlanMemoryStats {
        avg_plans_per_person: if person_count > 0.0 {
            total_plans as f64 / person_count
        } else {
            0.0
        },
        max_plans_per_person,
        selected_plan_share: if total_plans > 0 {
            person_count_usize as f64 / total_plans as f64
        } else {
            0.0
        },
    };
    let replanning_summary =
        apply_replanning_hook(
            scenario,
            &executed_scores,
            &simulation.leg_times,
            &simulation.observed_link_costs,
            &simulation.observed_link_time_profiles,
            iteration,
        );
    let observed_link_costs = simulation
        .observed_link_costs
        .iter()
        .map(|(link_id, travel_time_seconds)| LinkCostStat {
            link_id: link_id.clone(),
            travel_time_seconds: *travel_time_seconds,
        })
        .collect();
    let observed_link_profiles = simulation
        .observed_link_time_profiles
        .iter()
        .flat_map(|(link_id, profile)| {
            let free_speed_time_seconds = scenario
                .network
                .links
                .get(link_id)
                .map(|link| link.length_m / link.freespeed_mps)
                .unwrap_or(0.0);
            profile.iter().map(move |(hour_bucket, travel_time_seconds)| LinkProfileStat {
                link_id: link_id.clone(),
                hour_bucket: *hour_bucket,
                travel_time_seconds: *travel_time_seconds,
                delay_seconds: (*travel_time_seconds - free_speed_time_seconds).max(0.0),
            })
        })
        .collect();

    IterationOutput {
        iteration,
        mode_stats,
        travel_distance_stats,
        score_stats,
        plan_memory_stats,
        observed_link_costs,
        observed_link_profiles,
        events: simulation.events,
        replanning_summary,
    }
}

fn apply_replanning_hook(
    scenario: &mut Scenario,
    executed_scores: &[f64],
    leg_times: &[Vec<f64>],
    observed_link_costs: &BTreeMap<String, f64>,
    observed_link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
    iteration: u32,
) -> ReplanningSummary {
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
            plan_delta: 0,
            strategy_stats: scenario
                .config
                .replanning
                .strategies
                .iter()
                .map(|strategy| StrategyStat {
                    strategy_name: strategy.name.clone(),
                    sampled: 0,
                    applied: 0,
                })
                .collect(),
            reroute_details: Vec::new(),
        };
    }

    let initial_plan_count = scenario
        .population
        .persons
        .iter()
        .map(|person| person.plans.len())
        .sum::<usize>();
    let mut persons_replanned = 0usize;
    let mut reroute_details = Vec::new();
    let mut strategy_stats = scenario
        .config
        .replanning
        .strategies
        .iter()
        .map(|strategy| StrategyStat {
            strategy_name: strategy.name.clone(),
            sampled: 0,
            applied: 0,
        })
        .collect::<Vec<_>>();
    for ((person, executed_score), person_leg_times) in scenario
        .population
        .persons
        .iter_mut()
        .zip(executed_scores.iter().copied())
        .zip(leg_times.iter())
    {
        let Some(strategy_name) = select_strategy(
            &scenario.config.replanning.strategies,
            scenario.config.random_seed,
            iteration,
            scenario.config.last_iteration,
            &person.id,
        ) else {
            continue;
        };
        if let Some(stat) = strategy_stats
            .iter_mut()
            .find(|stat| stat.strategy_name == strategy_name)
        {
            stat.sampled += 1;
        }

        let replanned = match strategy_name {
            "BestScore" => apply_best_score_strategy(person),
            "ReRoute" => {
                let detail = reroute_selected_plan_with_stats(
                    person,
                    &scenario.network,
                    &scenario.config.scoring,
                    person_leg_times,
                    observed_link_costs,
                    observed_link_time_profiles,
                    executed_score,
                );
                if let Some(detail) = detail {
                    reroute_details.push(detail);
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
        if replanned {
            persons_replanned += 1;
            if let Some(stat) = strategy_stats
                .iter_mut()
                .find(|stat| stat.strategy_name == strategy_name)
            {
                stat.applied += 1;
            }
        }
        prune_plans(person, scenario.config.replanning.max_agent_plan_memory_size);
    }
    let final_plan_count = scenario
        .population
        .persons
        .iter()
        .map(|person| person.plans.len())
        .sum::<usize>();

    ReplanningSummary {
        strategies_considered: scenario.config.replanning.strategies.len(),
        persons_replanned,
        plan_delta: final_plan_count as isize - initial_plan_count as isize,
        strategy_stats,
        reroute_details,
    }
}

fn prune_plans(person: &mut Person, max_agent_plan_memory_size: Option<usize>) {
    let Some(max_size) = max_agent_plan_memory_size else {
        return;
    };
    if max_size == 0 || person.plans.len() <= max_size {
        return;
    }

    let selected_index = person.selected_plan_index;
    let mut plan_order = person
        .plans
        .iter()
        .enumerate()
        .map(|(index, plan)| (index, plan.score.unwrap_or(f64::NEG_INFINITY)))
        .collect::<Vec<_>>();
    plan_order.sort_by(|left, right| right.1.total_cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut keep = plan_order
        .into_iter()
        .take(max_size)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if !keep.contains(&selected_index) {
        if let Some((lowest_position, _)) = keep
            .iter()
            .enumerate()
            .map(|(position, index)| (position, person.plans[*index].score.unwrap_or(f64::NEG_INFINITY)))
            .min_by(|left, right| left.1.total_cmp(&right.1))
        {
            keep[lowest_position] = selected_index;
        }
    }
    keep.sort_unstable();

    let new_selected_index = keep
        .iter()
        .position(|index| *index == selected_index)
        .unwrap_or(0);
    let old_plans = std::mem::take(&mut person.plans);
    person.plans = old_plans
        .into_iter()
        .enumerate()
        .filter_map(|(index, plan)| keep.binary_search(&index).ok().map(|_| plan))
        .collect();
    person.selected_plan_index = new_selected_index;
}

fn select_strategy<'a>(
    strategies: &'a [StrategySetting],
    random_seed: u64,
    iteration: u32,
    last_iteration: u32,
    person_id: &str,
) -> Option<&'a str> {
    let active_strategies = strategies
        .iter()
        .filter(|strategy| strategy.weight > 0.0 && strategy_is_active(strategy, iteration, last_iteration))
        .collect::<Vec<_>>();
    let total_weight = active_strategies.iter().map(|strategy| strategy.weight).sum::<f64>();
    if total_weight <= 0.0 {
        return None;
    }

    let draw = replanning_draw(random_seed, iteration, person_id) * total_weight;
    let mut cumulative_weight = 0.0;
    for strategy in active_strategies {
        cumulative_weight += strategy.weight;
        if draw < cumulative_weight {
            return Some(strategy.name.as_str());
        }
    }

    strategies
        .iter()
        .rev()
        .find(|strategy| strategy.weight > 0.0)
        .map(|strategy| strategy.name.as_str())
}

fn strategy_is_active(strategy: &StrategySetting, iteration: u32, last_iteration: u32) -> bool {
    let Some(disable_after_fraction) = strategy.disable_after_fraction else {
        return true;
    };
    if !is_innovation_strategy(strategy.name.as_str()) || last_iteration == 0 {
        return true;
    }
    (iteration as f64) / (last_iteration as f64) < disable_after_fraction
}

fn is_innovation_strategy(strategy_name: &str) -> bool {
    matches!(strategy_name, "ReRoute")
}

fn replanning_draw(random_seed: u64, iteration: u32, person_id: &str) -> f64 {
    let mut hash = random_seed
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(u64::from(iteration) << 32);
    for byte in person_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        hash ^= hash >> 32;
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^= hash >> 31;
    (hash as f64) / ((u64::MAX as f64) + 1.0)
}

fn apply_best_score_strategy(person: &mut Person) -> bool {
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
        return true;
    }
    false
}

fn reroute_selected_plan_with_stats(
    person: &mut Person,
    network: &Network,
    scoring: &ScoringConfig,
    current_leg_travel_times: &[f64],
    link_costs: &BTreeMap<String, f64>,
    link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
    previous_score: f64,
) -> Option<RerouteStat> {
    let mut rerouted_plan = person.selected_plan().clone();
    let mut rerouted = false;
    let original_plan = person.selected_plan().clone();

    for leg_index in 0..rerouted_plan.elements.len() {
        let previous_link_id = previous_activity_at(&rerouted_plan, leg_index)
            .and_then(|activity| activity.link_id.as_deref());
        let next_link_id = next_activity_at(&rerouted_plan, leg_index)
            .and_then(|activity| activity.link_id.as_deref());
        let Some(PlanElement::Leg(existing_leg)) = rerouted_plan.elements.get(leg_index) else {
            continue;
        };
        let departure_time_s = leg_departure_time_seconds(&rerouted_plan, leg_index).unwrap_or(0.0);
        let Some(primary_route_node_ids) = shortest_route_node_ids_for_departure(
            network,
            previous_link_id,
            next_link_id,
            link_costs,
            link_time_profiles,
            departure_time_s,
        ) else {
            continue;
        };
        let current_route_links = route_link_sequence(
            existing_leg,
            previous_activity_at(&rerouted_plan, leg_index),
            next_activity_at(&rerouted_plan, leg_index),
            network,
        );
        let mut penalized_link_costs = link_costs.clone();
        let mut current_route_time_s = departure_time_s;
        for link_id in current_route_links {
            let observed_cost_s =
                link_cost_for_departure(link_id, current_route_time_s, link_costs, link_time_profiles);
            let free_speed_cost_s = network
                .links
                .get(link_id)
                .map(|link| link.length_m / link.freespeed_mps)
                .unwrap_or(observed_cost_s);
            let delay_s = (observed_cost_s - free_speed_cost_s).max(0.0);
            let penalty_factor = if free_speed_cost_s > 0.0 && delay_s > 0.0 {
                1.0 + (delay_s / free_speed_cost_s).min(2.0)
            } else {
                1.02
            };
            penalized_link_costs.insert(link_id.to_string(), observed_cost_s * penalty_factor);
            current_route_time_s += observed_cost_s;
        }
        let alternative_route_node_ids = shortest_route_node_ids_for_departure(
            network,
            previous_link_id,
            next_link_id,
            &penalized_link_costs,
            link_time_profiles,
            departure_time_s,
        )
        .unwrap_or_else(|| primary_route_node_ids.clone());
        let route_node_ids = choose_better_route_candidate(
            network,
            previous_link_id,
            next_link_id,
            link_costs,
            link_time_profiles,
            departure_time_s,
            &primary_route_node_ids,
            &alternative_route_node_ids,
        );
        let PlanElement::Leg(leg) = &mut rerouted_plan.elements[leg_index] else {
            continue;
        };
        if leg.route_node_ids != route_node_ids {
            leg.route_node_ids = route_node_ids;
            rerouted = true;
        }
    }

    if rerouted {
        let current_breakdown =
            score_plan_internal(&original_plan, scoring, network, current_leg_travel_times);
        let rerouted_leg_travel_times =
            estimate_plan_leg_travel_times_from_link_costs(&rerouted_plan, network, link_costs, link_time_profiles);
        let rerouted_breakdown =
            score_plan_internal(&rerouted_plan, scoring, network, &rerouted_leg_travel_times);
        let estimated_rerouted_score = rerouted_breakdown.total_score;
        if has_time_sensitive_activity_constraints(scoring)
            && estimated_rerouted_score <= previous_score + 1.0e-9
        {
            return None;
        }
        let mut leg_counter = 0usize;
        let leg_stats = original_plan
            .elements
            .iter()
            .enumerate()
            .filter_map(|(index, element)| match element {
                PlanElement::Leg(leg) => {
                    let previous_activity = previous_activity_at(&original_plan, index);
                    let next_activity = next_activity_at(&original_plan, index);
                    let current_link_ids = route_link_sequence(leg, previous_activity, next_activity, network)
                        .into_iter()
                        .map(str::to_string)
                        .collect::<Vec<_>>();
                    let rerouted_leg = match rerouted_plan.elements.get(index) {
                        Some(PlanElement::Leg(leg)) => leg,
                        _ => return None,
                    };
                    let rerouted_link_ids =
                        route_link_sequence(rerouted_leg, previous_activity, next_activity, network)
                            .into_iter()
                            .map(str::to_string)
                            .collect::<Vec<_>>();
                    let departure_time_seconds =
                        leg_departure_time_seconds(&original_plan, index).unwrap_or(0.0);
                    let stat = RerouteLegStat {
                        leg_index: leg_counter,
                        mode: leg.mode.clone(),
                        departure_time_seconds,
                        current_cost_seconds: route_cost_from_links(
                            &current_link_ids,
                            departure_time_seconds,
                            link_costs,
                            link_time_profiles,
                        ),
                        rerouted_cost_seconds: route_cost_from_links(
                            &rerouted_link_ids,
                            departure_time_seconds,
                            link_costs,
                            link_time_profiles,
                        ),
                        current_arrival_time_seconds: departure_time_seconds
                            + route_cost_from_links(
                                &current_link_ids,
                                departure_time_seconds,
                                link_costs,
                                link_time_profiles,
                            ),
                        rerouted_arrival_time_seconds: departure_time_seconds
                            + route_cost_from_links(
                                &rerouted_link_ids,
                                departure_time_seconds,
                                link_costs,
                                link_time_profiles,
                            ),
                        current_links: current_link_ids.join(","),
                        rerouted_links: rerouted_link_ids.join(","),
                    };
                    leg_counter += 1;
                    Some(stat)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        rerouted_plan.score = None;
        person.plans.push(rerouted_plan);
        person.selected_plan_index = person.plans.len() - 1;
        let previous_links = original_plan
            .elements
            .iter()
            .enumerate()
            .filter_map(|(index, element)| match element {
                PlanElement::Leg(leg) => {
                    let previous_activity = previous_activity_at(&original_plan, index);
                    let next_activity = next_activity_at(&original_plan, index);
                    Some(
                        route_link_sequence(leg, previous_activity, next_activity, network)
                            .into_iter()
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                    )
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("|");
        let rerouted_links = person
            .selected_plan()
            .elements
            .iter()
            .enumerate()
            .filter_map(|(index, element)| match element {
                PlanElement::Leg(leg) => {
                    let previous_activity = previous_activity_at(person.selected_plan(), index);
                    let next_activity = next_activity_at(person.selected_plan(), index);
                    Some(
                        route_link_sequence(leg, previous_activity, next_activity, network)
                            .into_iter()
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                    )
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("|");
        let score_components = current_breakdown
            .items
            .iter()
            .zip(rerouted_breakdown.items.iter())
            .enumerate()
            .map(|(component_index, (current, rerouted))| RerouteScoreComponentStat {
                component: format!("{:02}:{}", component_index, current.label),
                label: current.label.clone(),
                current_score: current.score,
                rerouted_score: rerouted.score,
                delta: rerouted.score - current.score,
            })
            .collect();
        return Some(RerouteStat {
            person_id: person.id.clone(),
            previous_links,
            rerouted_links,
            previous_score,
            estimated_rerouted_score,
            score_components,
            leg_stats,
        });
    }

    None
}

pub fn write_outputs(output_dir: &Path, output: &RunOutput) -> Result<(), CoreError> {
    fs::create_dir_all(output_dir).map_err(|source| CoreError::CreateOutputDirectory {
        path: output_dir.display().to_string(),
        source,
    })?;

    write_scorestats(&output_dir.join("scorestats.csv"), output)?;
    write_planstats(&output_dir.join("planstats.csv"), output)?;
    write_modestats(&output_dir.join("modestats.csv"), output)?;
    write_traveldistancestats(&output_dir.join("traveldistancestats.csv"), output)?;
    write_observed_link_costs(&output_dir.join("observed_link_costs.csv"), output)?;
    write_observed_link_profiles(&output_dir.join("observed_link_profiles.csv"), output)?;
    write_events(&output_dir.join("events.csv"), output)?;
    write_eventstats(&output_dir.join("eventstats.csv"), output)?;
    write_link_eventstats(&output_dir.join("link_eventstats.csv"), output)?;
    write_replanningstats(&output_dir.join("replanningstats.csv"), output)?;
    write_reroutestats(&output_dir.join("reroutestats.csv"), output)?;
    write_reroute_scorestats(&output_dir.join("reroute_scorestats.csv"), output)?;
    write_reroute_componentstats(&output_dir.join("reroute_componentstats.csv"), output)?;
    write_reroute_legstats(&output_dir.join("reroute_legstats.csv"), output)?;
    Ok(())
}

pub fn explain_person_score(scenario: &Scenario, person_id: &str) -> Option<PersonScoreBreakdown> {
    let person = scenario.population.persons.iter().find(|person| person.id == person_id)?;
    let simulation = simulate_traffic(&scenario.population, &scenario.network);
    let person_index = scenario
        .population
        .persons
        .iter()
        .position(|candidate| candidate.id == person_id)?;
    Some(score_plan_breakdown(
        person,
        &scenario.config.scoring,
        &scenario.network,
        &simulation.leg_times[person_index],
    ))
}

pub fn explain_person_reroute_score(
    scenario: &Scenario,
    person_id: &str,
) -> Option<PersonRerouteScoreBreakdown> {
    let person = scenario.population.persons.iter().find(|person| person.id == person_id)?;
    let simulation = simulate_traffic(&scenario.population, &scenario.network);
    let person_index = scenario
        .population
        .persons
        .iter()
        .position(|candidate| candidate.id == person_id)?;
    let current_breakdown = score_plan_internal(
        person.selected_plan(),
        &scenario.config.scoring,
        &scenario.network,
        &simulation.leg_times[person_index],
    );

    let mut rerouted_plan = person.selected_plan().clone();
    for leg_index in 0..rerouted_plan.elements.len() {
        let previous_link_id = previous_activity_at(&rerouted_plan, leg_index)
            .and_then(|activity| activity.link_id.as_deref());
        let next_link_id = next_activity_at(&rerouted_plan, leg_index)
            .and_then(|activity| activity.link_id.as_deref());
        let Some(PlanElement::Leg(_)) = rerouted_plan.elements.get(leg_index) else {
            continue;
        };
        let departure_time_s = leg_departure_time_seconds(&rerouted_plan, leg_index).unwrap_or(0.0);
        let route_node_ids = shortest_route_node_ids_for_departure(
            &scenario.network,
            previous_link_id,
            next_link_id,
            &simulation.observed_link_costs,
            &simulation.observed_link_time_profiles,
            departure_time_s,
        )?;
        let PlanElement::Leg(leg) = &mut rerouted_plan.elements[leg_index] else {
            continue;
        };
        leg.route_node_ids = route_node_ids;
    }

    let rerouted_leg_travel_times = estimate_plan_leg_travel_times_from_link_costs(
        &rerouted_plan,
        &scenario.network,
        &simulation.observed_link_costs,
        &simulation.observed_link_time_profiles,
    );
    let rerouted_breakdown = score_plan_internal(
        &rerouted_plan,
        &scenario.config.scoring,
        &scenario.network,
        &rerouted_leg_travel_times,
    );

    let items = current_breakdown
        .items
        .iter()
        .zip(rerouted_breakdown.items.iter())
        .map(|(current, rerouted)| RerouteScoreItem {
            label: current.label.clone(),
            current_score: current.score,
            rerouted_score: rerouted.score,
            delta: rerouted.score - current.score,
        })
        .collect();

    Some(PersonRerouteScoreBreakdown {
        person_id: person.id.clone(),
        current_total_score: current_breakdown.total_score,
        rerouted_total_score: rerouted_breakdown.total_score,
        items,
    })
}

pub fn analyze_events(output: &RunOutput) -> Vec<EventAnalysis> {
    let grouped = output
        .iterations
        .iter()
        .map(|iteration| (iteration.iteration, iteration.events.clone()))
        .collect::<Vec<_>>();
    analyze_event_groups(&grouped)
}

pub fn analyze_event_groups(grouped_events: &[(u32, Vec<EventRecord>)]) -> Vec<EventAnalysis> {
    grouped_events
        .iter()
        .map(|(iteration, events)| analyze_event_records(*iteration, events))
        .collect()
}

pub fn analyze_link_event_groups(grouped_events: &[(u32, Vec<EventRecord>)]) -> Vec<LinkEventAnalysis> {
    grouped_events
        .iter()
        .flat_map(|(iteration, events)| analyze_link_event_records(*iteration, events))
        .collect()
}

fn analyze_event_records(iteration: u32, events: &[EventRecord]) -> EventAnalysis {
    let mut departures = BTreeMap::<(&str, usize), f64>::new();
    let mut activity_starts = BTreeMap::<(&str, usize), f64>::new();
    let mut leg_travel_times = Vec::<f64>::new();
    let mut activity_durations = Vec::<f64>::new();
    let mut departure_count = 0usize;
    let mut arrival_count = 0usize;
    let mut link_enter_count = 0usize;
    let mut link_leave_count = 0usize;
    let mut activity_start_count = 0usize;
    let mut activity_end_count = 0usize;

    for event in events {
        match event.event_type.as_str() {
            "departure" => {
                departures.insert((event.person_id.as_str(), event.leg_index), event.time_seconds);
                departure_count += 1;
            }
            "arrival" => {
                if let Some(departure_time) = departures.remove(&(event.person_id.as_str(), event.leg_index)) {
                    leg_travel_times.push((event.time_seconds - departure_time).max(0.0));
                }
                arrival_count += 1;
            }
            "link_enter" => link_enter_count += 1,
            "link_leave" => link_leave_count += 1,
            event_type if event_type.starts_with("act_start:") => {
                activity_starts.insert((event.person_id.as_str(), event.leg_index), event.time_seconds);
                activity_start_count += 1;
            }
            event_type if event_type.starts_with("act_end:") => {
                if let Some(start_time) = activity_starts.remove(&(event.person_id.as_str(), event.leg_index)) {
                    activity_durations.push((event.time_seconds - start_time).max(0.0));
                }
                activity_end_count += 1;
            }
            _ => {}
        }
    }

    EventAnalysis {
        iteration,
        avg_leg_travel_time_seconds: if leg_travel_times.is_empty() {
            0.0
        } else {
            leg_travel_times.iter().sum::<f64>() / leg_travel_times.len() as f64
        },
        avg_activity_duration_seconds: if activity_durations.is_empty() {
            0.0
        } else {
            activity_durations.iter().sum::<f64>() / activity_durations.len() as f64
        },
        departures: departure_count,
        arrivals: arrival_count,
        link_enters: link_enter_count,
        link_leaves: link_leave_count,
        activity_starts: activity_start_count,
        activity_ends: activity_end_count,
    }
}

fn analyze_link_event_records(iteration: u32, events: &[EventRecord]) -> Vec<LinkEventAnalysis> {
    let mut open_enters = BTreeMap::<(String, usize, String), Vec<f64>>::new();
    let mut link_sums = BTreeMap::<String, f64>::new();
    let mut link_counts = BTreeMap::<String, usize>::new();

    for event in events {
        match event.event_type.as_str() {
            "link_enter" => {
                if let Some(link_id) = &event.link_id {
                    open_enters
                        .entry((event.person_id.clone(), event.leg_index, link_id.clone()))
                        .or_default()
                        .push(event.time_seconds);
                }
            }
            "link_leave" => {
                if let Some(link_id) = &event.link_id {
                    let key = (event.person_id.clone(), event.leg_index, link_id.clone());
                    if let Some(starts) = open_enters.get_mut(&key) {
                        if !starts.is_empty() {
                            let start_time = starts.remove(0);
                            *link_sums.entry(link_id.clone()).or_default() +=
                                (event.time_seconds - start_time).max(0.0);
                            *link_counts.entry(link_id.clone()).or_default() += 1;
                        }
                        if starts.is_empty() {
                            open_enters.remove(&key);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    link_sums
        .into_iter()
        .map(|(link_id, sum)| LinkEventAnalysis {
            iteration,
            avg_travel_time_seconds: sum / (*link_counts.get(&link_id).unwrap_or(&1) as f64),
            traversals: *link_counts.get(&link_id).unwrap_or(&0),
            link_id,
        })
        .collect()
}

pub fn explain_person_reroute(scenario: &Scenario, person_id: &str) -> Option<PersonRerouteExplanation> {
    let person = scenario.population.persons.iter().find(|person| person.id == person_id)?;
    let simulation = simulate_traffic(&scenario.population, &scenario.network);
    let plan = person.selected_plan();
    let mut legs = Vec::new();

    for (element_index, element) in plan.elements.iter().enumerate() {
        let PlanElement::Leg(leg) = element else {
            continue;
        };
        let previous_activity = previous_activity_at(plan, element_index);
        let next_activity = next_activity_at(plan, element_index);
        let current_link_ids = route_link_sequence(leg, previous_activity, next_activity, &scenario.network)
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let current_cost_seconds = current_link_ids
            .iter()
            .map(|link_id| {
                simulation
                    .observed_link_costs
                    .get(link_id)
                    .copied()
                    .or_else(|| {
                        scenario
                            .network
                            .links
                            .get(link_id)
                            .map(|link| link.length_m / link.freespeed_mps)
                    })
                    .unwrap_or(0.0)
            })
            .sum::<f64>();
        let rerouted_node_ids = shortest_route_node_ids_for_departure(
            &scenario.network,
            previous_activity.and_then(|activity| activity.link_id.as_deref()),
            next_activity.and_then(|activity| activity.link_id.as_deref()),
            &simulation.observed_link_costs,
            &simulation.observed_link_time_profiles,
            leg_departure_time_seconds(plan, element_index).unwrap_or(0.0),
        )
        .unwrap_or_default();
        let rerouted_leg = Leg {
            mode: leg.mode.clone(),
            route_node_ids: rerouted_node_ids.clone(),
        };
        let rerouted_link_ids = route_link_sequence(&rerouted_leg, previous_activity, next_activity, &scenario.network)
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let rerouted_cost_seconds = rerouted_link_ids
            .iter()
            .map(|link_id| {
                simulation
                    .observed_link_costs
                    .get(link_id)
                    .copied()
                    .or_else(|| {
                        scenario
                            .network
                            .links
                            .get(link_id)
                            .map(|link| link.length_m / link.freespeed_mps)
                    })
                    .unwrap_or(0.0)
            })
            .sum::<f64>();
        legs.push(RerouteLegExplanation {
            leg_index: legs.len(),
            mode: leg.mode.clone(),
            current_link_ids,
            current_cost_seconds,
            rerouted_node_ids,
            rerouted_link_ids,
            rerouted_cost_seconds,
        });
    }

    Some(PersonRerouteExplanation {
        person_id: person.id.clone(),
        legs,
    })
}

pub fn explain_person_plans(scenario: &Scenario, person_id: &str) -> Option<PersonPlansExplanation> {
    let person = scenario.population.persons.iter().find(|person| person.id == person_id)?;
    let plans = person
        .plans
        .iter()
        .enumerate()
        .map(|(index, plan)| PlanExplanation {
            index,
            score: plan.score,
            selected: index == person.selected_plan_index,
            leg_count: plan
                .elements
                .iter()
                .filter(|element| matches!(element, PlanElement::Leg(_)))
                .count(),
            activity_count: plan
                .elements
                .iter()
                .filter(|element| matches!(element, PlanElement::Activity(_)))
                .count(),
        })
        .collect();

    Some(PersonPlansExplanation {
        person_id: person.id.clone(),
        selected_plan_index: person.selected_plan_index,
        plans,
    })
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

fn write_planstats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;avg_plans_per_person;max_plans_per_person;selected_plan_share"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        writeln!(
            writer,
            "{};{:.6};{};{:.6}",
            iteration.iteration,
            iteration.plan_memory_stats.avg_plans_per_person,
            iteration.plan_memory_stats.max_plans_per_person,
            iteration.plan_memory_stats.selected_plan_share
        )
        .map_err(|source| write_error(path, source))?;
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

fn write_observed_link_costs(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(writer, "iteration;link_id;travel_time_seconds").map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        for stat in &iteration.observed_link_costs {
            writeln!(
                writer,
                "{};{};{:.6}",
                iteration.iteration, stat.link_id, stat.travel_time_seconds
            )
            .map_err(|source| write_error(path, source))?;
        }
    }
    Ok(())
}

fn write_observed_link_profiles(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(writer, "iteration;link_id;hour_bucket;travel_time_seconds;delay_seconds")
        .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        for stat in &iteration.observed_link_profiles {
            writeln!(
                writer,
                "{};{};{};{:.6};{:.6}",
                iteration.iteration, stat.link_id, stat.hour_bucket, stat.travel_time_seconds, stat.delay_seconds
            )
            .map_err(|source| write_error(path, source))?;
        }
    }
    Ok(())
}

fn write_events(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(writer, "iteration;time_seconds;person_id;event_type;link_id;leg_index")
        .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        for event in &iteration.events {
            writeln!(
                writer,
                "{};{:.6};{};{};{};{}",
                iteration.iteration,
                event.time_seconds,
                event.person_id,
                event.event_type,
                event.link_id.as_deref().unwrap_or(""),
                event.leg_index
            )
            .map_err(|source| write_error(path, source))?;
        }
    }
    Ok(())
}

fn write_eventstats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;avg_leg_travel_time_seconds;avg_activity_duration_seconds;departures;arrivals;link_enters;link_leaves;activity_starts;activity_ends"
    )
    .map_err(|source| write_error(path, source))?;
    for analysis in analyze_events(output) {
        writeln!(
            writer,
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
        )
        .map_err(|source| write_error(path, source))?;
    }
    Ok(())
}

fn write_link_eventstats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(writer, "iteration;link_id;avg_travel_time_seconds;traversals")
        .map_err(|source| write_error(path, source))?;
    let grouped = output
        .iterations
        .iter()
        .map(|iteration| (iteration.iteration, iteration.events.clone()))
        .collect::<Vec<_>>();
    for analysis in analyze_link_event_groups(&grouped) {
        writeln!(
            writer,
            "{};{};{:.6};{}",
            analysis.iteration, analysis.link_id, analysis.avg_travel_time_seconds, analysis.traversals
        )
        .map_err(|source| write_error(path, source))?;
    }
    Ok(())
}

fn write_replanningstats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;strategies_considered;persons_replanned;plan_delta;strategy_name;sampled;applied"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        for stat in &iteration.replanning_summary.strategy_stats {
            writeln!(
                writer,
                "{};{};{};{};{};{};{}",
                iteration.iteration,
                iteration.replanning_summary.strategies_considered,
                iteration.replanning_summary.persons_replanned,
                iteration.replanning_summary.plan_delta,
                stat.strategy_name,
                stat.sampled,
                stat.applied
            )
            .map_err(|source| write_error(path, source))?;
        }
    }
    Ok(())
}

fn write_reroutestats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;person_id;previous_links;rerouted_links;previous_score;estimated_rerouted_score;estimated_score_delta"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        for detail in &iteration.replanning_summary.reroute_details {
            writeln!(
                writer,
                "{};{};{};{};{:.6};{:.6};{:.6}",
                iteration.iteration,
                detail.person_id,
                detail.previous_links,
                detail.rerouted_links,
                detail.previous_score,
                detail.estimated_rerouted_score,
                detail.estimated_rerouted_score - detail.previous_score
            )
            .map_err(|source| write_error(path, source))?;
        }
    }
    Ok(())
}

fn write_reroute_scorestats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;person_id;component;label;current_score;rerouted_score;score_delta"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        for detail in &iteration.replanning_summary.reroute_details {
            for component in &detail.score_components {
                writeln!(
                    writer,
                    "{};{};{};{};{:.6};{:.6};{:.6}",
                    iteration.iteration,
                    detail.person_id,
                    component.component,
                    component.label,
                    component.current_score,
                    component.rerouted_score,
                    component.delta
                )
                .map_err(|source| write_error(path, source))?;
            }
        }
    }
    Ok(())
}

fn write_reroute_componentstats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;component;label;total_score_delta;avg_score_delta;count;zero_delta_count"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        let mut aggregates = BTreeMap::<(String, String), (f64, usize, usize)>::new();
        for detail in &iteration.replanning_summary.reroute_details {
            for component in &detail.score_components {
                let entry = aggregates
                    .entry((component.component.clone(), component.label.clone()))
                    .or_insert((0.0, 0, 0));
                entry.0 += component.delta;
                entry.1 += 1;
                if component.delta.abs() <= 1.0e-9 {
                    entry.2 += 1;
                }
            }
        }

        for ((component, label), (total_score_delta, count, zero_delta_count)) in aggregates {
            writeln!(
                writer,
                "{};{};{};{:.6};{:.6};{};{}",
                iteration.iteration,
                component,
                label,
                total_score_delta,
                if count > 0 { total_score_delta / count as f64 } else { 0.0 },
                count,
                zero_delta_count
            )
            .map_err(|source| write_error(path, source))?;
        }
    }
    Ok(())
}

fn write_reroute_legstats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;person_id;leg_index;mode;departure_time_seconds;current_cost_seconds;rerouted_cost_seconds;cost_delta_seconds;current_arrival_time_seconds;rerouted_arrival_time_seconds;current_links;rerouted_links"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in &output.iterations {
        for detail in &iteration.replanning_summary.reroute_details {
            for leg in &detail.leg_stats {
                writeln!(
                    writer,
                    "{};{};{};{};{:.6};{:.6};{:.6};{:.6};{:.6};{:.6};{};{}",
                    iteration.iteration,
                    detail.person_id,
                    leg.leg_index,
                    leg.mode,
                    leg.departure_time_seconds,
                    leg.current_cost_seconds,
                    leg.rerouted_cost_seconds,
                    leg.current_cost_seconds - leg.rerouted_cost_seconds,
                    leg.current_arrival_time_seconds,
                    leg.rerouted_arrival_time_seconds,
                    leg.current_links,
                    leg.rerouted_links
                )
                .map_err(|source| write_error(path, source))?;
            }
        }
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

fn simulate_traffic(population: &Population, network: &Network) -> SimulationSnapshot {
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
    let mut observed_link_sums = BTreeMap::<String, f64>::new();
    let mut observed_link_counts = BTreeMap::<String, usize>::new();
    let mut observed_link_profile_sums = BTreeMap::<String, BTreeMap<u32, f64>>::new();
    let mut observed_link_profile_counts = BTreeMap::<String, BTreeMap<u32, usize>>::new();
    let mut events = Vec::<EventRecord>::new();

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
        let leg_order = leg_order_for_element(&person.selected_plan(), pending_leg.plan_element_index);

        if let Some(activity) = previous_activity {
            events.push(EventRecord {
                time_seconds: pending_leg.departure_time_s,
                person_id: person.id.clone(),
                event_type: format!("act_end:{}", activity.activity_type),
                link_id: activity.link_id.clone(),
                leg_index: leg_order,
            });
        }
        events.push(EventRecord {
            time_seconds: pending_leg.departure_time_s,
            person_id: person.id.clone(),
            event_type: "departure".to_string(),
            link_id: previous_activity.and_then(|activity| activity.link_id.clone()),
            leg_index: leg_order,
        });

        let mut current_time_s = pending_leg.departure_time_s;
        for link_id in route_links {
            let Some(link) = network.links.get(link_id) else {
                continue;
            };
            events.push(EventRecord {
                time_seconds: current_time_s,
                person_id: person.id.clone(),
                event_type: "link_enter".to_string(),
                link_id: Some(link_id.to_string()),
                leg_index: leg_order,
            });
            let free_speed_exit_s = current_time_s + link.length_m / link.freespeed_mps;
            let queue_exit_s = next_link_exit_time_s.get(link_id).copied().unwrap_or(0.0);
            let exit_time_s = free_speed_exit_s.max(queue_exit_s);
            let headway_s = if link.capacity_veh_per_hour.is_finite() && link.capacity_veh_per_hour > 0.0 {
                3600.0 / link.capacity_veh_per_hour
            } else {
                0.0
            };
            let observed_travel_time_s = (exit_time_s - current_time_s).max(0.0);
            *observed_link_sums.entry(link_id.to_string()).or_default() += observed_travel_time_s;
            *observed_link_counts.entry(link_id.to_string()).or_default() += 1;
            let bucket = (current_time_s / 3600.0).floor().max(0.0) as u32;
            *observed_link_profile_sums
                .entry(link_id.to_string())
                .or_default()
                .entry(bucket)
                .or_default() += observed_travel_time_s;
            *observed_link_profile_counts
                .entry(link_id.to_string())
                .or_default()
                .entry(bucket)
                .or_default() += 1;
            next_link_exit_time_s.insert(link_id.to_string(), exit_time_s + headway_s);
            events.push(EventRecord {
                time_seconds: exit_time_s,
                person_id: person.id.clone(),
                event_type: "link_leave".to_string(),
                link_id: Some(link_id.to_string()),
                leg_index: leg_order,
            });
            current_time_s = exit_time_s;
        }

        let travel_time_s = (current_time_s - pending_leg.departure_time_s).max(0.0);
        if let Some(slot) = travel_times[pending_leg.person_index].get_mut(leg_order) {
            *slot = travel_time_s;
        }
        events.push(EventRecord {
            time_seconds: pending_leg.departure_time_s + travel_time_s,
            person_id: person.id.clone(),
            event_type: "arrival".to_string(),
            link_id: next_activity.and_then(|activity| activity.link_id.clone()),
            leg_index: leg_order,
        });
        if let Some(activity) = next_activity {
            events.push(EventRecord {
                time_seconds: pending_leg.departure_time_s + travel_time_s,
                person_id: person.id.clone(),
                event_type: format!("act_start:{}", activity.activity_type),
                link_id: activity.link_id.clone(),
                leg_index: leg_order,
            });
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

    let observed_link_costs = network
        .links
        .iter()
        .map(|(link_id, link)| {
            let observed_cost_s = observed_link_sums
                .get(link_id)
                .zip(observed_link_counts.get(link_id))
                .map(|(sum, count)| *sum / (*count as f64))
                .unwrap_or_else(|| link.length_m / link.freespeed_mps);
            (link_id.clone(), observed_cost_s)
        })
        .collect();
    let observed_link_time_profiles = observed_link_profile_sums
        .into_iter()
        .map(|(link_id, bucket_sums)| {
            let profile = bucket_sums
                .into_iter()
                .map(|(bucket, sum)| {
                    let count = observed_link_profile_counts
                        .get(&link_id)
                        .and_then(|counts| counts.get(&bucket))
                        .copied()
                        .unwrap_or(1);
                    (bucket, sum / count as f64)
                })
                .collect::<BTreeMap<_, _>>();
            (link_id, profile)
        })
        .collect();

    SimulationSnapshot {
        leg_times: travel_times,
        observed_link_costs,
        observed_link_time_profiles,
        events,
    }
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

fn leg_departure_time_seconds(plan: &Plan, leg_index: usize) -> Option<f64> {
    let mut current_time_s = 0.0;
    for (index, element) in plan.elements.iter().enumerate() {
        match element {
            PlanElement::Activity(activity) => current_time_s = activity_departure_time(activity, current_time_s),
            PlanElement::Leg(_) if index == leg_index => return Some(current_time_s),
            PlanElement::Leg(_) => {}
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

fn estimate_plan_leg_travel_times_from_link_costs(
    plan: &Plan,
    network: &Network,
    link_costs: &BTreeMap<String, f64>,
    link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
) -> Vec<f64> {
    plan
        .elements
        .iter()
        .enumerate()
        .filter_map(|(index, element)| match element {
            PlanElement::Leg(leg) => {
                let previous_activity = previous_activity_at(plan, index);
                let next_activity = next_activity_at(plan, index);
                let mut current_time_s = leg_departure_time_seconds(plan, index).unwrap_or(0.0);
                let travel_time = route_link_sequence(leg, previous_activity, next_activity, network)
                    .into_iter()
                    .map(|link_id| {
                        let cost = link_cost_for_departure(link_id, current_time_s, link_costs, link_time_profiles);
                        current_time_s += cost;
                        cost
                    })
                    .sum::<f64>();
                Some(travel_time)
            }
            _ => None,
        })
        .collect::<Vec<_>>()
}

fn choose_better_route_candidate(
    network: &Network,
    previous_link_id: Option<&str>,
    next_link_id: Option<&str>,
    link_costs: &BTreeMap<String, f64>,
    link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
    departure_time_s: f64,
    primary_route_node_ids: &[String],
    alternative_route_node_ids: &[String],
) -> Vec<String> {
    let primary_cost = route_node_ids_cost(
        network,
        previous_link_id,
        next_link_id,
        link_costs,
        link_time_profiles,
        departure_time_s,
        primary_route_node_ids,
    );
    let alternative_cost = route_node_ids_cost(
        network,
        previous_link_id,
        next_link_id,
        link_costs,
        link_time_profiles,
        departure_time_s,
        alternative_route_node_ids,
    );
    if alternative_cost + 1.0e-9 < primary_cost {
        alternative_route_node_ids.to_vec()
    } else {
        primary_route_node_ids.to_vec()
    }
}

fn route_node_ids_cost(
    network: &Network,
    previous_link_id: Option<&str>,
    next_link_id: Option<&str>,
    link_costs: &BTreeMap<String, f64>,
    link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
    departure_time_s: f64,
    route_node_ids: &[String],
) -> f64 {
    let probe_leg = Leg {
        mode: "car".to_string(),
        route_node_ids: route_node_ids.to_vec(),
    };
    let previous_activity = previous_link_id.map(|link_id| Activity {
        activity_type: String::new(),
        link_id: Some(link_id.to_string()),
        end_time_seconds: None,
        duration_seconds: None,
    });
    let next_activity = next_link_id.map(|link_id| Activity {
        activity_type: String::new(),
        link_id: Some(link_id.to_string()),
        end_time_seconds: None,
        duration_seconds: None,
    });
    let mut current_time_s = departure_time_s;
    let mut total_cost_s = 0.0;
    for link_id in route_link_sequence(
        &probe_leg,
        previous_activity.as_ref(),
        next_activity.as_ref(),
        network,
    ) {
        let cost_s = link_cost_for_departure(link_id, current_time_s, link_costs, link_time_profiles);
        total_cost_s += cost_s;
        current_time_s += cost_s;
    }
    total_cost_s
}

fn route_cost_from_links(
    link_ids: &[String],
    departure_time_s: f64,
    link_costs: &BTreeMap<String, f64>,
    link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
) -> f64 {
    let mut current_time_s = departure_time_s;
    let mut total_cost_s = 0.0;
    for link_id in link_ids {
        let cost_s = link_cost_for_departure(link_id, current_time_s, link_costs, link_time_profiles);
        total_cost_s += cost_s;
        current_time_s += cost_s;
    }
    total_cost_s
}

fn has_time_sensitive_activity_constraints(scoring: &ScoringConfig) -> bool {
    scoring.activity_params.values().any(|params| {
        params.opening_time_seconds.is_some()
            || params.closing_time_seconds.is_some()
            || params.latest_start_time_seconds.is_some()
            || params.earliest_end_time_seconds.is_some()
            || params.minimal_duration_seconds.is_some()
    })
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
    if activity_end < departure_time {
        score += marginal_utility_of_waiting_s * (departure_time - activity_end);
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

fn shortest_route_node_ids_for_departure(
    network: &Network,
    previous_link_id: Option<&str>,
    next_link_id: Option<&str>,
    link_costs: &BTreeMap<String, f64>,
    link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
    departure_time_s: f64,
) -> Option<Vec<String>> {
    let previous_link = network.links.get(previous_link_id?)?;
    let next_link = network.links.get(next_link_id?)?;
    let start_node_id = previous_link.to_node_id.as_str();
    let target_node_id = next_link.from_node_id.as_str();

    if start_node_id == target_node_id {
        return Some(Vec::new());
    }

    #[derive(Clone, Eq, PartialEq)]
    struct PendingNode {
        cost_ms: i64,
        node_id: String,
    }

    impl Ord for PendingNode {
        fn cmp(&self, other: &Self) -> Ordering {
            other
                .cost_ms
                .cmp(&self.cost_ms)
                .then_with(|| self.node_id.cmp(&other.node_id))
        }
    }

    impl PartialOrd for PendingNode {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut queue = BinaryHeap::new();
    let mut best_cost_ms = BTreeMap::<String, i64>::new();
    let mut predecessor = BTreeMap::<String, String>::new();

    best_cost_ms.insert(start_node_id.to_string(), 0);
    queue.push(PendingNode {
        cost_ms: 0,
        node_id: start_node_id.to_string(),
    });

    while let Some(current) = queue.pop() {
        let Some(&known_cost_ms) = best_cost_ms.get(&current.node_id) else {
            continue;
        };
        if current.cost_ms != known_cost_ms {
            continue;
        }
        if current.node_id == target_node_id {
            break;
        }

        for link in network.links.values().filter(|link| link.from_node_id == current.node_id) {
            let current_departure_time_s = departure_time_s + (current.cost_ms as f64) / 1000.0;
            let link_cost_ms = to_millis(link_cost_for_departure(
                &link.id,
                current_departure_time_s,
                link_costs,
                link_time_profiles,
            ));
            let next_cost_ms = current.cost_ms + link_cost_ms;
            let should_update = best_cost_ms
                .get(&link.to_node_id)
                .map(|&cost_ms| next_cost_ms < cost_ms)
                .unwrap_or(true);
            if should_update {
                best_cost_ms.insert(link.to_node_id.clone(), next_cost_ms);
                predecessor.insert(link.to_node_id.clone(), current.node_id.clone());
                queue.push(PendingNode {
                    cost_ms: next_cost_ms,
                    node_id: link.to_node_id.clone(),
                });
            }
        }
    }

    if !best_cost_ms.contains_key(target_node_id) {
        return None;
    }

    let mut route_node_ids = Vec::new();
    let mut cursor = target_node_id.to_string();
    while cursor != start_node_id {
        route_node_ids.push(cursor.clone());
        cursor = predecessor.get(&cursor)?.clone();
    }
    route_node_ids.reverse();
    Some(route_node_ids)
}

fn link_cost_for_departure(
    link_id: &str,
    departure_time_s: f64,
    link_costs: &BTreeMap<String, f64>,
    link_time_profiles: &BTreeMap<String, BTreeMap<u32, f64>>,
) -> f64 {
    let bucket = (departure_time_s / 3600.0).floor().max(0.0) as u32;
    link_time_profiles
        .get(link_id)
        .and_then(|profile| {
            profile
                .get(&bucket)
                .copied()
                .or_else(|| profile.range(..=bucket).next_back().map(|(_, value)| *value))
                .or_else(|| profile.range(bucket..).next().map(|(_, value)| *value))
        })
        .or_else(|| link_costs.get(link_id).copied())
        .unwrap_or(0.0)
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
                        disable_after_fraction: None,
                    }],
                    max_agent_plan_memory_size: None,
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

        let summary =
            apply_replanning_hook(&mut scenario, &[1.0], &[Vec::new()], &BTreeMap::new(), &BTreeMap::new(), 0);

        assert_eq!(summary.persons_replanned, 1);
        assert_eq!(scenario.population.persons[0].selected_plan_index, 1);
        assert_eq!(scenario.population.persons[0].plans[0].score, Some(1.0));
    }

    #[test]
    fn activity_scoring_penalizes_waiting_after_closing_time() {
        let mut scoring = ScoringConfig {
            waiting_utils_per_hour: -6.0,
            ..ScoringConfig::default()
        };
        scoring.activity_params.insert(
            "w".to_string(),
            ActivityScoringParameters {
                typical_duration_seconds: 8.0 * 3600.0,
                opening_time_seconds: Some(7.0 * 3600.0),
                closing_time_seconds: Some(18.0 * 3600.0),
                ..ActivityScoringParameters::default()
            },
        );

        let activity = Activity {
            activity_type: "w".to_string(),
            link_id: Some("1".to_string()),
            end_time_seconds: None,
            duration_seconds: None,
        };

        let score = score_activity(&activity, &scoring, 17.0 * 3600.0, Some(20.0 * 3600.0));

        assert!((score + 12.0).abs() < 1.0e-9);
    }

    #[test]
    fn reroute_replaces_selected_plan_route_with_shortest_path() {
        let mut network = Network::default();
        for (id, from_node_id, to_node_id, length) in [
            ("start", "s0", "s1", 10.0),
            ("slow-1", "s1", "slow", 100.0),
            ("slow-2", "slow", "target", 100.0),
            ("fast-1", "s1", "fast", 10.0),
            ("fast-2", "fast", "target", 10.0),
            ("end", "target", "s2", 10.0),
        ] {
            network.links.insert(
                id.to_string(),
                Link {
                    id: id.to_string(),
                    from_node_id: from_node_id.to_string(),
                    to_node_id: to_node_id.to_string(),
                    length_m: length,
                    freespeed_mps: 10.0,
                    capacity_veh_per_hour: 3600.0,
                },
            );
        }

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
                        name: "ReRoute".to_string(),
                        weight: 1.0,
                        disable_after_fraction: None,
                    }],
                    max_agent_plan_memory_size: None,
                },
            },
            network,
            population: Population {
                persons: vec![Person {
                    id: "1".to_string(),
                    plans: vec![Plan {
                        score: None,
                        elements: vec![
                            PlanElement::Activity(Activity {
                                activity_type: "h".to_string(),
                                link_id: Some("start".to_string()),
                                end_time_seconds: Some(0.0),
                                duration_seconds: None,
                            }),
                            PlanElement::Leg(Leg {
                                mode: "car".to_string(),
                                route_node_ids: vec!["slow".to_string(), "target".to_string()],
                            }),
                            PlanElement::Activity(Activity {
                                activity_type: "w".to_string(),
                                link_id: Some("end".to_string()),
                                end_time_seconds: None,
                                duration_seconds: Some(3600.0),
                            }),
                        ],
                    }],
                    selected_plan_index: 0,
                }],
            },
        };

        let simulation = simulate_traffic(&scenario.population, &scenario.network);
        let summary = apply_replanning_hook(
            &mut scenario,
            &[0.0],
            &simulation.leg_times,
            &simulation.observed_link_costs,
            &simulation.observed_link_time_profiles,
            0,
        );
        let person = &scenario.population.persons[0];
        let PlanElement::Leg(leg) = &person.selected_plan().elements[1] else {
            panic!("expected leg element");
        };

        assert_eq!(summary.persons_replanned, 1);
        assert_eq!(person.plans.len(), 2);
        assert_eq!(person.selected_plan_index, 1);
        assert_eq!(person.plans[0].score, Some(0.0));
        assert_eq!(person.plans[1].score, None);
        assert_eq!(leg.route_node_ids, vec!["fast", "target"]);
    }

    #[test]
    fn reroute_uses_observed_link_costs_not_just_free_speed() {
        let mut network = Network::default();
        for (id, from_node_id, to_node_id, length, capacity) in [
            ("start", "s0", "s1", 10.0, 3600.0),
            ("slow-1", "s1", "slow", 100.0, 3600.0),
            ("slow-2", "slow", "target", 100.0, 3600.0),
            ("fast-1", "s1", "fast", 10.0, 1.0),
            ("fast-2", "fast", "target", 10.0, 1.0),
            ("end", "target", "s2", 10.0, 3600.0),
        ] {
            network.links.insert(
                id.to_string(),
                Link {
                    id: id.to_string(),
                    from_node_id: from_node_id.to_string(),
                    to_node_id: to_node_id.to_string(),
                    length_m: length,
                    freespeed_mps: 10.0,
                    capacity_veh_per_hour: capacity,
                },
            );
        }

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
                        name: "ReRoute".to_string(),
                        weight: 1.0,
                        disable_after_fraction: None,
                    }],
                    max_agent_plan_memory_size: None,
                },
            },
            network,
            population: Population {
                persons: vec![
                    Person {
                        id: "1".to_string(),
                        plans: vec![Plan {
                            score: None,
                            elements: vec![
                                PlanElement::Activity(Activity {
                                    activity_type: "h".to_string(),
                                    link_id: Some("start".to_string()),
                                    end_time_seconds: Some(0.0),
                                    duration_seconds: None,
                                }),
                                PlanElement::Leg(Leg {
                                    mode: "car".to_string(),
                                    route_node_ids: vec!["fast".to_string(), "target".to_string()],
                                }),
                                PlanElement::Activity(Activity {
                                    activity_type: "w".to_string(),
                                    link_id: Some("end".to_string()),
                                    end_time_seconds: None,
                                    duration_seconds: Some(3600.0),
                                }),
                            ],
                        }],
                        selected_plan_index: 0,
                    },
                    Person {
                        id: "2".to_string(),
                        plans: vec![Plan {
                            score: None,
                            elements: vec![
                                PlanElement::Activity(Activity {
                                    activity_type: "h".to_string(),
                                    link_id: Some("start".to_string()),
                                    end_time_seconds: Some(0.0),
                                    duration_seconds: None,
                                }),
                                PlanElement::Leg(Leg {
                                    mode: "car".to_string(),
                                    route_node_ids: vec!["fast".to_string(), "target".to_string()],
                                }),
                                PlanElement::Activity(Activity {
                                    activity_type: "w".to_string(),
                                    link_id: Some("end".to_string()),
                                    end_time_seconds: None,
                                    duration_seconds: Some(3600.0),
                                }),
                            ],
                        }],
                        selected_plan_index: 0,
                    },
                ],
            },
        };

        let simulation = simulate_traffic(&scenario.population, &scenario.network);
        let summary = apply_replanning_hook(
            &mut scenario,
            &[0.0, 0.0],
            &simulation.leg_times,
            &simulation.observed_link_costs,
            &simulation.observed_link_time_profiles,
            0,
        );
        let person = &scenario.population.persons[0];
        let PlanElement::Leg(leg) = &person.selected_plan().elements[1] else {
            panic!("expected leg element");
        };

        assert_eq!(summary.persons_replanned, 2);
        assert_eq!(leg.route_node_ids, vec!["slow", "target"]);
    }

    #[test]
    fn zero_weight_best_score_does_not_override_reroute() {
        let mut network = Network::default();
        for (id, from_node_id, to_node_id, length) in [
            ("start", "s0", "s1", 10.0),
            ("slow-1", "s1", "slow", 100.0),
            ("slow-2", "slow", "target", 100.0),
            ("fast-1", "s1", "fast", 10.0),
            ("fast-2", "fast", "target", 10.0),
            ("end", "target", "s2", 10.0),
        ] {
            network.links.insert(
                id.to_string(),
                Link {
                    id: id.to_string(),
                    from_node_id: from_node_id.to_string(),
                    to_node_id: to_node_id.to_string(),
                    length_m: length,
                    freespeed_mps: 10.0,
                    capacity_veh_per_hour: 3600.0,
                },
            );
        }

        let mut scenario = Scenario {
            config: MatsimConfig {
                random_seed: 1,
                network_path: String::new(),
                plans_path: String::new(),
                output_directory: String::new(),
                last_iteration: 1,
                scoring: ScoringConfig::default(),
                replanning: ReplanningConfig {
                    strategies: vec![
                        StrategySetting {
                            name: "BestScore".to_string(),
                            weight: 0.0,
                            disable_after_fraction: None,
                        },
                        StrategySetting {
                            name: "ReRoute".to_string(),
                            weight: 1.0,
                            disable_after_fraction: None,
                        },
                    ],
                    max_agent_plan_memory_size: None,
                },
            },
            network,
            population: Population {
                persons: vec![Person {
                    id: "1".to_string(),
                    plans: vec![
                        Plan {
                            score: Some(100.0),
                            elements: vec![
                                PlanElement::Activity(Activity {
                                    activity_type: "h".to_string(),
                                    link_id: Some("start".to_string()),
                                    end_time_seconds: Some(0.0),
                                    duration_seconds: None,
                                }),
                                PlanElement::Leg(Leg {
                                    mode: "car".to_string(),
                                    route_node_ids: vec!["slow".to_string(), "target".to_string()],
                                }),
                                PlanElement::Activity(Activity {
                                    activity_type: "w".to_string(),
                                    link_id: Some("end".to_string()),
                                    end_time_seconds: None,
                                    duration_seconds: Some(3600.0),
                                }),
                            ],
                        },
                        Plan {
                            score: Some(500.0),
                            elements: Vec::new(),
                        },
                    ],
                    selected_plan_index: 0,
                }],
            },
        };

        let simulation = simulate_traffic(&scenario.population, &scenario.network);
        let summary = apply_replanning_hook(
            &mut scenario,
            &[100.0],
            &simulation.leg_times,
            &simulation.observed_link_costs,
            &simulation.observed_link_time_profiles,
            0,
        );
        let person = &scenario.population.persons[0];

        assert_eq!(summary.persons_replanned, 1);
        assert_eq!(person.selected_plan_index, 2);
        assert_eq!(person.plans.len(), 3);
    }

    #[test]
    fn innovation_strategy_can_be_disabled_after_fraction() {
        let reroute = StrategySetting {
            name: "ReRoute".to_string(),
            weight: 1.0,
            disable_after_fraction: Some(0.5),
        };
        let best_score = StrategySetting {
            name: "BestScore".to_string(),
            weight: 1.0,
            disable_after_fraction: Some(0.5),
        };

        assert!(strategy_is_active(&reroute, 0, 4));
        assert!(strategy_is_active(&reroute, 1, 4));
        assert!(!strategy_is_active(&reroute, 2, 4));
        assert!(strategy_is_active(&best_score, 2, 4));
    }

    #[test]
    fn prune_plans_keeps_selected_plan_within_memory_limit() {
        let mut person = Person {
            id: "1".to_string(),
            plans: vec![
                Plan {
                    score: Some(1.0),
                    elements: Vec::new(),
                },
                Plan {
                    score: Some(10.0),
                    elements: Vec::new(),
                },
                Plan {
                    score: Some(5.0),
                    elements: Vec::new(),
                },
            ],
            selected_plan_index: 2,
        };

        prune_plans(&mut person, Some(2));

        assert_eq!(person.plans.len(), 2);
        assert_eq!(person.selected_plan_index, 1);
        assert_eq!(person.plans[0].score, Some(10.0));
        assert_eq!(person.plans[1].score, Some(5.0));
    }

    #[test]
    fn link_event_analysis_pairs_enter_and_leave_per_link() {
        let grouped = vec![(
            0,
            vec![
                EventRecord {
                    time_seconds: 10.0,
                    person_id: "1".to_string(),
                    event_type: "link_enter".to_string(),
                    link_id: Some("a".to_string()),
                    leg_index: 0,
                },
                EventRecord {
                    time_seconds: 14.0,
                    person_id: "1".to_string(),
                    event_type: "link_leave".to_string(),
                    link_id: Some("a".to_string()),
                    leg_index: 0,
                },
                EventRecord {
                    time_seconds: 20.0,
                    person_id: "2".to_string(),
                    event_type: "link_enter".to_string(),
                    link_id: Some("a".to_string()),
                    leg_index: 0,
                },
                EventRecord {
                    time_seconds: 26.0,
                    person_id: "2".to_string(),
                    event_type: "link_leave".to_string(),
                    link_id: Some("a".to_string()),
                    leg_index: 0,
                },
            ],
        )];

        let analyses = analyze_link_event_groups(&grouped);

        assert_eq!(analyses.len(), 1);
        assert_eq!(analyses[0].iteration, 0);
        assert_eq!(analyses[0].link_id, "a");
        assert_eq!(analyses[0].traversals, 2);
        assert!((analyses[0].avg_travel_time_seconds - 5.0).abs() < 1.0e-9);
    }
}
