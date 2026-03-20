use std::collections::BTreeMap;
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
    pub length_m: f64,
    pub freespeed_mps: f64,
}

#[derive(Debug, Clone, Default)]
pub struct Population {
    pub persons: Vec<Person>,
}

#[derive(Debug, Clone)]
pub struct Person {
    pub id: String,
    pub selected_plan: Plan,
}

#[derive(Debug, Clone, Default)]
pub struct Plan {
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
    pub route_link_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RunOutput {
    pub last_iteration: u32,
    pub mode_stats: Vec<ModeStat>,
    pub travel_distance_stats: TravelDistanceStats,
    pub score_stats: ScoreStats,
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

pub fn run_single_iteration(scenario: &Scenario) -> RunOutput {
    let mut mode_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total_legs = 0usize;
    let mut total_leg_distance_m = 0.0_f64;
    let mut total_plan_distance_m = 0.0_f64;

    for person in &scenario.population.persons {
        let mut person_distance_m = 0.0_f64;
        let mut last_activity: Option<&Activity> = None;

        for element in &person.selected_plan.elements {
            match element {
                PlanElement::Activity(activity) => last_activity = Some(activity),
                PlanElement::Leg(leg) => {
                    total_legs += 1;
                    *mode_counts.entry(leg.mode.clone()).or_default() += 1;

                    let distance_m = leg_distance_m(leg, last_activity, next_activity(&person.selected_plan, leg), &scenario.network);
                    total_leg_distance_m += distance_m;
                    person_distance_m += distance_m;
                }
            }
        }

        total_plan_distance_m += person_distance_m;
    }

    let person_count = scenario.population.persons.len() as f64;
    let leg_count = total_legs as f64;
    let total_score: f64 = scenario
        .population
        .persons
        .iter()
        .map(|person| score_plan(&person.selected_plan, &scenario.config.scoring, &scenario.network))
        .sum();

    let score_avg = if person_count > 0.0 { total_score / person_count } else { 0.0 };

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
        avg_executed: score_avg,
        avg_worst: score_avg,
        avg_average: score_avg,
        avg_best: score_avg,
    };

    RunOutput {
        last_iteration: scenario.config.last_iteration,
        mode_stats,
        travel_distance_stats,
        score_stats,
    }
}

pub fn write_outputs(output_dir: &Path, output: &RunOutput) -> Result<(), CoreError> {
    fs::create_dir_all(output_dir).map_err(|source| CoreError::CreateOutputDirectory {
        path: output_dir.display().to_string(),
        source,
    })?;

    write_scorestats(&output_dir.join("scorestats.csv"), output)?;
    write_modestats(
        &output_dir.join("modestats.csv"),
        &output.mode_stats,
        output.last_iteration,
    )?;
    write_traveldistancestats(
        &output_dir.join("traveldistancestats.csv"),
        &output.travel_distance_stats,
        output.last_iteration,
    )?;
    Ok(())
}

pub fn explain_person_score(scenario: &Scenario, person_id: &str) -> Option<PersonScoreBreakdown> {
    let person = scenario.population.persons.iter().find(|person| person.id == person_id)?;
    Some(score_plan_breakdown(person, &scenario.config.scoring, &scenario.network))
}

fn write_scorestats(path: &Path, output: &RunOutput) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "iteration;avg_executed;avg_worst;avg_average;avg_best"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in 0..=output.last_iteration {
        writeln!(
            writer,
            "{};{:.6};{:.6};{:.6};{:.6}",
            iteration,
            output.score_stats.avg_executed,
            output.score_stats.avg_worst,
            output.score_stats.avg_average,
            output.score_stats.avg_best
        )
        .map_err(|source| write_error(path, source))?;
    }
    Ok(())
}

fn write_modestats(path: &Path, stats: &[ModeStat], last_iteration: u32) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    write!(writer, "iteration").map_err(|source| write_error(path, source))?;
    for stat in stats {
        write!(writer, ";{}", stat.mode).map_err(|source| write_error(path, source))?;
    }
    writeln!(writer).map_err(|source| write_error(path, source))?;

    for iteration in 0..=last_iteration {
        write!(writer, "{iteration}").map_err(|source| write_error(path, source))?;
        for stat in stats {
            write!(writer, ";{:.1}", stat.share).map_err(|source| write_error(path, source))?;
        }
        writeln!(writer).map_err(|source| write_error(path, source))?;
    }
    Ok(())
}

fn write_traveldistancestats(
    path: &Path,
    stats: &TravelDistanceStats,
    last_iteration: u32,
) -> Result<(), CoreError> {
    let mut writer = csv_writer(path)?;
    writeln!(
        writer,
        "ITERATION;avg. Average Leg distance;avg. Average Trip distance"
    )
    .map_err(|source| write_error(path, source))?;
    for iteration in 0..=last_iteration {
        writeln!(
            writer,
            "{};{};{}",
            iteration, stats.avg_leg_distance_per_plan_m, stats.avg_trip_distance_per_plan_m
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
    if leg.route_link_ids.is_empty() && previous_link.is_some() && previous_link == next_link {
        return 0.0;
    }

    let mut distance_m = 0.0_f64;

    if let Some(activity) = previous_activity {
        distance_m += activity
            .link_id
            .as_ref()
            .and_then(|id| network.links.get(id))
            .map(|link| link.length_m)
            .unwrap_or(0.0);
    }

    for link_id in &leg.route_link_ids {
        distance_m += network.links.get(link_id).map(|link| link.length_m).unwrap_or(0.0);
    }

    if let Some(activity) = next_activity {
        distance_m += activity
            .link_id
            .as_ref()
            .and_then(|id| network.links.get(id))
            .map(|link| link.length_m)
            .unwrap_or(0.0);
    }

    distance_m
}

fn score_plan(plan: &Plan, scoring: &ScoringConfig, network: &Network) -> f64 {
    score_plan_internal(plan, scoring, network).total_score
}

fn score_plan_breakdown(person: &Person, scoring: &ScoringConfig, network: &Network) -> PersonScoreBreakdown {
    let breakdown = score_plan_internal(&person.selected_plan, scoring, network);
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

fn score_plan_internal(plan: &Plan, scoring: &ScoringConfig, network: &Network) -> PlanScoreBreakdown {
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

    for element in &plan.elements {
        match element {
            PlanElement::Activity(activity) => {
                let start = current_time;
                let end = if let Some(end_time) = activity.end_time_seconds {
                    end_time
                } else if let Some(duration) = activity.duration_seconds {
                    start + duration
                } else {
                    start
                };
                let activity_index = activity_windows.len();
                activity_windows.push((activity_index, start, end));
                current_time = end;
                last_activity = Some(activity);
            }
            PlanElement::Leg(leg) => {
                let travel_time =
                    leg_travel_time_seconds(leg, last_activity, next_activity(plan, leg), network);
                let leg_score = score_leg(leg, scoring, network, travel_time, &mut seen_modes);
                score += leg_score;
                items.push(ScoreBreakdownItem {
                    label: format!("leg:{}", leg.mode),
                    start_time_seconds: current_time,
                    end_time_seconds: current_time + travel_time,
                    score: leg_score,
                });
                current_time += travel_time;
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
    network: &Network,
    travel_time_seconds: f64,
    seen_modes: &mut BTreeMap<String, ()>,
) -> f64 {
    let params = scoring
        .mode_params
        .get(&leg.mode)
        .cloned()
        .unwrap_or_default();
    let distance_m = leg_distance_m(leg, None, None, network);
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
    if leg.route_link_ids.is_empty() && previous_link.is_some() && previous_link == next_link {
        return 0.0;
    }

    let mut travel_time = 0.0_f64;
    if let Some(activity) = previous_activity {
        if let Some(link_id) = activity.link_id.as_ref() {
            if let Some(link) = network.links.get(link_id) {
                travel_time += link.length_m / link.freespeed_mps;
            }
        }
    }
    for link_id in &leg.route_link_ids {
        if let Some(link) = network.links.get(link_id) {
            travel_time += link.length_m / link.freespeed_mps;
        }
    }
    if let Some(activity) = next_activity {
        if let Some(link_id) = activity.link_id.as_ref() {
            if let Some(link) = network.links.get(link_id) {
                travel_time += link.length_m / link.freespeed_mps;
            }
        }
    }
    travel_time
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leg_distance_uses_route_and_boundary_links() {
        let mut network = Network::default();
        for (id, length) in [("1", 10.0), ("2", 20.0), ("3", 30.0)] {
            network.links.insert(
                id.to_string(),
                Link {
                    id: id.to_string(),
                    length_m: length,
                    freespeed_mps: 1.0,
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
            route_link_ids: vec!["2".to_string()],
        };

        assert_eq!(leg_distance_m(&leg, Some(&previous), Some(&next), &network), 60.0);
    }

    #[test]
    fn empty_route_on_same_link_is_zero_distance() {
        let mut network = Network::default();
        network.links.insert(
            "1".to_string(),
            Link {
                id: "1".to_string(),
                length_m: 10.0,
                freespeed_mps: 1.0,
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
            route_link_ids: vec![],
        };

        assert_eq!(leg_distance_m(&leg, Some(&previous), Some(&next), &network), 0.0);
    }
}
