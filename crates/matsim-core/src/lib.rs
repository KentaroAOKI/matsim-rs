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
    pub performing_utils_per_hour: f64,
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
    let mut total_score = 0.0_f64;

    for person in &scenario.population.persons {
        let mut person_distance_m = 0.0_f64;
        let mut last_activity: Option<&Activity> = None;

        for element in &person.selected_plan.elements {
            match element {
                PlanElement::Activity(activity) => {
                    total_score += score_activity(activity, scenario.config.performing_utils_per_hour);
                    last_activity = Some(activity);
                }
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

fn score_activity(activity: &Activity, performing_utils_per_hour: f64) -> f64 {
    let duration_seconds = activity.duration_seconds.or(activity.end_time_seconds).unwrap_or(0.0);
    (duration_seconds / 3600.0) * performing_utils_per_hour
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
