use std::borrow::Cow;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use matsim_core::{
    Activity, ActivityScoringParameters, Leg, Link, MatsimConfig, Network, Person, Plan, PlanElement,
    Population, Scenario, ScoringConfig, ModeScoringParameters, ReplanningConfig, StrategySetting,
};
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IoError {
    #[error("failed to open {path}: {source}")]
    OpenFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read xml {path}: {source}")]
    ReadXml {
        path: String,
        #[source]
        source: quick_xml::Error,
    },
    #[error("missing required config value: {0}")]
    MissingConfig(&'static str),
    #[error("invalid utf-8 in {path}: {source}")]
    InvalidUtf8 {
        path: String,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("invalid float `{value}` in {path}")]
    InvalidFloat { path: String, value: String },
}

pub fn load_scenario(config_path: &Path) -> Result<Scenario, IoError> {
    let config = load_config(config_path)?;
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let network_path = resolve_input_path(config_dir, &config.network_path);
    let plans_path = resolve_input_path(config_dir, &config.plans_path);

    Ok(Scenario {
        network: load_network(&network_path)?,
        population: load_population(&plans_path)?,
        config: MatsimConfig {
            network_path: network_path.display().to_string(),
            plans_path: plans_path.display().to_string(),
            ..config
        },
    })
}

pub fn load_config(path: &Path) -> Result<MatsimConfig, IoError> {
    let mut reader = xml_reader(path)?;
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut current_module: Option<String> = None;
    let mut random_seed: Option<u64> = None;
    let mut network_path: Option<String> = None;
    let mut plans_path: Option<String> = None;
    let mut output_directory: Option<String> = None;
    let mut last_iteration = 0_u32;
    let mut scoring = ScoringConfig::default();
    let mut replanning = ReplanningConfig::default();
    let mut current_paramset_type: Option<String> = None;
    let mut current_activity_params = ActivityScoringParameters::default();
    let mut current_activity_type: Option<String> = None;
    let mut current_mode_params = ModeScoringParameters::default();
    let mut current_mode: Option<String> = None;
    let mut current_strategy_name: Option<String> = None;
    let mut current_strategy_weight: Option<f64> = None;

    loop {
        match reader.read_event_into(&mut buf).map_err(|source| IoError::ReadXml {
            path: path.display().to_string(),
            source,
        })? {
            Event::Start(ref e) if e.name().as_ref() == b"module" => {
                current_module = attr_string(path, e, b"name")?;
            }
            Event::Start(ref e) if e.name().as_ref() == b"parameterset" => {
                current_paramset_type = attr_string(path, e, b"type")?;
                if current_paramset_type.as_deref() == Some("activityParams") {
                    current_activity_params = ActivityScoringParameters::default();
                    current_activity_type = None;
                } else if current_paramset_type.as_deref() == Some("modeParams") {
                    current_mode_params = ModeScoringParameters::default();
                    current_mode = None;
                } else if current_paramset_type.as_deref() == Some("strategysettings") {
                    current_strategy_name = None;
                    current_strategy_weight = None;
                }
            }
            Event::End(ref e) if e.name().as_ref() == b"module" => {
                current_module = None;
            }
            Event::End(ref e) if e.name().as_ref() == b"parameterset" => {
                if current_paramset_type.as_deref() == Some("activityParams") {
                    if let Some(activity_type) = current_activity_type.take() {
                        scoring.activity_params.insert(activity_type, current_activity_params.clone());
                    }
                } else if current_paramset_type.as_deref() == Some("modeParams") {
                    if let Some(mode) = current_mode.take() {
                        scoring.mode_params.insert(mode, current_mode_params.clone());
                    }
                } else if current_paramset_type.as_deref() == Some("strategysettings") {
                    if let Some(name) = current_strategy_name.take() {
                        replanning.strategies.push(StrategySetting {
                            name,
                            weight: current_strategy_weight.unwrap_or(0.0),
                        });
                    }
                }
                current_paramset_type = None;
            }
            Event::Empty(ref e) if e.name().as_ref() == b"param" => {
                let name = attr_string(path, e, b"name")?.unwrap_or_default();
                let value = attr_string(path, e, b"value")?.unwrap_or_default();
                match current_module.as_deref() {
                    Some("global") if name == "randomSeed" => {
                        random_seed = value.parse::<u64>().ok();
                    }
                    Some("network") if name == "inputNetworkFile" => network_path = Some(value.clone()),
                    Some("plans") if name == "inputPlansFile" => plans_path = Some(value.clone()),
                    Some("controller") if name == "outputDirectory" => output_directory = Some(value.clone()),
                    Some("controller") if name == "lastIteration" => {
                        last_iteration = value.parse::<u32>().unwrap_or(0);
                    }
                    Some("scoring") if name == "performing" => {
                        scoring.performing_utils_per_hour = parse_scoring_value(path, &value)?;
                    }
                    Some("scoring") if name == "lateArrival" => {
                        scoring.late_arrival_utils_per_hour = parse_scoring_value(path, &value)?;
                    }
                    Some("scoring") if name == "earlyDeparture" => {
                        scoring.early_departure_utils_per_hour = parse_scoring_value(path, &value)?;
                    }
                    Some("scoring") if name == "waiting" => {
                        scoring.waiting_utils_per_hour = parse_scoring_value(path, &value)?;
                    }
                    _ => {}
                }

                if current_module.as_deref() == Some("scoring")
                    && current_paramset_type.as_deref() == Some("activityParams")
                {
                    match name.as_str() {
                        "activityType" => current_activity_type = Some(value.clone()),
                        "typicalDuration" => {
                            current_activity_params.typical_duration_seconds = parse_time(&value)?;
                        }
                        "openingTime" => {
                            current_activity_params.opening_time_seconds = Some(parse_time(&value)?);
                        }
                        "closingTime" => {
                            current_activity_params.closing_time_seconds = Some(parse_time(&value)?);
                        }
                        "latestStartTime" => {
                            current_activity_params.latest_start_time_seconds = Some(parse_time(&value)?);
                        }
                        "earliestEndTime" => {
                            current_activity_params.earliest_end_time_seconds = Some(parse_time(&value)?);
                        }
                        "minimalDuration" => {
                            current_activity_params.minimal_duration_seconds = Some(parse_time(&value)?);
                        }
                        _ => {}
                    }
                }
                if current_module.as_deref() == Some("scoring")
                    && current_paramset_type.as_deref() == Some("modeParams")
                {
                    match name.as_str() {
                        "mode" => current_mode = Some(value.clone()),
                        "marginalUtilityOfTraveling_util_hr" => {
                            current_mode_params.marginal_utility_of_traveling_utils_per_hour =
                                parse_scoring_value(path, &value)?;
                        }
                        "marginalUtilityOfDistance_util_m" => {
                            current_mode_params.marginal_utility_of_distance_utils_per_meter =
                                parse_f64(path, &value)?;
                        }
                        "monetaryDistanceRate" => {
                            current_mode_params.monetary_distance_rate = parse_f64(path, &value)?;
                        }
                        "constant" => {
                            current_mode_params.constant = parse_f64(path, &value)?;
                        }
                        "dailyMonetaryConstant" => {
                            current_mode_params.daily_monetary_constant = parse_f64(path, &value)?;
                        }
                        "dailyUtilityConstant" => {
                            current_mode_params.daily_utility_constant = parse_f64(path, &value)?;
                        }
                        _ => {}
                    }
                }
                if current_module.as_deref() == Some("replanning")
                    && current_paramset_type.as_deref() == Some("strategysettings")
                {
                    match name.as_str() {
                        "strategyName" => current_strategy_name = Some(value.clone()),
                        "weight" => current_strategy_weight = Some(parse_f64(path, &value)?),
                        _ => {}
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(MatsimConfig {
        random_seed: random_seed.unwrap_or(0),
        network_path: network_path.ok_or(IoError::MissingConfig("network.inputNetworkFile"))?,
        plans_path: plans_path.ok_or(IoError::MissingConfig("plans.inputPlansFile"))?,
        output_directory: output_directory.unwrap_or_else(|| "./output-rust".to_string()),
        last_iteration,
        scoring,
        replanning,
    })
}

pub fn load_network(path: &Path) -> Result<Network, IoError> {
    let mut reader = xml_reader(path)?;
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut network = Network::default();

    loop {
        match reader.read_event_into(&mut buf).map_err(|source| IoError::ReadXml {
            path: path.display().to_string(),
            source,
        })? {
            Event::Empty(ref e) if e.name().as_ref() == b"link" => {
                let id = attr_string(path, e, b"id")?.unwrap_or_default();
                let from_node_id = attr_string(path, e, b"from")?.unwrap_or_default();
                let to_node_id = attr_string(path, e, b"to")?.unwrap_or_default();
                let length_m = attr_string(path, e, b"length")?
                    .as_deref()
                    .map(|value| parse_f64(path, value))
                    .transpose()?
                    .unwrap_or(0.0);
                let capacity_veh_per_hour = attr_string(path, e, b"capacity")?
                    .as_deref()
                    .map(|value| parse_f64(path, value))
                    .transpose()?
                    .unwrap_or(f64::INFINITY);
                let freespeed_mps = attr_string(path, e, b"freespeed")?
                    .as_deref()
                    .map(|value| parse_f64(path, value))
                    .transpose()?
                    .unwrap_or(1.0);

                network.links.insert(
                    id.clone(),
                    Link {
                        id,
                        from_node_id,
                        to_node_id,
                        length_m,
                        freespeed_mps,
                        capacity_veh_per_hour,
                    },
                );
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(network)
}

pub fn load_population(path: &Path) -> Result<Population, IoError> {
    let mut reader = xml_reader(path)?;
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut population = Population::default();
    let mut current_person_id: Option<String> = None;
    let mut current_plan: Option<Plan> = None;
    let mut current_plan_selected = false;
    let mut fallback_plan: Option<Plan> = None;
    let mut selected_plan: Option<Plan> = None;
    let mut inside_route = false;
    let mut route_buffer = String::new();

    loop {
        match reader.read_event_into(&mut buf).map_err(|source| IoError::ReadXml {
            path: path.display().to_string(),
            source,
        })? {
            Event::Start(ref e) if e.name().as_ref() == b"person" => {
                current_person_id = attr_string(path, e, b"id")?;
                selected_plan = None;
                fallback_plan = None;
            }
            Event::End(ref e) if e.name().as_ref() == b"person" => {
                if let Some(person_id) = current_person_id.take() {
                    let plan = selected_plan.take().or_else(|| fallback_plan.take()).unwrap_or_default();
                    population.persons.push(Person {
                        id: person_id,
                        selected_plan: plan,
                    });
                }
            }
            Event::Start(ref e) if e.name().as_ref() == b"plan" => {
                current_plan = Some(Plan::default());
                current_plan_selected = attr_string(path, e, b"selected")?
                    .map(|value| value.eq_ignore_ascii_case("yes"))
                    .unwrap_or(false);
            }
            Event::End(ref e) if e.name().as_ref() == b"plan" => {
                if let Some(plan) = current_plan.take() {
                    if current_plan_selected {
                        selected_plan = Some(plan);
                    } else if fallback_plan.is_none() {
                        fallback_plan = Some(plan);
                    }
                }
                current_plan_selected = false;
            }
            Event::Empty(ref e) if e.name().as_ref() == b"act" => {
                if let Some(plan) = current_plan.as_mut() {
                    plan.elements.push(PlanElement::Activity(parse_activity(path, e)?));
                }
            }
            Event::Start(ref e) if e.name().as_ref() == b"leg" => {
                if let Some(plan) = current_plan.as_mut() {
                    plan.elements.push(PlanElement::Leg(Leg {
                        mode: attr_string(path, e, b"mode")?.unwrap_or_else(|| "unknown".to_string()),
                        route_node_ids: Vec::new(),
                    }));
                }
            }
            Event::Start(ref e) if e.name().as_ref() == b"route" => {
                inside_route = true;
                route_buffer.clear();
            }
            Event::Text(text) if inside_route => {
                route_buffer.push_str(&decode_text(path, text.as_ref())?);
            }
            Event::End(ref e) if e.name().as_ref() == b"route" => {
                inside_route = false;
                if let Some(plan) = current_plan.as_mut() {
                    if let Some(PlanElement::Leg(leg)) = plan.elements.last_mut() {
                        leg.route_node_ids = route_buffer
                            .split_whitespace()
                            .map(|id| id.to_string())
                            .collect();
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(population)
}

fn parse_activity(path: &Path, e: &BytesStart<'_>) -> Result<Activity, IoError> {
    Ok(Activity {
        activity_type: attr_string(path, e, b"type")?.unwrap_or_else(|| "unknown".to_string()),
        link_id: attr_string(path, e, b"link")?,
        end_time_seconds: attr_string(path, e, b"end_time")?
            .as_deref()
            .map(parse_time)
            .transpose()?,
        duration_seconds: attr_string(path, e, b"dur")?
            .as_deref()
            .map(parse_time)
            .transpose()?,
    })
}

fn resolve_input_path(base_dir: &Path, value: &str) -> PathBuf {
    let candidate = Path::new(value);
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base_dir.join(candidate)
    }
}

fn xml_reader(path: &Path) -> Result<Reader<BufReader<Box<dyn Read>>>, IoError> {
    let file = File::open(path).map_err(|source| IoError::OpenFile {
        path: path.display().to_string(),
        source,
    })?;
    let boxed: Box<dyn Read> = if path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
        Box::new(GzDecoder::new(file))
    } else {
        Box::new(file)
    };

    Ok(Reader::from_reader(BufReader::new(boxed)))
}

fn attr_string(path: &Path, event: &BytesStart<'_>, key: &[u8]) -> Result<Option<String>, IoError> {
    for attr in event.attributes().flatten() {
        if attr.key.as_ref() == key {
            let value = std::str::from_utf8(attr.value.as_ref()).map_err(|source| IoError::InvalidUtf8 {
                path: path.display().to_string(),
                source,
            })?;
            return Ok(Some(value.to_string()));
        }
    }
    Ok(None)
}

fn decode_text(path: &Path, bytes: &[u8]) -> Result<String, IoError> {
    let text = std::str::from_utf8(bytes).map_err(|source| IoError::InvalidUtf8 {
        path: path.display().to_string(),
        source,
    })?;
    Ok(match quick_xml::escape::unescape(text).map_err(|source| IoError::ReadXml {
        path: path.display().to_string(),
        source: quick_xml::Error::Escape(source),
    })? {
        Cow::Borrowed(value) => value.to_string(),
        Cow::Owned(value) => value,
    })
}

fn parse_scoring_value(path: &Path, value: &str) -> Result<f64, IoError> {
    parse_f64(path, value.trim_start_matches('+'))
}

fn parse_f64(path: &Path, value: &str) -> Result<f64, IoError> {
    value.parse::<f64>().map_err(|_| IoError::InvalidFloat {
        path: path.display().to_string(),
        value: value.to_string(),
    })
}

fn parse_time(value: &str) -> Result<f64, IoError> {
    let parts: Result<Vec<f64>, IoError> = value
        .split(':')
        .map(|part| {
            part.parse::<f64>().map_err(|_| IoError::InvalidFloat {
                path: "<time>".to_string(),
                value: value.to_string(),
            })
        })
        .collect();
    let parts = parts?;

    Ok(match parts.as_slice() {
        [hours, minutes, seconds] => hours * 3600.0 + minutes * 60.0 + seconds,
        [hours, minutes] => hours * 3600.0 + minutes * 60.0,
        [seconds] => *seconds,
        _ => {
            return Err(IoError::InvalidFloat {
                path: "<time>".to_string(),
                value: value.to_string(),
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parses_hh_mm_ss_time() {
        assert_eq!(parse_time("06:00:30").unwrap(), 21_630.0);
        assert_eq!(parse_time("00:10").unwrap(), 600.0);
    }

    #[test]
    fn loads_equil_benchmark_scoring_params() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .join("matsim-example-project/scenarios/equil/config-benchmark.xml");
        let config = load_config(&root).unwrap();
        assert_eq!(config.scoring.performing_utils_per_hour, 6.0);
        assert_eq!(config.scoring.late_arrival_utils_per_hour, -18.0);
        assert_eq!(
            config.scoring.activity_params.get("h").unwrap().typical_duration_seconds,
            43_200.0
        );
        assert_eq!(
            config.scoring.activity_params.get("w").unwrap().closing_time_seconds,
            Some(64_800.0)
        );
        assert_eq!(config.replanning.strategies.len(), 2);
        assert_eq!(config.replanning.strategies[0].name, "BestScore");
        assert_eq!(config.replanning.strategies[1].weight, 0.1);
    }

    #[test]
    fn loads_equil_network_freespeed() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .join("matsim-libs/examples/scenarios/equil/network.xml");
        let network = load_network(&root).unwrap();
        assert_eq!(network.links.get("1").unwrap().freespeed_mps, 27.78);
        assert_eq!(network.links.get("1").unwrap().to_node_id, "2");
    }

    #[test]
    fn loads_equil_v4_route_as_nodes() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .join("matsim-libs/examples/scenarios/equil/plans100.xml");
        let population = load_population(&root).unwrap();
        let first_person = &population.persons[0];
        let first_leg = first_person
            .selected_plan
            .elements
            .iter()
            .find_map(|element| match element {
                PlanElement::Leg(leg) => Some(leg),
                _ => None,
            })
            .unwrap();
        assert_eq!(first_leg.route_node_ids, vec!["2", "7", "12"]);
    }
}
