//! Low-latency Raft defaults, configurable for slower networks or storage.
//!
//! All values are milliseconds. Use the same settings on every cluster member:
//! `FLOWER_RAFT_HEARTBEAT_MS`, `FLOWER_RAFT_ELECTION_MIN_MS`, and
//! `FLOWER_RAFT_ELECTION_MAX_MS`. OpenRaft 0.9's committed-leader failure detector
//! waits election_max (leader lease) + a random election_min..election_max, and
//! checks on ticks spaced at 1.5 * heartbeat. Election values alone therefore
//! understate the expected failover delay.

use anyhow::{ensure, Context, Result};
use openraft::Config;
use std::time::{Duration, Instant};

pub(super) fn configure(config: Config) -> Result<Config> {
    configure_with(config, |name| match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    })
}

fn configure_with(
    mut config: Config,
    read: impl Fn(&str) -> Result<Option<String>>,
) -> Result<Config> {
    for (name, value) in [
        ("FLOWER_RAFT_HEARTBEAT_MS", &mut config.heartbeat_interval),
        (
            "FLOWER_RAFT_ELECTION_MIN_MS",
            &mut config.election_timeout_min,
        ),
        (
            "FLOWER_RAFT_ELECTION_MAX_MS",
            &mut config.election_timeout_max,
        ),
    ] {
        if let Some(raw) = read(name)? {
            *value = raw
                .parse()
                .with_context(|| format!("{name} must be an integer number of milliseconds"))?;
        }
        ensure!(*value > 0, "{name} must be positive");
    }
    // These are actual arithmetic constraints in pinned OpenRaft 0.9.25:
    // raft/mod.rs multiplies heartbeat by 3 before dividing by 2, and
    // engine/engine_config.rs doubles election_max. The latter also bounds
    // leader_lease + election_timeout. There is no operational one-hour cap.
    for (name, value, multiplier) in [
        ("FLOWER_RAFT_HEARTBEAT_MS", config.heartbeat_interval, 3),
        (
            "FLOWER_RAFT_ELECTION_MAX_MS",
            config.election_timeout_max,
            2,
        ),
    ] {
        let derived = value
            .checked_mul(multiplier)
            .with_context(|| format!("{name} overflows OpenRaft's timer arithmetic"))?;
        ensure!(
            Instant::now()
                .checked_add(Duration::from_millis(derived))
                .is_some(),
            "{name} exceeds this platform's deadline range"
        );
    }
    Ok(config.validate()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_low_latency_defaults_include_the_additional_leader_lease() {
        let config = configure_with(Config::default(), |_| Ok(None)).unwrap();
        assert_eq!(config.heartbeat_interval, 50);
        assert_eq!(config.election_timeout_min, 150);
        assert_eq!(config.election_timeout_max, 300);
        assert_eq!(
            config.election_timeout_min + config.election_timeout_max,
            450
        );
        assert_eq!(config.election_timeout_max * 2, 600);
    }

    #[test]
    fn operators_can_restore_slower_cluster_timing_without_changing_code() {
        let config = configure_with(Config::default(), |name| {
            Ok(Some(
                match name {
                    "FLOWER_RAFT_HEARTBEAT_MS" => "200",
                    "FLOWER_RAFT_ELECTION_MIN_MS" => "800",
                    "FLOWER_RAFT_ELECTION_MAX_MS" => "1600",
                    _ => unreachable!(),
                }
                .into(),
            ))
        })
        .unwrap();
        assert_eq!(
            (
                config.heartbeat_interval,
                config.election_timeout_min,
                config.election_timeout_max
            ),
            (200, 800, 1600)
        );
    }

    #[test]
    fn invalid_values_and_inconsistent_timing_fail_configuration() {
        for bad in ["0", "-1", "not-ms", "18446744073709551615"] {
            let error = configure_with(Config::default(), |name| {
                Ok((name == "FLOWER_RAFT_HEARTBEAT_MS").then(|| bad.into()))
            })
            .unwrap_err();
            assert!(
                error.to_string().contains("FLOWER_RAFT_HEARTBEAT_MS"),
                "{error}"
            );
        }
        assert!(configure_with(Config::default(), |name| Ok((name
            == "FLOWER_RAFT_HEARTBEAT_MS")
            .then(|| "150".into())))
        .is_err());
        assert!(configure_with(Config::default(), |name| Ok((name
            == "FLOWER_RAFT_ELECTION_MIN_MS")
            .then(|| "300".into())))
        .is_err());
    }

    #[test]
    fn long_operator_timing_has_only_numeric_and_protocol_constraints() {
        let config = configure_with(Config::default(), |name| {
            Ok(Some(
                match name {
                    "FLOWER_RAFT_HEARTBEAT_MS" => "3600001",
                    "FLOWER_RAFT_ELECTION_MIN_MS" => "7200002",
                    "FLOWER_RAFT_ELECTION_MAX_MS" => "10800003",
                    _ => unreachable!(),
                }
                .into(),
            ))
        })
        .unwrap();
        assert_eq!(config.heartbeat_interval, 3_600_001);
        let error = configure_with(Config::default(), |name| {
            Ok((name == "FLOWER_RAFT_ELECTION_MAX_MS").then(|| u64::MAX.to_string()))
        })
        .unwrap_err();
        assert!(error.to_string().contains("FLOWER_RAFT_ELECTION_MAX_MS"));
    }
}
