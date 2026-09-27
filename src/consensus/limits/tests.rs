use super::*;
use crate::consensus::Commit;
use serde_json::json;
use std::collections::BTreeMap;

fn settings(pairs: &[(&str, String)]) -> Result<Limits> {
    Limits::parse(|name| {
        Ok(pairs
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.clone()))
    })
}

#[test]
fn default_and_operator_budgets_share_one_validated_policy() {
    let defaults = settings(&[]).unwrap();
    assert_eq!(defaults.read_timeout, Duration::from_secs(10));
    assert_eq!(defaults.transaction_max_bytes, 32 * 1024 * 1024);
    assert_eq!(defaults.snapshot_purge_batch_logs, 1);
    let limits = settings(&[
        ("FLOWER_READ_TIMEOUT_MS", "17".into()),
        ("FLOWER_COMMIT_TIMEOUT_MS", "19".into()),
        ("FLOWER_APPEND_TIMEOUT_MS", "23".into()),
        ("FLOWER_PEER_CONNECT_TIMEOUT_MS", "29".into()),
        ("FLOWER_PEER_IDLE_TIMEOUT_MS", "31".into()),
        ("FLOWER_SNAPSHOT_TIMEOUT_MS", "37".into()),
        (
            "FLOWER_TRANSACTION_MAX_BYTES",
            (128 * 1024 * 1024).to_string(),
        ),
        ("FLOWER_RPC_MAX_BYTES", (256 * 1024 * 1024).to_string()),
        ("FLOWER_SNAPSHOT_CHUNK_BYTES", (1024 * 1024).to_string()),
        ("FLOWER_SNAPSHOT_AFTER_LOGS", "1024".into()),
        ("FLOWER_SNAPSHOT_LAG_LOGS", "2048".into()),
        ("FLOWER_SNAPSHOT_KEEP_LOGS", "0".into()),
        ("FLOWER_SNAPSHOT_PURGE_BATCH_LOGS", "3".into()),
        ("FLOWER_RAFT_PAYLOAD_ENTRIES", "512".into()),
    ])
    .unwrap();
    assert_eq!(
        [
            limits.read_timeout.as_millis(),
            limits.commit_timeout.as_millis(),
            limits.append_timeout.as_millis(),
            limits.peer_connect_timeout.as_millis(),
            limits.peer_idle_timeout.as_millis(),
            limits.snapshot_timeout.as_millis()
        ],
        [17, 19, 23, 29, 31, 37]
    );
    assert_eq!(limits.transaction_max_bytes, 128 * 1024 * 1024);
    assert_eq!(limits.rpc_max_bytes, 256 * 1024 * 1024);
    assert_eq!(limits.snapshot_chunk_bytes, 1024 * 1024);
    assert_eq!(
        (
            limits.snapshot_after_logs,
            limits.snapshot_lag_logs,
            limits.snapshot_keep_logs,
            limits.snapshot_purge_batch_logs,
            limits.raft_payload_entries
        ),
        (1024, 2048, 0, 3, 512)
    );
}

#[test]
fn invalid_budgets_fail_before_node_startup() {
    for name in [
        "FLOWER_READ_TIMEOUT_MS",
        "FLOWER_COMMIT_TIMEOUT_MS",
        "FLOWER_APPEND_TIMEOUT_MS",
        "FLOWER_PEER_CONNECT_TIMEOUT_MS",
        "FLOWER_PEER_IDLE_TIMEOUT_MS",
        "FLOWER_SNAPSHOT_TIMEOUT_MS",
        "FLOWER_TRANSACTION_MAX_BYTES",
        "FLOWER_RPC_MAX_BYTES",
        "FLOWER_SNAPSHOT_CHUNK_BYTES",
        "FLOWER_SNAPSHOT_AFTER_LOGS",
        "FLOWER_SNAPSHOT_LAG_LOGS",
        "FLOWER_SNAPSHOT_PURGE_BATCH_LOGS",
        "FLOWER_RAFT_PAYLOAD_ENTRIES",
    ] {
        for value in ["0", "-1", "not-a-number", "18446744073709551616"] {
            let error = settings(&[(name, value.into())]).unwrap_err();
            assert!(error.to_string().contains(name), "{name}={value}: {error}");
        }
    }
    let error = settings(&[("FLOWER_RPC_MAX_BYTES", usize::MAX.to_string())]).unwrap_err();
    assert!(error.to_string().contains("platform byte buffer"));
    assert!(settings(&[("FLOWER_SNAPSHOT_LAG_LOGS", "256".into())]).is_err());
    assert!(settings(&[("FLOWER_RAFT_PAYLOAD_ENTRIES", u64::MAX.to_string())]).is_err());
}

#[test]
fn wire_budget_includes_maximum_append_envelope_and_snapshot_segments() {
    let headroom = append_envelope_bytes().unwrap();
    let max_transaction = 2048;
    let pairs = [
        ("FLOWER_TRANSACTION_MAX_BYTES", max_transaction.to_string()),
        (
            "FLOWER_RPC_MAX_BYTES",
            (max_transaction + headroom).to_string(),
        ),
        ("FLOWER_SNAPSHOT_CHUNK_BYTES", "1".into()),
    ];
    let limits = settings(&pairs).unwrap();
    let mut too_small = pairs.clone();
    too_small[1].1 = (max_transaction + headroom - 1).to_string();
    assert!(settings(&too_small)
        .unwrap_err()
        .to_string()
        .contains("Raft envelope"));
    // Segments carry their bytes as they are.
    let mut chunk = pairs.clone();
    chunk[2].1 = max_transaction.to_string();
    assert!(settings(&chunk).is_ok());
    chunk[2].1 = (max_transaction + 1).to_string();
    assert!(settings(&chunk)
        .unwrap_err()
        .to_string()
        .contains("FLOWER_SNAPSHOT_CHUNK_BYTES plus"));

    let mut command = RaftCommand::Single(Commit {
        internal: false,
        request_id: "one".into(),
        fingerprint: "hash".into(),
        expected_revision: u64::MAX,
        puts: BTreeMap::new(),
        deletes: vec![],
        result: json!(""),
    });
    let padding = max_transaction - serde_json::to_vec(&command).unwrap().len();
    if let RaftCommand::Single(commit) = &mut command {
        commit.result = json!("x".repeat(padding));
    }
    assert_eq!(serde_json::to_vec(&command).unwrap().len(), max_transaction);
    let id = LogId::new(CommittedLeaderId::new(u64::MAX, u64::MAX), u64::MAX);
    let request = openraft::raft::AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new(u64::MAX, u64::MAX),
        prev_log_id: Some(id),
        entries: vec![Entry {
            log_id: id,
            payload: EntryPayload::Normal(command),
        }],
        leader_commit: Some(id),
    };
    assert_eq!(
        serde_json::to_vec(&request).unwrap().len(),
        limits.rpc_max_bytes
    );
}

#[test]
fn batching_uses_conservative_bytes_without_an_independent_command_count_limit() {
    let limits = Limits {
        transaction_max_bytes: 1024,
        ..Limits::default()
    };
    let wrapper = b"{\"batch\":[]}".len();
    assert!(!limits.commit_group_fits(0, 0));
    assert!(limits.commit_group_fits(1024 - wrapper, 1));
    assert!(!limits.commit_group_fits(1025 - wrapper, 1));
    assert!(limits.commit_group_fits(1024 - wrapper - 511, 512));
    assert!(!limits.commit_group_fits(1025 - wrapper - 511, 512));
    assert!(!limits.commit_group_fits(usize::MAX, 2));
    assert!(!limits.commit_group_fits(1, usize::MAX));
}

#[test]
fn persisted_log_positions_reject_distance_overflow_without_a_fixed_count_ceiling() {
    let mut limits = Limits::default();
    limits.validate_log_index(None).unwrap();
    limits.validate_log_index(Some(1024)).unwrap();
    limits.snapshot_after_logs = u64::MAX - 1025;
    limits.validate_log_index(Some(1024)).unwrap();
    limits.snapshot_after_logs += 1;
    assert!(limits
        .validate_log_index(Some(1024))
        .unwrap_err()
        .to_string()
        .contains("FLOWER_SNAPSHOT_AFTER_LOGS"));
    limits.snapshot_after_logs = 256;
    limits.snapshot_purge_batch_logs = u64::MAX;
    assert!(limits
        .validate_log_index(Some(0))
        .unwrap_err()
        .to_string()
        .contains("FLOWER_SNAPSHOT_PURGE_BATCH_LOGS"));
    limits.snapshot_purge_batch_logs = 1;
    limits.raft_payload_entries = u64::MAX;
    assert!(limits
        .validate_log_index(Some(0))
        .unwrap_err()
        .to_string()
        .contains("FLOWER_RAFT_PAYLOAD_ENTRIES"));
    assert!(limits
        .validate_log_index(Some(u64::MAX))
        .unwrap_err()
        .to_string()
        .contains("exhausts u64"));
}

#[test]
fn admission_counts_exact_wire_bytes_without_allocating_the_encoding() {
    let controls: String = (0_u8..32).map(char::from).collect();
    let mut nested = json!({"leaf": "🌸\"\\\r\n\t"});
    for _ in 0..100 {
        nested = json!({"array": [nested]});
    }
    for value in [
        json!(null),
        json!(true),
        json!(false),
        json!([]),
        json!({}),
        json!([0, -1, i64::MIN, u64::MAX]),
        json!([0.0, -0.0, 1.0, 1.5, 1e-7, 1e21, f64::MAX, f64::MIN_POSITIVE]),
        json!({controls.as_str(): controls, "Unicode🌸": "é漢字🌸\u{2028}\u{2029}"}),
        json!({"long": "ordinary UTF-8 text without escapes".repeat(4096)}),
        nested,
    ] {
        assert_eq!(
            encoded_json_len(&value).unwrap(),
            serde_json::to_vec(&value).unwrap().len()
        );
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            encoded_json_len(&value).unwrap(),
            serde_json::to_vec(&value).unwrap().len()
        );
    }
    let invalid_keys = BTreeMap::from([(vec![1, 2], 3)]);
    assert!(encoded_json_len(&invalid_keys).is_err());
}

#[test]
fn command_admission_counts_single_fenced_and_compact_serialization_exactly() {
    let command = |index| Commit {
        internal: index % 2 != 0,
        request_id: format!("request-{index}\n🌸"),
        fingerprint: "fingerprint\\\"".into(),
        expected_revision: index,
        puts: BTreeMap::from([("shared\n🌸".into(), json!({"value": index, "float": 1.0}))]),
        deletes: vec!["deleted\r\t".into()],
        result: json!({"ok": true, "result": -0.0}),
    };
    for encoded in [
        RaftCommand::Single(command(0)),
        RaftCommand::Fenced {
            leader_id: CommittedLeaderId::new(u64::MAX, u64::MAX),
            commit: command(0),
        },
        RaftCommand::Batch {
            batch: crate::consensus::CompactBatch::new((0..96).map(command).collect()).unwrap(),
        },
    ] {
        let length = serde_json::to_vec(&encoded).unwrap().len();
        assert_eq!(encoded_json_len(&encoded).unwrap(), length);
        let mut limits = Limits {
            transaction_max_bytes: length,
            ..Limits::default()
        };
        assert!(encoded_json_len(&encoded).unwrap() <= limits.transaction_max_bytes);
        limits.transaction_max_bytes -= 1;
        assert!(encoded_json_len(&encoded).unwrap() > limits.transaction_max_bytes);
    }
}

#[test]
fn admission_counter_rejects_arithmetic_overflow_without_wrapping() {
    let mut counter = JsonByteCount(usize::MAX - 1);
    assert_eq!(counter.write(b"x").unwrap(), 1);
    assert_eq!(counter.0, usize::MAX);
    assert!(counter.write_all(b"x").is_err());
    assert_eq!(counter.0, usize::MAX);
    counter.flush().unwrap();
}

#[test]
fn supplemental_snapshot_policy_is_configurable_without_changing_entry_trigger() {
    let limits = settings(&[
        ("FLOWER_SNAPSHOT_AFTER_BYTES", "0".into()),
        ("FLOWER_SNAPSHOT_MAX_AGE_MS", "0".into()),
        ("FLOWER_SNAPSHOT_CHECK_MS", "17".into()),
        ("FLOWER_SNAPSHOT_DUTY_PERCENT", "100".into()),
    ])
    .unwrap();
    assert_eq!(limits.snapshot_after_logs, 256);
    assert_eq!(limits.snapshot_after_bytes, 0);
    assert_eq!(limits.snapshot_max_age, Duration::ZERO);
    assert_eq!(limits.snapshot_check_interval, Duration::from_millis(17));
    assert_eq!(limits.snapshot_duty_percent, 100);
    for (name, value) in [
        ("FLOWER_SNAPSHOT_AFTER_BYTES", "-1"),
        ("FLOWER_SNAPSHOT_MAX_AGE_MS", "-1"),
        ("FLOWER_SNAPSHOT_CHECK_MS", "0"),
        ("FLOWER_SNAPSHOT_DUTY_PERCENT", "0"),
        ("FLOWER_SNAPSHOT_DUTY_PERCENT", "101"),
    ] {
        assert!(settings(&[(name, value.into())]).is_err(), "{name}={value}");
    }
}

#[test]
fn applied_states_wait_half_a_second_to_be_written_together_by_default() {
    let interval = |value: &str| {
        settings(&[("FLOWER_PERSIST_INTERVAL_MS", value.into())]).map(|limits| limits.persist_interval)
    };
    assert_eq!(settings(&[]).unwrap().persist_interval, Duration::from_millis(500));
    assert_eq!(interval("0").unwrap(), Duration::ZERO);
    assert_eq!(interval("60000").unwrap(), Duration::from_secs(60));
    for invalid in ["60001", "-1", "soon"] {
        assert!(interval(invalid).is_err(), "{invalid}");
    }
}
