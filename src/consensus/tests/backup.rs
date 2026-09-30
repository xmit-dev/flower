//! Continuous backups and point-in-time restores, through real replicas.
use super::*;
use crate::consensus::{BackupConfig, RestoreOptions, RestorePoint, Snapshot};
use serde_json::Value;

fn config(directory: &TempDir) -> BackupConfig {
    let mut config = BackupConfig::directory(directory.path());
    config.interval = Duration::from_millis(40);
    config.tip_interval = Duration::from_millis(100);
    config
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Wait until the node's backup has shipped everything it applied, with a
/// base written and none in progress.
async fn shipped(node: &Node) -> Value {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let applied = node.raft().metrics().last_applied.map(|id| id.index);
        let status = node.raft().backup_status().unwrap();
        if status["role"] == "leader"
            && status["applied"].as_u64() == applied
            && status["lagEntries"] == 0
            && !status["newestBase"].is_null()
            && status["baseInProgress"].is_null()
            && status["failing"] == false
        {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "backup did not catch up: {status:#}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn generations(directory: &TempDir) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory.path().join("generations"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

async fn restore(
    config: &BackupConfig,
    data: &std::path::Path,
    id: u64,
    advertise: &str,
    point: RestorePoint,
) -> anyhow::Result<Value> {
    crate::consensus::restore_backup(RestoreOptions {
        config: config.clone(),
        data: data.to_owned(),
        id,
        advertise: advertise.into(),
        replica: None,
        point,
        generation: None,
        progress: false,
    })
    .await
}

/// The state a restore to `point` leaves, as a store opened on it serves
/// (from its database, which stays open until this is dropped).
struct Restored {
    state: Snapshot,
    summary: Value,
    _store: Store,
    _data: TempDir,
}

async fn restored(config: &BackupConfig, point: RestorePoint) -> Restored {
    let data = tempfile::tempdir().unwrap();
    let summary = restore(config, data.path(), 7, "127.0.0.1:1", point)
        .await
        .unwrap();
    let store = Store::open(7, data.path().into()).await.unwrap();
    Restored {
        state: store.snapshot().await,
        summary,
        _store: store,
        _data: data,
    }
}

/// Record a point between rounds: a time, the last index before it, and
/// the state then.
async fn record(node: &Node, points: &mut Vec<(u64, u64, Snapshot)>) {
    shipped(node).await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    let time = now_ms();
    let index = node.raft().metrics().last_applied.unwrap().index;
    points.push((time, index, node.raft().local_snapshot().await));
    tokio::time::sleep(Duration::from_millis(15)).await;
}

async fn write(node: &Node, round: u64, count: u64, revision: &mut u64) {
    for n in 0..count {
        node.raft()
            .commit(commit(
                &format!("round-{round}-{n}"),
                *revision,
                round * 10_000 + n,
            ))
            .await
            .unwrap();
        *revision += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backups_restore_the_state_at_any_point_in_time_or_index_and_survive_an_outage() {
    let backups = tempfile::tempdir().unwrap();
    let mut config = config(&backups);
    // A base every hundred or so commits, so restores start from several.
    config.base_after_bytes = 30_000;
    let mut node = Node::backed_up(1, config.clone()).await;
    node.raft()
        .initialize(BTreeMap::from([(node.id, node.address.clone())]))
        .await
        .unwrap();
    leader(std::slice::from_ref(&node)).await;
    let mut revision = 0;
    // (a time between rounds, the last index before it, the state then)
    let mut points: Vec<(u64, u64, Snapshot)> = Vec::new();
    record(&node, &mut points).await;
    for round in 1..=3 {
        write(&node, round, 120, &mut revision).await;
        record(&node, &mut points).await;
    }
    let generation = generations(&backups);
    assert_eq!(generation.len(), 1);

    // The backup store stops taking segments while Raft keeps compacting
    // its log: the store holds what was purged, and shipping resumes where
    // it stopped once the store is back.
    use std::os::unix::fs::PermissionsExt;
    let log = backups
        .path()
        .join("generations")
        .join(&generation[0])
        .join("log");
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o555)).unwrap();
    let writable = std::fs::File::create(log.join("probe")).is_ok();
    let _ = std::fs::remove_file(log.join("probe"));
    let mut round = 4;
    if writable {
        eprintln!("the backup directory stays writable (running as root?): no outage");
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        write(&node, round, 100, &mut revision).await;
        round += 1;
        let status = node.raft().backup_status().unwrap();
        if writable
            || (status["failing"] == true && status["hold"]["entries"].as_u64().unwrap() > 0)
        {
            break;
        }
        assert!(Instant::now() < deadline, "no purge was held: {status:#}");
    }
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o755)).unwrap();
    record(&node, &mut points).await;
    let status = node.raft().backup_status().unwrap();
    assert!(
        writable || status["errors"].as_u64().unwrap() > 0,
        "{status:#}"
    );
    assert_eq!(
        generations(&backups),
        generation,
        "the outage broke the generation"
    );

    for extra in 0..2 {
        write(&node, round + extra, 60, &mut revision).await;
        record(&node, &mut points).await;
    }
    let status = shipped(&node).await;
    assert!(status["basesWritten"].as_u64().unwrap() >= 3, "{status:#}");
    let bases = std::fs::read_dir(
        backups
            .path()
            .join("generations")
            .join(&generation[0])
            .join("bases"),
    )
    .unwrap()
    .count();
    assert!(bases >= 3);

    // Every recorded point restores exactly, by time and by index.
    for (time, index, expected) in &points {
        let at = restored(&config, RestorePoint::Time(*time)).await;
        assert_eq!(at.state, *expected, "restore at {time}: {:#}", at.summary);
        assert_eq!(at.summary["restored"]["index"], *index);
        let through = restored(&config, RestorePoint::Index(*index)).await;
        assert_eq!(through.state, *expected, "restore through index {index}");
    }
    let latest = restored(&config, RestorePoint::Latest).await;
    assert_eq!(latest.state, node.raft().local_snapshot().await);
    let final_index = latest.summary["restored"]["index"].as_u64().unwrap();
    assert_eq!(
        Some(final_index),
        node.raft().metrics().last_applied.map(|id| id.index)
    );
    // Points outside the backups are refused.
    let data = tempfile::tempdir().unwrap();
    let before = restore(
        &config,
        data.path(),
        7,
        "127.0.0.1:1",
        RestorePoint::Time(1_000),
    )
    .await
    .unwrap_err();
    assert!(
        before.to_string().contains("no base at or before"),
        "{before:#}"
    );
    let data = tempfile::tempdir().unwrap();
    let beyond = restore(
        &config,
        data.path(),
        7,
        "127.0.0.1:1",
        RestorePoint::Index(final_index + 100),
    )
    .await
    .unwrap_err();
    assert!(beyond.to_string().contains("through index"), "{beyond:#}");
    // A restore needs an empty directory.
    let data = tempfile::tempdir().unwrap();
    restore(&config, data.path(), 7, "127.0.0.1:1", RestorePoint::Latest)
        .await
        .unwrap();
    assert!(
        restore(&config, data.path(), 7, "127.0.0.1:1", RestorePoint::Latest)
            .await
            .unwrap_err()
            .to_string()
            .contains("already holds a database")
    );
    let listed = crate::consensus::list_backups(&config, None).await.unwrap();
    assert_eq!(listed["generations"].as_array().unwrap().len(), 1);
    assert_eq!(listed["generations"][0]["reason"], "new");
    assert_eq!(
        listed["generations"][0]["restorable"]["to"]["index"].as_u64(),
        Some(final_index)
    );

    // The restored state starts a new cluster of its own, which serves,
    // takes writes and backs up as a new generation saying where it came
    // from.
    node.stop().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let directory = tempfile::tempdir().unwrap();
    restore(&config, directory.path(), 9, &address, RestorePoint::Latest)
        .await
        .unwrap();
    let mut restored_node = Node {
        id: 9,
        address,
        directory,
        host: None,
        backup: Some(config.clone()),
        consensus: None,
        server: None,
        stop_server: None,
    };
    restored_node.start_with_listener(listener).await;
    leader(std::slice::from_ref(&restored_node)).await;
    assert_eq!(restored_node.raft().local_snapshot().await, latest.state);
    let term = restored_node.raft().metrics().current_term;
    assert!(term > 1 << 20, "term {term}");
    write(&restored_node, 99, 3, &mut revision).await;
    shipped(&restored_node).await;
    let now = generations(&backups);
    assert_eq!(now.len(), 2);
    let record: Value = serde_json::from_slice(
        &std::fs::read(
            backups
                .path()
                .join("generations")
                .join(&now[1])
                .join("generation.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(record["reason"], "restored");
    assert_eq!(record["previous"], generation[0].as_str());
    assert_eq!(record["restoredFrom"]["generation"], generation[0].as_str());
    assert_eq!(record["restoredFrom"]["index"].as_u64(), Some(final_index));
    // A restart continues its generation rather than saying so again.
    restored_node.stop().await;
    restored_node.restart().await;
    leader(std::slice::from_ref(&restored_node)).await;
    write(&restored_node, 100, 2, &mut revision).await;
    let status = shipped(&restored_node).await;
    assert_eq!(status["generation"], now[1].as_str());
    assert_eq!(generations(&backups), now);
    // The newest generation restores the new cluster's writes; the old one,
    // named, its own history.
    let newest = restored(&config, RestorePoint::Latest).await;
    assert_eq!(newest.state, restored_node.raft().local_snapshot().await);
    let data = tempfile::tempdir().unwrap();
    let old = crate::consensus::restore_backup(RestoreOptions {
        config: config.clone(),
        data: data.path().to_owned(),
        id: 7,
        advertise: "127.0.0.1:1".into(),
        replica: None,
        point: RestorePoint::Latest,
        generation: Some(generation[0].clone()),
        progress: false,
    })
    .await
    .unwrap();
    assert_eq!(old["restored"]["index"].as_u64(), Some(final_index));
    restored_node.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_and_a_restarted_cluster_continue_the_generation() {
    let backups = tempfile::tempdir().unwrap();
    let config = config(&backups);
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(Node::backed_up(id, config.clone()).await);
    }
    nodes[0]
        .raft()
        .initialize(
            nodes
                .iter()
                .map(|node| (node.id, node.address.clone()))
                .collect(),
        )
        .await
        .unwrap();
    let mut revision = 0;
    let first = leader(&nodes).await;
    write(&nodes[first], 1, 40, &mut revision).await;
    let status = shipped(&nodes[first]).await;
    let generation = status["generation"].as_str().unwrap().to_owned();
    for (index, node) in nodes.iter().enumerate() {
        if index != first {
            assert_eq!(node.raft().backup_status().unwrap()["role"], "follower");
        }
    }
    // The leader stops; another continues from where it left off.
    nodes[first].stop().await;
    let second = leader(&nodes).await;
    assert_ne!(first, second);
    write(&nodes[second], 2, 40, &mut revision).await;
    let status = shipped(&nodes[second]).await;
    assert_eq!(status["generation"], generation.as_str());
    assert_eq!(generations(&backups), vec![generation.clone()]);
    // Every node restarts; the new leader continues again.
    nodes[first].restart().await;
    for node in &mut nodes {
        node.stop().await;
    }
    for node in &mut nodes {
        node.restart().await;
    }
    let third = leader(&nodes).await;
    write(&nodes[third], 3, 20, &mut revision).await;
    let status = shipped(&nodes[third]).await;
    assert_eq!(status["generation"], generation.as_str());
    assert_eq!(generations(&backups), vec![generation]);
    let latest = restored(&config, RestorePoint::Latest).await;
    assert_eq!(latest.state, nodes[third].raft().local_snapshot().await);
    for node in &mut nodes {
        node.stop().await;
    }
}
