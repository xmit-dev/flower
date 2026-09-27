use super::*;
use openraft::{CommittedLeaderId, LogId};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

struct Gate {
    batcher: Batcher,
    started: mpsc::UnboundedReceiver<usize>,
    release: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
}

fn gate(capacity: usize, timeout: Duration) -> Gate {
    let (started_tx, started) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let release_operation = release.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let operation_calls = calls.clone();
    let batcher = Batcher::new(
        move || {
            let release = release_operation.clone();
            let started = started_tx.clone();
            let calls = operation_calls.clone();
            async move {
                // Capture the proof generation at START, not at completion. This
                // would expose an unsafe late arrival sharing an older barrier.
                let generation = calls.fetch_add(1, Ordering::SeqCst) + 1;
                started.send(generation).unwrap();
                release.acquire_owned().await.unwrap().forget();
                Ok(ReadFence {
                    applied: LogId::new(CommittedLeaderId::new(1, 1), generation as u64),
                    revision: generation as u64,
                })
            }
        },
        Settings { capacity, timeout },
    );
    Gate {
        batcher,
        started,
        release,
        calls,
    }
}

fn enroll(batcher: &Batcher) -> oneshot::Receiver<Outcome> {
    let (sender, receiver) = oneshot::channel();
    batcher.0.sender.try_send(sender).unwrap();
    receiver
}

#[tokio::test]
async fn only_callers_enrolled_before_barrier_start_share_its_fence() {
    let mut gate = gate(8, READ_TIMEOUT);
    let first = enroll(&gate.batcher);
    let same_cohort = enroll(&gate.batcher);
    assert_eq!(gate.started.recv().await, Some(1));
    let mut arrived_during_barrier = enroll(&gate.batcher);
    gate.release.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap().revision, 1);
    assert_eq!(same_cohort.await.unwrap().unwrap().revision, 1);
    assert_eq!(gate.started.recv().await, Some(2));
    assert!(matches!(
        arrived_during_barrier.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    gate.release.add_permits(1);
    assert_eq!(arrived_during_barrier.await.unwrap().unwrap().revision, 2);
    gate.batcher.shutdown().await;
}

#[tokio::test]
async fn two_stage_cohorts_cannot_attach_new_replica_reads_to_an_active_leader_proof() {
    let mut leader = gate(8, READ_TIMEOUT);
    let leader_requests = leader.batcher.clone();
    let replica = Batcher::new(
        move || {
            let leader = leader_requests.clone();
            async move { leader.request().await }
        },
        Settings {
            capacity: 8,
            timeout: READ_TIMEOUT,
        },
    );
    let first = enroll(&replica);
    assert_eq!(leader.started.recv().await, Some(1));
    let mut later = enroll(&replica);
    leader.release.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap().revision, 1);
    assert_eq!(leader.started.recv().await, Some(2));
    assert!(matches!(
        later.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    leader.release.add_permits(1);
    assert_eq!(later.await.unwrap().unwrap().revision, 2);
    replica.shutdown().await;
    leader.batcher.shutdown().await;
}

#[tokio::test]
async fn overload_and_cancellation_are_bounded_without_poisoning_the_next_cohort() {
    let mut gate = gate(2, READ_TIMEOUT);
    let active = enroll(&gate.batcher);
    assert_eq!(gate.started.recv().await, Some(1));
    let canceled = enroll(&gate.batcher);
    let surviving = enroll(&gate.batcher);
    let error = gate.batcher.request().await.unwrap_err();
    assert!(error.to_string().contains("queue is full"));
    drop(canceled);
    gate.release.add_permits(1);
    active.await.unwrap().unwrap();
    assert_eq!(gate.started.recv().await, Some(2));
    gate.release.add_permits(1);
    assert_eq!(surviving.await.unwrap().unwrap().revision, 2);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    gate.batcher.shutdown().await;
}

#[tokio::test]
async fn canceling_a_whole_queued_cohort_does_not_start_another_barrier() {
    let mut gate = gate(8, READ_TIMEOUT);
    let active = enroll(&gate.batcher);
    assert_eq!(gate.started.recv().await, Some(1));
    drop(enroll(&gate.batcher));
    drop(enroll(&gate.batcher));
    gate.release.add_permits(1);
    active.await.unwrap().unwrap();
    // Let the actor drain both canceled entries and return to recv().
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    let next = enroll(&gate.batcher);
    assert_eq!(gate.started.recv().await, Some(2));
    gate.release.add_permits(1);
    next.await.unwrap().unwrap();
    gate.batcher.shutdown().await;
}

#[tokio::test]
async fn shutdown_and_final_drop_release_active_and_queued_callers() {
    for explicit_shutdown in [true, false] {
        let mut gate = gate(8, READ_TIMEOUT);
        let active = enroll(&gate.batcher);
        assert_eq!(gate.started.recv().await, Some(1));
        let queued = enroll(&gate.batcher);
        if explicit_shutdown {
            gate.batcher.shutdown().await;
            assert!(
                gate.batcher
                    .request()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("stopped")
            );
        }
        drop(gate.batcher);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), active)
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), queued)
                .await
                .unwrap()
                .is_err()
        );
    }
}

#[tokio::test]
async fn caller_deadline_includes_time_queued_behind_another_barrier() {
    let mut gate = gate(8, Duration::from_millis(100));
    let active = enroll(&gate.batcher);
    assert_eq!(gate.started.recv().await, Some(1));
    tokio::time::sleep(Duration::from_millis(30)).await;
    // First barrier expires after 100 ms. This caller then has only the
    // remainder of its original deadline, not a new 100 ms for barrier #2.
    let error = gate.batcher.request().await.unwrap_err();
    assert!(error.to_string().contains("queued read timed out"));
    assert!(active.await.unwrap().is_err());
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
    gate.batcher.shutdown().await;
}

#[test]
fn capacity_defaults_scale_with_cpu_and_operator_override_has_only_platform_bounds() {
    assert_eq!(Settings::parse(None, 18).unwrap().capacity, 1152);
    assert_eq!(Settings::parse(None, 1).unwrap().capacity, 64);
    assert_eq!(Settings::parse(Some("1"), 18).unwrap().capacity, 1);
    assert_eq!(
        Settings::parse(Some("1000000"), 18).unwrap().capacity,
        1_000_000
    );
    assert_eq!(
        Settings::parse(Some(&tokio::sync::Semaphore::MAX_PERMITS.to_string()), 18)
            .unwrap()
            .capacity,
        tokio::sync::Semaphore::MAX_PERMITS
    );
    for value in ["0", "-1", "", "no", "1.5"] {
        assert!(Settings::parse(Some(value), 18).is_err(), "{value}");
    }
    assert!(
        Settings::parse(
            Some(&(tokio::sync::Semaphore::MAX_PERMITS + 1).to_string()),
            18
        )
        .is_err()
    );
}
