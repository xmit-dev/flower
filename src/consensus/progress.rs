//! What an idle watch waits for. OpenRaft republishes its metrics on every
//! quorum acknowledgement, twenty times a second on an idle leader. One task
//! condenses them into the only changes a watch acts on, so an idle watch
//! costs nothing until one arrives.
use std::sync::Arc;
use std::time::Duration;

use openraft::{LogId, ServerState};
use tokio::sync::watch;
use tokio::time::Instant;

use super::FlowerRaft;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    /// The last applied log. Application state can change only when it moves.
    pub applied: Option<LogId<u64>>,
    /// Increases whenever this replica's data may have stopped being current:
    /// its role, term or leader changed, or, as leader, its lease lapsed with
    /// no quorum acknowledgement. Fresh watches then prove a new read fence.
    pub suspicion: u64,
    pub running: bool,
}

/// Leadership facts whose change makes every earlier freshness proof suspect.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Role {
    state: ServerState,
    term: u64,
    leader: Option<u64>,
}

pub(super) fn spawn(
    raft: FlowerRaft,
    lease: Duration,
) -> (Arc<watch::Sender<()>>, watch::Receiver<Progress>) {
    let mut metrics = raft.metrics();
    let (role, applied, running, mut lease_end) = {
        let current = metrics.borrow_and_update();
        (
            role_of(&current),
            current.last_applied,
            is_running(&current),
            lease_end_of(&current, lease),
        )
    };
    let (sender, receiver) = watch::channel(Progress {
        applied,
        suspicion: 0,
        running,
    });
    let (lifetime, mut dropped) = watch::channel(());
    tokio::spawn(async move {
        let mut role = role;
        let mut lapsed = false;
        loop {
            tokio::select! {
                biased;
                _ = dropped.changed() => return,
                changed = metrics.changed() => {
                    if changed.is_err() {
                        sender.send_modify(|progress| progress.running = false);
                        return;
                    }
                    let (next, applied, running, end) = {
                        let current = metrics.borrow_and_update();
                        (role_of(&current), current.last_applied, is_running(&current), lease_end_of(&current, lease))
                    };
                    // A new acknowledgement moves the lease end by at least one
                    // heartbeat; millisecond rounding moves it by far less.
                    if end.is_none_or(|end| lease_end.is_none_or(|old| end > old + Duration::from_millis(5))) {
                        lapsed = false;
                    }
                    lease_end = end;
                    let suspect = next != role;
                    role = next;
                    sender.send_if_modified(|progress| {
                        let changed = progress.applied != applied || progress.running != running || suspect;
                        progress.applied = applied;
                        progress.running = running;
                        if suspect {
                            progress.suspicion += 1;
                        }
                        changed
                    });
                    if !running {
                        return;
                    }
                }
                // Heartbeats stopped reaching a quorum: another leader may exist.
                _ = sleep_until(lease_end), if lease_end.is_some() && !lapsed => {
                    lapsed = true;
                    sender.send_modify(|progress| progress.suspicion += 1);
                }
            }
        }
    });
    (Arc::new(lifetime), receiver)
}

fn role_of(metrics: &openraft::RaftMetrics<u64, openraft::BasicNode>) -> Role {
    Role {
        state: metrics.state,
        term: metrics.current_term,
        leader: metrics.current_leader,
    }
}

fn is_running(metrics: &openraft::RaftMetrics<u64, openraft::BasicNode>) -> bool {
    metrics.running_state.is_ok() && metrics.state != ServerState::Shutdown
}

/// A leader's lease runs from the send time of its last quorum-acknowledged
/// heartbeat, which OpenRaft reports as elapsed milliseconds.
fn lease_end_of(
    metrics: &openraft::RaftMetrics<u64, openraft::BasicNode>,
    lease: Duration,
) -> Option<Instant> {
    if metrics.state != ServerState::Leader {
        return None;
    }
    let elapsed = Duration::from_millis(metrics.millis_since_quorum_ack?);
    Instant::now().checked_sub(elapsed)?.checked_add(lease)
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
