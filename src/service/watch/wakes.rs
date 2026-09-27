//! Which watch hubs a publication can change. A hub's result stays exact
//! while the certificate of its evaluation holds, and only a write of
//! something that certificate observed can end that, so a publication wakes
//! just the hubs whose observations it wrote. A result without a certificate,
//! such as one that read the clock, wakes on every publication of its state.
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use tokio::sync::watch;

use crate::consensus::changes::Changes;
use crate::evaluator::{DependencyCertificate, Observation, touched, touches_everything as global};

/// Keys of recent publications, which a registration checks for writes its
/// hub may have missed while it evaluated.
const RECENT_KEYS: usize = 1 << 16;

/// A hub's signal: the latest revision whose writes touched its result.
/// Subscribers holding an earlier result refresh.
pub(super) type Signal = Arc<watch::Sender<u64>>;

/// Raise `signal` to `revision`.
pub(super) fn wake(signal: &Signal, revision: u64) {
    signal.send_if_modified(|woken| {
        let raised = revision > *woken;
        *woken = (*woken).max(revision);
        raised
    });
}

#[derive(Default)]
pub(super) struct Wakes {
    hubs: HashMap<u64, Hub>,
    /// Observed record keys and membership markers, with the hubs observing them.
    keys: HashMap<String, HashSet<u64>>,
    /// Observed windows by their marker, by hub.
    ranges: HashMap<String, HashMap<u64, Vec<(String, String)>>>,
    /// Hubs whose results have no certificate.
    every: HashSet<u64>,
    recent: VecDeque<Arc<Changes>>,
    recent_keys: usize,
    /// A registration at an earlier revision may have missed writes.
    floor: u64,
}

struct Hub {
    signal: Signal,
    keys: Vec<String>,
    markers: Vec<String>,
}

impl Wakes {
    /// Start from the state published at `revision`, after which every
    /// publication arrives through `publish`.
    pub fn restart(&mut self, revision: u64) {
        self.recent.clear();
        self.recent_keys = 0;
        self.floor = revision;
        self.wake_all(revision);
    }

    /// Wake `signal` for writes after `revision` to what `certificate`
    /// observed, or to anything when there is none, replacing what the hub
    /// watched before. Writes it may have missed wake it at once.
    pub fn register(
        &mut self,
        id: u64,
        signal: &Signal,
        revision: u64,
        certificate: Option<&DependencyCertificate>,
    ) {
        self.remove(id);
        let mut hub = Hub {
            signal: signal.clone(),
            keys: Vec::new(),
            markers: Vec::new(),
        };
        match certificate {
            None => {
                self.every.insert(id);
            }
            Some(certificate) => {
                for observation in certificate.observations() {
                    match observation {
                        Observation::Key(key) => {
                            if self.keys.entry(key.into()).or_default().insert(id) {
                                hub.keys.push(key.into());
                            }
                        }
                        Observation::Range {
                            marker,
                            lower,
                            upper,
                        } => {
                            let windows = self
                                .ranges
                                .entry(marker.into())
                                .or_default()
                                .entry(id)
                                .or_default();
                            if windows.is_empty() {
                                hub.markers.push(marker.into());
                            }
                            windows.push((lower.into(), upper.into()));
                        }
                    }
                }
            }
        }
        self.hubs.insert(id, hub);
        let missed = if revision < self.floor {
            Some(self.floor)
        } else {
            self.recent
                .iter()
                .rev()
                .take_while(|changes| changes.revision > revision)
                .filter(|changes| self.touches(id, changes))
                .map(|changes| changes.revision)
                .max()
        };
        if let Some(missed) = missed {
            wake(signal, missed);
        }
    }

    pub fn remove(&mut self, id: u64) {
        let Some(hub) = self.hubs.remove(&id) else {
            return;
        };
        self.every.remove(&id);
        for key in hub.keys {
            if let Some(hubs) = self.keys.get_mut(&key) {
                hubs.remove(&id);
                if hubs.is_empty() {
                    self.keys.remove(&key);
                }
            }
        }
        for marker in hub.markers {
            if let Some(windows) = self.ranges.get_mut(&marker) {
                windows.remove(&id);
                if windows.is_empty() {
                    self.ranges.remove(&marker);
                }
            }
        }
    }

    /// Wake the hubs these writes touch, and keep them for registrations
    /// of results evaluated before them.
    pub fn publish(&mut self, changes: Arc<Changes>) {
        let revision = changes.revision;
        match &changes.keys {
            Some(keys) if !keys.iter().any(|key| global(key)) => {
                let mut woken: HashSet<u64> = self.every.iter().copied().collect();
                for key in keys {
                    self.touched_hubs(key, |id| {
                        woken.insert(id);
                    });
                }
                for hub in woken.iter().filter_map(|id| self.hubs.get(id)) {
                    wake(&hub.signal, revision);
                }
            }
            _ => self.wake_all(revision),
        }
        if changes.keys.is_none() {
            self.recent.clear();
            self.recent_keys = 0;
            self.floor = self.floor.max(revision);
            return;
        }
        self.recent_keys += changes.keys.as_ref().map_or(0, Vec::len);
        self.recent.push_back(changes);
        while self.recent_keys > RECENT_KEYS
            && let Some(oldest) = self.recent.pop_front()
        {
            self.recent_keys -= oldest.keys.as_ref().map_or(0, Vec::len);
            self.floor = self.floor.max(oldest.revision);
        }
    }

    fn wake_all(&self, revision: u64) {
        for hub in self.hubs.values() {
            wake(&hub.signal, revision);
        }
    }

    fn touched_hubs(&self, key: &str, mut hub: impl FnMut(u64)) {
        touched(key, |id| {
            if let Some(hubs) = self.keys.get(id) {
                hubs.iter().copied().for_each(&mut hub);
            }
            if let Some(windows) = self.ranges.get(id) {
                for (id, windows) in windows {
                    if windows
                        .iter()
                        .any(|(lower, upper)| lower.as_str() <= key && key < upper.as_str())
                    {
                        hub(*id);
                    }
                }
            }
        });
    }

    fn touches(&self, id: u64, changes: &Changes) -> bool {
        if self.every.contains(&id) {
            return true;
        }
        let Some(keys) = &changes.keys else {
            return true;
        };
        keys.iter().any(|key| {
            let mut found = global(key);
            if !found {
                self.touched_hubs(key, |hub| found |= hub == id);
            }
            found
        })
    }

    #[cfg(test)]
    pub fn watched(&self) -> usize {
        self.hubs.len()
    }
}
