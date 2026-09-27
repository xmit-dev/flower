//! The keys each apply writes, in publication order, for readers that wake
//! only when a write touches what they read.
use std::sync::Arc;

use tokio::sync::broadcast;

/// Enough applies for a reader that falls behind briefly. One that lags
/// further learns so, and must treat everything as changed.
const CAPACITY: usize = 4096;

/// Which application state changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    Root,
    Partition(String),
    /// Every state: an installed snapshot replaced them.
    All,
}

/// One apply's writes to one application state, once readers can see them.
#[derive(Debug)]
pub struct Changes {
    pub scope: Scope,
    /// The revision these writes produced. `Scope::All` carries 0.
    pub revision: u64,
    /// The stored keys written, or `None` when anything may have changed.
    pub keys: Option<Vec<String>>,
}

impl Changes {
    /// Whether these changes can touch the state `partition` names, the root
    /// application for `None`.
    pub fn concern(&self, partition: Option<&str>) -> bool {
        match (&self.scope, partition) {
            (Scope::All, _) | (Scope::Root, None) => true,
            (Scope::Partition(changed), Some(partition)) => changed == partition,
            _ => false,
        }
    }
}

pub(super) type Sender = broadcast::Sender<Arc<Changes>>;

pub(super) fn sender() -> Sender {
    broadcast::channel(CAPACITY).0
}
