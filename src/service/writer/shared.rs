//! Share prepared patches with the writer overlay until last-write-wins
//! compaction. Only surviving values need an owned JSON tree for Raft.
use super::*;
use crate::consensus::{BatchItem, CompactBatch};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub(super) struct SharedCommit {
    internal: bool,
    request_id: String,
    fingerprint: String,
    expected_revision: u64,
    // The input map already supplied sorted unique keys. Staging and
    // compaction only iterate them; a second search tree adds no value.
    puts: Vec<(String, Arc<Value>)>,
    deletes: Vec<String>,
    result: Value,
}

impl SharedCommit {
    pub(super) fn stage(state: &mut Snapshot, command: Commit) -> Result<Self, ApiError> {
        // All fallible accounting still observes the complete owned patch and
        // runs before changing either records or receipts.
        let plan = plan_stage(state, &command)?;
        let command = Self {
            internal: command.internal,
            request_id: command.request_id,
            fingerprint: command.fingerprint,
            expected_revision: command.expected_revision,
            puts: command
                .puts
                .into_iter()
                .map(|(key, value)| (key, Arc::new(value)))
                .collect(),
            deletes: command.deletes,
            result: command.result,
        };
        for key in &command.deletes {
            state.data.remove(key);
        }
        for (key, value) in &command.puts {
            state.data.insert_shared(key.clone(), value.clone());
        }
        publish_stage(state, &command.request_id, plan);
        Ok(command)
    }

    pub(super) fn into_commit(self) -> Commit {
        Commit {
            internal: self.internal,
            request_id: self.request_id,
            fingerprint: self.fingerprint,
            expected_revision: self.expected_revision,
            puts: self
                .puts
                .into_iter()
                .map(|(key, value)| (key, Arc::unwrap_or_clone(value)))
                .collect(),
            deletes: self.deletes,
            result: self.result,
        }
    }

    pub(super) fn compact(commands: Vec<Self>) -> anyhow::Result<CompactBatch> {
        let expected_revision = commands
            .first()
            .ok_or_else(|| {
                anyhow::anyhow!("a consensus group requires at least one application commit")
            })?
            .expected_revision;
        let mut puts = BTreeMap::new();
        let mut deletes = BTreeSet::new();
        let mut items = Vec::with_capacity(commands.len());
        for (index, command) in commands.into_iter().enumerate() {
            anyhow::ensure!(
                expected_revision.checked_add(index as u64) == Some(command.expected_revision),
                "group commits require contiguous expected revisions"
            );
            // Match CompactBatch::new: deletes precede puts within an invocation,
            // and later invocations replace earlier changes. Intermediate JSON
            // trees are dropped without ever being deep-copied for staging.
            for key in command.deletes {
                puts.remove(&key);
                deletes.insert(key);
            }
            for (key, value) in command.puts {
                deletes.remove(&key);
                puts.insert(key, value);
            }
            // As CompactBatch::new logs it: a receipt-less item has no identity.
            let identity = |value: String| {
                if command.internal {
                    String::new()
                } else {
                    value
                }
            };
            items.push(BatchItem {
                internal: command.internal,
                request_id: identity(command.request_id),
                fingerprint: identity(command.fingerprint),
                result: command.result,
            });
        }
        // A successor snapshot may still own a final value, requiring one copy.
        // Consensus validates the finished batch and its exact encoded size.
        Ok(CompactBatch {
            expected_revision,
            puts: puts
                .into_iter()
                .map(|(key, value)| (key, Arc::unwrap_or_clone(value)))
                .collect(),
            deletes: deletes.into_iter().collect(),
            items,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(index: u64, puts: &[(&str, Value)], deletes: &[&str]) -> Commit {
        Commit {
            internal: false,
            request_id: format!("request-{index}"),
            fingerprint: format!("fingerprint-{index}"),
            expected_revision: index,
            puts: puts
                .iter()
                .map(|(key, value)| ((*key).into(), value.clone()))
                .collect(),
            deletes: deletes.iter().map(|key| (*key).into()).collect(),
            result: json!({"invocation": index}),
        }
    }

    fn put<'a>(command: &'a SharedCommit, key: &str) -> &'a Arc<Value> {
        &command.puts.iter().find(|(name, _)| name == key).unwrap().1
    }

    #[test]
    fn shared_staging_and_compaction_match_owned_overwrites_deletes_and_reinsertion() {
        let mut state = Snapshot::default();
        state
            .data
            .insert("untouched".into(), json!({"retain": [1, 2, 3]}));
        let original = state.clone();
        let mut reference = state.clone();
        let mut commands = vec![
            command(
                0,
                &[
                    ("hot", json!({"large": "x".repeat(4096)})),
                    ("other", json!(1)),
                ],
                &[],
            ),
            command(1, &[("hot", json!({"changed": 2}))], &["other"]),
            command(2, &[("other", json!(3))], &["hot"]),
            command(3, &[("hot", json!({"final": 4}))], &["hot", "other"]),
        ];
        commands[2].internal = true;
        let expected = CompactBatch::new(commands.clone()).unwrap();
        let mut shared = Vec::new();
        let mut overwritten = None;
        for command in commands {
            // The old owned staging path is an independent semantic reference.
            stage(&mut reference, &command).unwrap();
            let staged = SharedCommit::stage(&mut state, command).unwrap();
            for (key, value) in &staged.puts {
                assert!(Arc::ptr_eq(value, state.data.get_shared(key).unwrap()));
            }
            if shared.is_empty() {
                overwritten = Some(Arc::downgrade(put(&staged, "hot")));
            }
            assert_eq!(state, reference);
            shared.push(staged);
        }
        assert_eq!(
            state.requests.len(),
            3,
            "internal work has no retry receipt"
        );
        assert_eq!(state.requests["request-0"].result, json!({"invocation": 0}));
        assert_eq!(state.requests["request-3"].revision, 4);
        assert_eq!(original.revision, 0);
        assert_eq!(original.data.len(), 1);
        assert!(Arc::ptr_eq(
            original.data.get_shared("untouched").unwrap(),
            state.data.get_shared("untouched").unwrap(),
        ));
        assert!(overwritten.as_ref().unwrap().upgrade().is_some());
        let compact = SharedCommit::compact(shared).unwrap();
        assert_eq!(compact, expected);
        assert_eq!(
            serde_json::to_vec(&compact).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
        assert!(
            overwritten.unwrap().upgrade().is_none(),
            "overwritten trees do not survive compaction"
        );
    }

    #[test]
    fn shared_single_preserves_wire_and_moves_a_value_after_the_overlay_drops() {
        let original = command(0, &[("hot", json!("x".repeat(4096)))], &["old"]);
        let mut state = Snapshot::default();
        let shared = SharedCommit::stage(&mut state, original.clone()).unwrap();
        let allocation = put(&shared, "hot").as_str().unwrap().as_ptr();
        drop(state);
        let owned = shared.into_commit();
        assert_eq!(owned.puts["hot"].as_str().unwrap().as_ptr(), allocation);
        assert_eq!(owned, original);
        assert_eq!(
            serde_json::to_vec(&owned).unwrap(),
            serde_json::to_vec(&original).unwrap()
        );
    }

    #[test]
    fn shared_staging_accounting_failure_keeps_records_receipts_and_revision_unchanged() {
        use crate::consensus::retention as policy;
        let mut state = Snapshot::default();
        let retention = policy::State {
            database: "a".repeat(32),
            incarnation: "b".repeat(32),
            current_epoch: 0,
            min_epoch: 0,
            receipt_bytes: 0,
            receipt_count: 0,
            session_bytes: 0,
            session_count: 0,
            max_receipt_bytes: Some(0),
            gc_cursor: None,
            gc_complete: false,
            gc_receipts_complete: false,
            gc_session_cursor: None,
            rotation: None,
        };
        state.data.insert(policy::KEY.into(), json!(retention));
        state.data.insert("old".into(), json!("untouched"));
        let original = state.clone();
        let mut command = command(0, &[("new", json!({"never": "visible"}))], &["old"]);
        command.request_id = policy::scope_request_id(&retention, "request");
        let expected_error = stage(&mut original.clone(), &command).unwrap_err();
        let error = SharedCommit::stage(&mut state, command).unwrap_err();
        assert_eq!(
            (error.status, error.code),
            (expected_error.status, expected_error.code)
        );
        assert_eq!(state, original);
        assert!(state.data.ptr_eq(&original.data));
        assert!(state.requests.ptr_eq(&original.requests));
    }

    #[test]
    fn shared_compaction_rejects_empty_and_noncontiguous_groups() {
        assert!(SharedCommit::compact(Vec::new()).is_err());
        let mut state = Snapshot::default();
        let first = SharedCommit::stage(&mut state, command(0, &[], &[])).unwrap();
        let second = SharedCommit::stage(&mut state, command(2, &[], &[])).unwrap();
        assert!(SharedCommit::compact(vec![first, second]).is_err());
    }

    #[test]
    fn shared_empty_puts_keep_deletes_revisions_and_per_invocation_receipts() {
        let mut state = Snapshot::default();
        state.data.insert("old".into(), json!(true));
        let mut commands = vec![
            command(0, &[], &["old"]),
            command(1, &[], &[]),
            command(2, &[], &[]),
        ];
        commands[1].internal = true;
        let expected = CompactBatch::new(commands.clone()).unwrap();
        let shared = commands
            .into_iter()
            .map(|command| {
                let command = SharedCommit::stage(&mut state, command).unwrap();
                assert!(command.puts.is_empty());
                command
            })
            .collect();
        assert_eq!(SharedCommit::compact(shared).unwrap(), expected);
        assert!(!state.data.contains_key("old"));
        assert_eq!(state.revision, 3);
        assert_eq!(state.requests.len(), 2);
        assert_eq!(state.requests["request-0"].revision, 1);
        assert_eq!(state.requests["request-2"].revision, 3);
        assert_eq!(state.requests["request-2"].result, json!({"invocation": 2}));
    }
}
