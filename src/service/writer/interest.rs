//! What maintenance waits for between runs. A run's hint (NextRun) holds until
//! the time it names or until a write to something the run read, so after an
//! evaluation that recorded its reads, only a write to them makes it run
//! again soon; other writes leave its timer as the hint set it. Without such
//! a record (a failure, a follower, a locked database, a graph that reads the
//! clock, no certificate) any write of the database does, as it always did.
use std::collections::{HashMap, HashSet};

use crate::consensus::changes::Changes;
use crate::evaluator::{MutationCertificate, Observation, touched, touches_everything};

#[derive(Debug)]
pub(crate) struct Interest {
    /// The revision the hint reflects: writes up to it were read, the run's
    /// own commit included.
    seen: u64,
    wake: Wake,
}

#[derive(Debug)]
enum Wake {
    /// A follower runs nothing until it leads, which the Raft progress says.
    Never,
    /// Any later write of this database.
    Any,
    /// Later writes to what the run read.
    Reads(Reads),
}

#[derive(Debug, Default)]
struct Reads {
    /// Records and membership markers, as `touched` names them.
    keys: HashSet<String>,
    /// Declared index windows `[lower, upper)` by the marker their entries move.
    ranges: HashMap<String, Vec<(String, String)>>,
}

impl Interest {
    /// Every write of the database after `seen`.
    pub fn any(seen: u64) -> Self {
        Self {
            seen,
            wake: Wake::Any,
        }
    }

    /// No write: only a leadership change, which the actor hears of itself.
    pub fn never() -> Self {
        Self {
            seen: u64::MAX,
            wake: Wake::Never,
        }
    }

    /// Writes after `seen` to what `certificate` observed.
    pub fn reads(seen: u64, certificate: &MutationCertificate) -> Self {
        let mut reads = Reads::default();
        for observation in certificate.observations() {
            match observation {
                Observation::Key(key) => {
                    reads.keys.insert(key.to_owned());
                }
                Observation::Range {
                    marker,
                    lower,
                    upper,
                } => reads
                    .ranges
                    .entry(marker.to_owned())
                    .or_default()
                    .push((lower.to_owned(), upper.to_owned())),
            }
        }
        Self {
            seen,
            wake: Wake::Reads(reads),
        }
    }

    /// Whether `changes`, which concern this database, can have moved a due
    /// time the hint did not account for.
    pub fn touched_by(&self, changes: &Changes) -> bool {
        let reads = match &self.wake {
            Wake::Never => return false,
            Wake::Any => None,
            Wake::Reads(reads) => Some(reads),
        };
        // A replaced state or an installed snapshot says nothing of its keys,
        // and its revision may not follow the one the hint saw.
        let Some(keys) = &changes.keys else {
            return true;
        };
        if changes.revision <= self.seen {
            return false;
        }
        let Some(reads) = reads else { return true };
        keys.iter()
            .any(|key| touches_everything(key) || reads.touched(key))
    }

    /// Records, markers and windows it waits for, for tests and logs.
    pub fn observed(&self) -> Option<usize> {
        match &self.wake {
            Wake::Reads(reads) => {
                Some(reads.keys.len() + reads.ranges.values().map(Vec::len).sum::<usize>())
            }
            _ => None,
        }
    }
}

impl Reads {
    /// As a watch hub's certificate: the record, a marker it moves, or a
    /// window of that marker's index holding it.
    fn touched(&self, key: &str) -> bool {
        let mut found = false;
        touched(key, |id| {
            found = found
                || self.keys.contains(id)
                || self.ranges.get(id).is_some_and(|windows| {
                    windows
                        .iter()
                        .any(|(lower, upper)| lower.as_str() <= key && key < upper.as_str())
                });
        });
        found
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::*;
    use crate::consensus::Records;
    use crate::consensus::changes::Scope;
    use crate::evaluator::{Evaluation, evaluate, hash, invoke_at, invoke_maintenance_at};

    const BUNDLE: &str = r#"var __flowerBundle={default:{collections:[{name:'tasks',indexes:{byDue:['due']}}],definitions:{
      save:{kind:'mutationMethod',name:'save',compute:(ctx,args)=>{ctx.set({kind:'collection',name:args.collection},args.id,args.value);return null;}},
      drop:{kind:'mutationMethod',name:'drop',compute:(ctx,args)=>{ctx.delete({kind:'collection',name:'tasks'},args.id);return null;}},
      run:{kind:'mutationMethod',name:'run',compute:(ctx)=>{
        if(ctx.get({kind:'collection',name:'settings'},'paused')!==null) return {$flower:{next:null}};
        const first=ctx.range({kind:'range',collection:'tasks',fields:['due'],options:{gte:0,limit:1}}).rows[0];
        return {$flower:{next:first ? first.value.due : null}};
      }}
    },http:{},maintenance:{name:'run',kind:'mutation'}}};"#;

    fn apply(data: &mut Records, evaluation: &Evaluation) {
        for key in &evaluation.deletes {
            data.remove(key);
        }
        for (key, value) in &evaluation.puts {
            data.insert(key.clone(), value.clone());
        }
    }

    /// What a write of `args` to `name` changes, as the store publishes it.
    fn write(data: &Records, name: &str, args: Value) -> Arc<Changes> {
        let evaluation = invoke_at(
            data.clone(),
            json!({"name":name,"args":args,"requestId":name}),
            "mutation",
            5,
        )
        .unwrap();
        Arc::new(Changes {
            scope: Scope::Root,
            revision: 11,
            keys: Some(
                evaluation
                    .puts
                    .keys()
                    .chain(&evaluation.deletes)
                    .cloned()
                    .collect(),
            ),
        })
    }

    fn task(id: &str, due: u64) -> Value {
        json!({"collection":"tasks","id":id,"value":{"due":due}})
    }

    #[test]
    fn maintenance_waits_for_writes_to_what_its_run_read() {
        let deployed = evaluate(
            BTreeMap::new(),
            json!({"requestId":"deploy","bundle":{"hash":hash(BUNDLE.as_bytes()),"javascript":BUNDLE}}),
        )
        .unwrap();
        let mut data: Records = deployed.puts.into();
        for (id, due) in [("a", 100), ("b", 200), ("c", 300)] {
            let saved = invoke_at(
                data.clone(),
                json!({"name":"save","args":task(id, due),"requestId":id}),
                "mutation",
                1,
            )
            .unwrap();
            apply(&mut data, &saved);
        }
        let run = invoke_maintenance_at(
            data.clone(),
            json!({"name":"run","args":null,"requestId":"__flower.maintenance"}),
            2,
        )
        .unwrap();
        assert_eq!(run.value, json!({"$flower":{"next":100}}));
        let interest = Interest::reads(10, run.mutation_certificate.as_ref().unwrap());
        assert!(interest.observed().unwrap() > 0);
        let expect = |changes: Arc<Changes>, touched: bool, why: &str| {
            assert_eq!(
                interest.touched_by(&changes),
                touched,
                "{why}: {:?}",
                changes.keys
            );
        };
        // Records it never read, and a task due after those its limit-1 page
        // examined (a and, looking ahead, b), change nothing it computed.
        expect(
            write(
                &data,
                "save",
                json!({"collection":"notes","id":"n","value":1}),
            ),
            false,
            "another collection",
        );
        expect(write(&data, "save", task("d", 400)), false, "a later task");
        expect(
            write(&data, "save", task("c", 250)),
            false,
            "a later task moved",
        );
        // An earlier task, the first one moved or gone, the one after it, or
        // a record it read that was missing, may move the due time.
        expect(write(&data, "save", task("e", 50)), true, "an earlier task");
        expect(write(&data, "save", task("e", 150)), true, "a task between");
        expect(
            write(&data, "save", task("a", 500)),
            true,
            "the first task moved",
        );
        expect(
            write(&data, "drop", json!({"id":"a"})),
            true,
            "the first task gone",
        );
        expect(
            write(&data, "save", task("b", 150)),
            true,
            "the second task moved",
        );
        expect(
            write(
                &data,
                "save",
                json!({"collection":"settings","id":"paused","value":true}),
            ),
            true,
            "a missing record it read",
        );
        // Code, registrations and anything of unknown extent concern every reader.
        let global = |keys: Option<Vec<String>>, revision| {
            Arc::new(Changes {
                scope: Scope::Root,
                revision,
                keys,
            })
        };
        expect(global(Some(vec!["bundle".into()]), 11), true, "new code");
        expect(
            global(Some(vec!["maintenanceMethod".into()]), 11),
            true,
            "a new handler",
        );
        expect(global(None, 11), true, "replaced state");
        expect(global(None, 3), true, "an installed snapshot");
        // Writes the run already read, its own commit included, are covered.
        let seen = write(&data, "save", task("e", 50));
        let seen = global(seen.keys.clone(), 10);
        expect(seen.clone(), false, "at the revision it read");
        assert!(Interest::any(10).touched_by(&global(Some(vec!["clock".into()]), 11)));
        assert!(!Interest::any(10).touched_by(&seen));
        assert!(!Interest::never().touched_by(&global(None, 11)));
        assert!(!Interest::never().touched_by(&global(Some(vec!["bundle".into()]), 11)));
    }
}
