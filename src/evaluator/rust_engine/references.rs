//! Foreign keys: a collection's rows refer to rows of another collection by
//! holding their keys, in fields or in their own keys. At the end of every
//! mutation, each row it wrote refers only to rows that exist, and no row it
//! deleted is still referred to; a deployment that declares a reference checks
//! the rows already there. Removing or updating the rows that refer to a
//! deleted one (`onDelete`) is the SDK's, inside the mutation, so that
//! triggers see those changes too: this is the invariant alone.
use super::*;
use std::ops::Bound;

/// Where a row holds the key of the row it refers to, besides its fields.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(untagged)]
pub enum KeyPart {
    /// `true`: the row's whole key is the target's key (one row extends another).
    Whole(bool),
    /// The first components of the row's JSON tuple key.
    Leading(usize),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ReferenceSpec {
    /// The collection whose rows refer.
    pub collection: String,
    /// The collection whose rows they refer to.
    pub target: String,
    /// The fields that hold the target's key, in order. A missing or null
    /// field refers to nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    /// Or where in the row's key it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyPart>,
    /// The target's keys are canonical JSON (`collection.key(schema)`): the
    /// parts make one, a single part itself and several a tuple. Otherwise a
    /// single part, a string, is the key.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub json: bool,
}

/// What the parts of a reference name.
enum Named {
    /// Nothing: a part is missing or null.
    Nothing,
    /// The target's key.
    Key(String),
    /// No key the target can have: a plain key that isn't a string, say.
    Impossible,
}

impl ReferenceSpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.collection.is_empty() || self.target.is_empty() {
            return Err("a reference names its collection and its target".into());
        }
        let distinct = self
            .fields
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == self.fields.len();
        match (&self.key, self.fields.len()) {
            (None, 0) => Err("a reference names its fields or its key".into()),
            (None, _) if self.fields.iter().any(String::is_empty) || !distinct => {
                Err("reference fields must be distinct nonempty strings".into())
            }
            (None, count) if count > 1 && !self.json => {
                Err("several fields refer only to a target whose keys are JSON tuples".into())
            }
            (None, _) => Ok(()),
            (Some(_), count) if count > 0 => {
                Err("a reference names its fields or its key, not both".into())
            }
            (Some(KeyPart::Whole(true)), _) => Ok(()),
            (Some(KeyPart::Whole(false)), _) => Err("a reference's key is true or a count".into()),
            (Some(KeyPart::Leading(0)), _) => {
                Err("a reference holds at least one component of its key".into())
            }
            (Some(KeyPart::Leading(count)), _) if *count > 1 && !self.json => Err(
                "several key components refer only to a target whose keys are JSON tuples".into(),
            ),
            (Some(KeyPart::Leading(_)), _) => Ok(()),
        }
    }

    pub(super) fn allocation_cost(&self) -> usize {
        self.fields.iter().fold(
            256usize
                .saturating_add(self.collection.len())
                .saturating_add(self.target.len()),
            |bytes, field| bytes.saturating_add(32 + field.len()),
        )
    }

    /// The index that finds a target's referring rows by their fields.
    pub(crate) fn index(&self) -> Option<IndexSpec> {
        (!self.fields.is_empty()).then(|| IndexSpec {
            collection: self.collection.clone(),
            fields: self.fields.clone(),
        })
    }

    /// The parts of a row's reference: its fields' values or its key's leading
    /// components. None when one is missing or null.
    fn parts(&self, key: &str, value: &Value) -> Option<Vec<Value>> {
        let parts: Vec<Value> = match &self.key {
            None => {
                let row = value.as_object()?;
                self.fields
                    .iter()
                    .map(|field| row.get(field).cloned())
                    .collect::<Option<_>>()?
            }
            Some(KeyPart::Whole(_)) => vec![Value::String(key.to_owned())],
            Some(KeyPart::Leading(count)) => {
                let Ok(Value::Array(mut components)) = serde_json::from_str::<Value>(key) else {
                    return None;
                };
                if components.len() < *count {
                    return None;
                }
                components.truncate(*count);
                components
            }
        };
        (!parts.iter().any(Value::is_null)).then_some(parts)
    }

    /// The target's key a row refers to.
    fn named(&self, key: &str, value: &Value) -> Named {
        if matches!(self.key, Some(KeyPart::Whole(_))) {
            // Both keyed alike, as the SDK requires: the same raw key.
            return Named::Key(key.to_owned());
        }
        let Some(mut parts) = self.parts(key, value) else {
            return Named::Nothing;
        };
        if self.json {
            let value = if parts.len() == 1 {
                parts.pop().expect("one part")
            } else {
                Value::Array(parts)
            };
            return Named::Key(canonical_json(&value));
        }
        match parts.pop() {
            Some(Value::String(key)) if parts.is_empty() => Named::Key(key),
            _ => Named::Impossible,
        }
    }

    /// The parts a row referring to the target's `key` holds, or None if no
    /// row can: a JSON key that isn't a tuple of as many parts, say.
    fn parts_of(&self, key: &str) -> Option<Vec<Value>> {
        let count = match &self.key {
            None => self.fields.len(),
            Some(KeyPart::Leading(count)) => *count,
            Some(KeyPart::Whole(_)) => return Some(vec![Value::String(key.to_owned())]),
        };
        if !self.json {
            return Some(vec![Value::String(key.to_owned())]);
        }
        let value = serde_json::from_str::<Value>(key).ok()?;
        if count == 1 {
            return Some(vec![value]);
        }
        match value {
            Value::Array(parts) if parts.len() == count => Some(parts),
            _ => None,
        }
    }

    fn place(&self) -> Value {
        match &self.key {
            None => json!({"fields": self.fields}),
            Some(KeyPart::Whole(_)) => json!({"key": true}),
            Some(KeyPart::Leading(count)) => json!({"key": count}),
        }
    }

    /// A row of `collection`, keyed `key`, refers to `target_key`, which no row has.
    fn missing(&self, key: &str, target_key: Option<&str>) -> EngineError {
        let message = match target_key {
            Some(target_key) => format!(
                "{} row {} refers to {} row {}, which does not exist",
                self.collection,
                shown(key),
                self.target,
                shown(target_key)
            ),
            None => format!(
                "{} row {} refers to a key no {} row can have",
                self.collection,
                shown(key),
                self.target
            ),
        };
        let mut error = EngineError::new("FOREIGN_KEY_VIOLATION", message);
        error.details = Some(json!({
            "collection": self.collection, "key": key, "references": self.place(),
            "target": self.target, "targetKey": target_key,
        }));
        error
    }

    /// The target's row keyed `target_key` is gone while `key` still refers to it.
    fn referred(&self, target_key: &str, key: Option<&str>) -> EngineError {
        let by = match key {
            Some(key) => format!("{} row {}", self.collection, shown(key)),
            None => format!("a {} row", self.collection),
        };
        let mut error = EngineError::new(
            "FOREIGN_KEY_VIOLATION",
            format!(
                "{} row {} is deleted while {by} still refers to it",
                self.target,
                shown(target_key)
            ),
        );
        error.details = Some(json!({
            "collection": self.collection, "key": key, "references": self.place(),
            "target": self.target, "targetKey": target_key, "deleted": true,
        }));
        error
    }
}

/// A key as a message shows it: quoted, and cut short past 200 characters.
fn shown(key: &str) -> String {
    let cut: String = key.chars().take(200).collect();
    let quoted = serde_json::to_string(&cut).expect("key encodes");
    if cut.len() < key.len() {
        format!("{}…", &quoted[..quoted.len() - 1])
    } else {
        quoted
    }
}

/// The source IDs of `collection`'s rows whose keys start with `prefix`:
/// escaping a key character by character keeps its prefixes, so they are
/// the IDs that start with this.
pub(super) fn source_key_prefix(collection: &str, prefix: &str) -> String {
    let id = source_id(collection, prefix);
    // Drop the closing `"]`.
    id[..id.len() - 2].to_owned()
}

impl Engine<'_> {
    /// After a mutation's (or deployment's) final preview, when `staged` is
    /// the state it commits: each source row it changed refers only to rows
    /// that exist, and each it deleted is referred to by none. Rows it left
    /// as they were need no check: deleting what they refer to is checked
    /// from that side. Reads go into the certificate, so an optimistic
    /// mutation conflicts with a concurrent write that changes the outcome.
    pub(super) fn check_references(&mut self) -> EngineResult<()> {
        let schema = self.schema.clone();
        if schema.references.is_empty() {
            return Ok(());
        }
        let mut changed: Vec<&String> = self
            .changed
            .iter()
            .filter(|id| id.starts_with("source:"))
            .collect();
        // One error for the same writes, whatever the set's order.
        changed.sort_unstable();
        let changed: Vec<String> = changed.into_iter().cloned().collect();
        for (position, id) in changed.iter().enumerate() {
            if position % 64 == 0 {
                self.check_fatal()?;
            }
            let (collection, key) = source_pair(id)?;
            let before = self.base.get_shared(id).cloned();
            let after = self.staged.get_shared(id).cloned();
            match (before, after) {
                (before, Some(after)) => {
                    for reference in schema
                        .references
                        .iter()
                        .filter(|r| r.collection == collection)
                    {
                        // A row that was there referred to a row that exists, and still does
                        // if its reference didn't change (a reference in its key never does).
                        if let Some(before) = &before
                            && (reference.key.is_some()
                                || reference.parts(&key, before) == reference.parts(&key, &after))
                        {
                            continue;
                        }
                        let named = reference.named(&key, &after);
                        self.check_target(reference, &key, named)?;
                    }
                }
                (Some(_), None) => {
                    for reference in schema.references.iter().filter(|r| r.target == collection) {
                        self.check_unreferred(reference, &key)?;
                    }
                }
                (None, None) => {}
            }
        }
        Ok(())
    }

    fn check_target(
        &mut self,
        reference: &ReferenceSpec,
        key: &str,
        named: Named,
    ) -> EngineResult<()> {
        match named {
            Named::Nothing => Ok(()),
            Named::Impossible => Err(reference.missing(key, None)),
            Named::Key(target_key) => {
                let id = source_id(&reference.target, &target_key);
                self.record_read(&id);
                if self.staged.contains_key(&id) {
                    Ok(())
                } else {
                    Err(reference.missing(key, Some(&target_key)))
                }
            }
        }
    }

    /// No row refers to the deleted target row keyed `target_key` through `reference`.
    fn check_unreferred(
        &mut self,
        reference: &ReferenceSpec,
        target_key: &str,
    ) -> EngineResult<()> {
        let Some(parts) = reference.parts_of(target_key) else {
            return Ok(());
        };
        let collection = reference.collection.as_str();
        match &reference.key {
            Some(KeyPart::Whole(_)) => {
                let id = source_id(collection, target_key);
                self.record_read(&id);
                if self.staged.contains_key(&id) {
                    return Err(reference.referred(target_key, Some(target_key)));
                }
            }
            Some(KeyPart::Leading(_)) => {
                // Keys are canonical JSON tuples: `[a,b` then `]` or `,…`.
                let mut head = canonical_json(&Value::Array(parts));
                head.pop();
                let exact = format!("{head}]");
                let id = source_id(collection, &exact);
                self.record_read(&id);
                if self.staged.contains_key(&id) {
                    return Err(reference.referred(target_key, Some(&exact)));
                }
                let lower = source_key_prefix(collection, &format!("{head},"));
                let upper = format!("{}-", &lower[..lower.len() - 1]);
                self.window_read(
                    dependencies::keys_marker(&collection_id(collection))
                        .expect("collection marker"),
                    &lower,
                    &upper,
                );
                let found = self
                    .staged
                    .range_shared::<(Bound<&str>, Bound<&str>)>((
                        Bound::Included(lower.as_str()),
                        Bound::Excluded(upper.as_str()),
                    ))
                    .next()
                    .map(|(id, _)| id.clone());
                if let Some(found) = found {
                    let (_, key) = source_pair(&found)?;
                    return Err(reference.referred(target_key, Some(&key)));
                }
            }
            None => {
                let expected = if parts.len() == 1 {
                    parts.into_iter().next().expect("one part")
                } else {
                    Value::Array(parts)
                };
                self.marker_read(indexes::bucket_id(collection, &reference.fields, &expected));
                if let Some(key) = self.first_indexed(collection, &reference.fields, &expected)? {
                    return Err(reference.referred(target_key, key.as_deref()));
                }
            }
        }
        Ok(())
    }

    /// Whether a row's `fields` equal `expected`, by the durable index, as
    /// staged: Some with its key when readable, None if there is none.
    fn first_indexed(
        &mut self,
        collection: &str,
        fields: &[String],
        expected: &Value,
    ) -> EngineResult<Option<Option<String>>> {
        let scalar = ranges::equality_position(fields.len(), expected);
        let prefix = match &scalar {
            Some(components) => format!("{}{components}", ranges::prefix(collection, fields)),
            None => indexes::bucket_prefix(collection, fields, &canonical_json(expected)),
        };
        let found = self
            .staged
            .range_shared::<(Bound<&str>, Bound<&str>)>((
                Bound::Included(prefix.as_str()),
                Bound::Unbounded,
            ))
            .next()
            .filter(|(id, _)| id.starts_with(&prefix))
            .map(|(id, _)| id[prefix.len()..].to_owned());
        Ok(found.map(|suffix| match scalar {
            Some(_) => ranges::text_restored(&suffix),
            None => serde_json::from_str::<String>(&suffix).ok(),
        }))
    }

    /// A deployment that declares references checks the rows already there:
    /// every row of their collections refers to a row that exists. Only
    /// references `previous` lacked: the others held all along.
    pub(super) fn validate_new_references(&mut self, previous: &Schema) -> EngineResult<()> {
        let added: Vec<ReferenceSpec> = self
            .schema
            .references
            .iter()
            .filter(|reference| !previous.references.contains(reference))
            .cloned()
            .collect();
        for reference in added {
            let prefix = format!(
                "source:[{},",
                serde_json::to_string(&reference.collection).expect("collection encodes")
            );
            let snapshot = self.staged.clone();
            for (position, (id, value)) in snapshot
                .range_shared::<(Bound<&str>, Bound<&str>)>((
                    Bound::Included(prefix.as_str()),
                    Bound::Unbounded,
                ))
                .enumerate()
            {
                if !id.starts_with(&prefix) {
                    break;
                }
                if position % 64 == 0 {
                    self.check_fatal()?;
                }
                let (_, key) = source_pair(id)?;
                let named = reference.named(&key, value);
                self.check_target(&reference, &key, named)?;
            }
        }
        Ok(())
    }
}
