//! Bounded RFC 6902 add/remove/replace diff. Values remain borrowed until the
//! event is encoded; no application code or unbounded LCS runs in this path.
use serde::Serialize;
use serde_json::Value;

pub(super) const MAX_OPERATIONS: usize = 256;
const MAX_WORK: usize = 100_000;
const MAX_PATH_BYTES: usize = 16 * 1024;
const MAX_TOTAL_PATH_BYTES: usize = 64 * 1024;
const MAX_TRAVERSED_PATH_BYTES: usize = 1024 * 1024;

#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub(super) enum Operation<'a> {
    Add { path: String, value: &'a Value },
    Remove { path: String },
    Replace { path: String, value: &'a Value },
}

#[derive(Default)]
struct Diff<'a> {
    operations: Vec<Operation<'a>>,
    work: usize,
    path_bytes: usize,
    traversed_path_bytes: usize,
}

impl<'a> Diff<'a> {
    fn step(&mut self) -> Result<(), ()> {
        self.work += 1;
        (self.work <= MAX_WORK).then_some(()).ok_or(())
    }

    fn push(&mut self, op: Operation<'a>) -> Result<(), ()> {
        let path = match &op {
            Operation::Add { path, .. }
            | Operation::Remove { path }
            | Operation::Replace { path, .. } => path,
        };
        self.path_bytes += path.len();
        if self.operations.len() >= MAX_OPERATIONS || self.path_bytes > MAX_TOTAL_PATH_BYTES {
            return Err(());
        }
        self.operations.push(op);
        Ok(())
    }

    fn path(&mut self, parent: &str, key: &str) -> Result<String, ()> {
        let path = child(parent, key)?;
        self.traversed_path_bytes += path.len();
        // Shared long prefixes can otherwise cause large repeated allocations
        // even when the resulting patch contains just one changed leaf.
        if self.traversed_path_bytes > MAX_TRAVERSED_PATH_BYTES {
            return Err(());
        }
        Ok(path)
    }

    fn equal(&mut self, left: &Value, right: &Value) -> Result<bool, ()> {
        self.step()?;
        match (left, right) {
            (Value::Array(left), Value::Array(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (left, right) in left.iter().zip(right) {
                    if !self.equal(left, right)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (Value::Object(left), Value::Object(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for ((left_key, left), (right_key, right)) in left.iter().zip(right) {
                    if left_key != right_key || !self.equal(left, right)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(left == right),
        }
    }

    fn walk(&mut self, left: &Value, right: &'a Value, path: &str) -> Result<(), ()> {
        self.step()?;
        match (left, right) {
            (Value::Object(left), Value::Object(right)) => {
                for key in left.keys() {
                    self.step()?;
                    if !right.contains_key(key) {
                        let path = self.path(path, key)?;
                        self.push(Operation::Remove { path })?;
                    }
                }
                for (key, value) in right {
                    let path = self.path(path, key)?;
                    if let Some(previous) = left.get(key) {
                        self.walk(previous, value, &path)?;
                    } else {
                        self.step()?;
                        self.push(Operation::Add { path, value })?;
                    }
                }
            }
            (Value::Array(left), Value::Array(right)) => {
                // Keep common ends, then edit the middle by index. This handles
                // append, truncate and splice without quadratic subsequences.
                let mut prefix = 0;
                while prefix < left.len().min(right.len())
                    && self.equal(&left[prefix], &right[prefix])?
                {
                    prefix += 1;
                }
                let mut suffix = 0;
                while suffix < left.len().min(right.len()) - prefix
                    && self.equal(
                        &left[left.len() - suffix - 1],
                        &right[right.len() - suffix - 1],
                    )?
                {
                    suffix += 1;
                }
                let old_middle = left.len() - prefix - suffix;
                let new_middle = right.len() - prefix - suffix;
                for index in prefix..prefix + old_middle.min(new_middle) {
                    let path = self.path(path, &index.to_string())?;
                    self.walk(&left[index], &right[index], &path)?;
                }
                for index in (prefix + new_middle..prefix + old_middle).rev() {
                    let path = self.path(path, &index.to_string())?;
                    self.push(Operation::Remove { path })?;
                }
                for (index, value) in right
                    .iter()
                    .enumerate()
                    .take(prefix + new_middle)
                    .skip(prefix + old_middle)
                {
                    let path = self.path(path, &index.to_string())?;
                    self.push(Operation::Add { path, value })?;
                }
            }
            _ if left != right => self.push(Operation::Replace {
                path: path.into(),
                value: right,
            })?,
            _ => {}
        }
        Ok(())
    }
}

fn child(parent: &str, key: &str) -> Result<String, ()> {
    // Bound before allocating; one huge key must not be copied into hundreds
    // of operation paths. The escaped pointer is at most twice the key bytes.
    if parent.len() + key.len() + 1 > MAX_PATH_BYTES {
        return Err(());
    }
    let escaped = key.replace('~', "~0").replace('/', "~1");
    if parent.len() + escaped.len() + 1 > MAX_PATH_BYTES {
        return Err(());
    }
    Ok(format!("{parent}/{escaped}"))
}

pub(super) fn diff<'a>(left: &Value, right: &'a Value) -> Option<Vec<Operation<'a>>> {
    let mut diff = Diff::default();
    diff.walk(left, right, "").ok()?;
    Some(diff.operations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn apply(value: &mut Value, operations: &[Operation<'_>]) {
        for operation in operations {
            let (path, replacement) = match operation {
                Operation::Add { path, value } | Operation::Replace { path, value } => {
                    (path, Some(*value))
                }
                Operation::Remove { path } => (path, None),
            };
            if path.is_empty() {
                *value = replacement.unwrap().clone();
                continue;
            }
            let (parent, key) = path.rsplit_once('/').unwrap();
            let key = key.replace("~1", "/").replace("~0", "~");
            match value.pointer_mut(parent).unwrap() {
                Value::Object(object) => {
                    if let Some(value) = replacement {
                        object.insert(key, value.clone());
                    } else {
                        assert!(object.remove(&key).is_some());
                    }
                }
                Value::Array(array) => {
                    let index: usize = key.parse().unwrap();
                    match operation {
                        Operation::Add { value, .. } => array.insert(index, (*value).clone()),
                        Operation::Replace { value, .. } => array[index] = (*value).clone(),
                        Operation::Remove { .. } => {
                            array.remove(index);
                        }
                    }
                }
                _ => panic!("Invalid patch target"),
            }
        }
    }

    #[test]
    fn pointers_and_array_splices_reconstruct_exact_values() {
        for (left, right) in [
            (
                json!({"~/": {"": 1}, "removed": null}),
                json!({"~/": {"": 2}, "added": true}),
            ),
            (json!([1, 2, 3]), json!([1, 2, 3, 4])),
            (json!([1, 2, 3]), json!([1])),
            (json!([1, 2, 3]), json!([1, 8, 9, 2, 3])),
            (json!([1, 8, 9, 2, 3]), json!([1, 2, 3])),
            (json!([{"a": 1}, {"b": 2}]), json!([{"a": 2}, {"b": 2}])),
            (json!(null), json!({"x": true})),
        ] {
            let operations = diff(&left, &right).unwrap();
            let mut actual = left;
            apply(&mut actual, &operations);
            assert_eq!(actual, right);
        }
    }

    #[test]
    fn deterministic_random_arrays_and_objects_round_trip() {
        let mut seed = 12345_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..2000 {
            let left: Vec<_> = (0..next() % 20)
                .map(|_| json!({"value":next() % 8}))
                .collect();
            let right: Vec<_> = (0..next() % 20)
                .map(|_| json!({"value":next() % 8}))
                .collect();
            let mut actual = json!({"array":left,"before":true});
            let target = json!({"array":right,"after":false});
            let operations = diff(&actual, &target).unwrap();
            apply(&mut actual, &operations);
            assert_eq!(actual, target);
        }
    }

    #[test]
    fn excessive_operations_work_and_pointer_bytes_fall_back() {
        assert!(diff(&json!([]), &json!(vec![0; MAX_OPERATIONS + 1])).is_none());
        assert!(diff(&json!(vec![0; MAX_WORK]), &json!(vec![0; MAX_WORK])).is_none());
        let large_key = "x".repeat(MAX_PATH_BYTES + 1);
        assert!(diff(&json!({}), &json!({large_key:1})).is_none());
        let shared_key = "x".repeat(8 * 1024);
        let children: serde_json::Map<_, _> = (0..200)
            .map(|index| (index.to_string(), json!(index)))
            .collect();
        let left = json!({shared_key.clone():children});
        let mut right = left.clone();
        right[&shared_key]["0"] = json!(-1);
        assert!(
            diff(&left, &right).is_none(),
            "traversed paths, not just emitted paths, must be bounded"
        );
    }
}
