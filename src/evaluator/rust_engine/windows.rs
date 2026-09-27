//! The windows of ordered positions that derived scans depend on.
//!
//! A scan's result depends only on the index entries up to the last one it
//! examined, and on the values of the rows it returned. Each scan dependency
//! therefore records two half-open windows of positions within one collection
//! index: membership (entries entering, leaving or moving) and values (rows
//! returned). Positions are ordered-entry IDs after their index prefix; see
//! `ranges::position`. A write stabs the windows of its row's old and new
//! positions, so an unrelated write never re-runs the derivation.
use super::*;
use std::hash::BuildHasher;
use std::sync::OnceLock;

pub(super) const PREFIX: &str = "scan:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Window {
    pub collection: String,
    /// `None` for source-key scans, which order rows by key.
    pub fields: Option<Vec<String>>,
    pub lower: String,
    pub upper: String,
    /// The returned rows' positions, when any row was returned.
    pub values: Option<(String, String)>,
}

impl Window {
    pub(super) fn is_dependency(dependency: &str) -> bool {
        dependency.starts_with(PREFIX)
    }

    pub(super) fn dependency(&self) -> String {
        let values = self
            .values
            .as_ref()
            .map(|(lower, upper)| json!([lower, upper]));
        format!(
            "{PREFIX}{}",
            canonical_json(&json!([
                self.collection,
                self.fields,
                self.lower,
                self.upper,
                values
            ]))
        )
    }

    /// Stored dependencies are trusted evaluator output; anything else is
    /// malformed rather than a weaker dependency.
    pub(super) fn parse(dependency: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(dependency.strip_prefix(PREFIX)?).ok()?;
        let [collection, fields, lower, upper, values] = value.as_array()?.as_slice() else {
            return None;
        };
        let fields = match fields {
            Value::Null => None,
            Value::Array(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        field
                            .as_str()
                            .filter(|field| !field.is_empty())
                            .map(str::to_owned)
                    })
                    .collect::<Option<Vec<_>>>()?;
                if fields.is_empty() || fields.iter().collect::<HashSet<_>>().len() != fields.len()
                {
                    return None;
                }
                Some(fields)
            }
            _ => return None,
        };
        let text = |value: &Value| value.as_str().map(str::to_owned);
        let window = Self {
            collection: text(collection).filter(|collection| !collection.is_empty())?,
            fields,
            lower: text(lower)?,
            upper: text(upper)?,
            values: match values {
                Value::Null => None,
                Value::Array(bounds) => match bounds.as_slice() {
                    [lower, upper] => Some((text(lower)?, text(upper)?)),
                    _ => return None,
                },
                _ => return None,
            },
        };
        let ordered = window.lower < window.upper
            && window.values.as_ref().is_none_or(|(lower, upper)| {
                window.lower <= *lower && lower < upper && *upper <= window.upper
            });
        (ordered && window.dependency() == dependency).then_some(window)
    }

    /// The bucket this window stays within, if it stays within one: the
    /// first field's token of its lower bound, when the upper bound is at
    /// most that token's successor. Every position it holds then starts with
    /// the token, so no write outside that bucket can reach it.
    pub(super) fn bucket(&self) -> Option<&str> {
        self.fields.as_ref()?;
        let bucket = bucket(&self.lower)?;
        (self.upper.as_str() <= successor(bucket).as_str()).then_some(bucket)
    }

    /// What the dependency of every window of `bucket`, in the index of
    /// `fields` on `collection`, starts with. The lower bound comes third;
    /// escaping it escapes its first token alike.
    pub(super) fn bucket_dependency(collection: &str, fields: &[String], bucket: &str) -> String {
        let index = canonical_json(&json!([collection, fields]));
        let bucket = canonical_json(&Value::String(bucket.into()));
        format!(
            "{PREFIX}{},{}",
            &index[..index.len() - 1],
            &bucket[..bucket.len() - 1]
        )
    }

    /// The coarse marker that the window refines, for certificates that
    /// validate against a snapshot rather than a write.
    pub(super) fn marker(&self, schema: &Schema) -> String {
        match &self.fields {
            Some(fields)
                if schema.indexes.iter().any(|index| {
                    index.collection == self.collection && &index.fields == fields
                }) =>
            {
                ranges::dependency(&self.collection, fields)
            }
            _ => collection_id(&self.collection),
        }
    }
}

/// The first field's token of an ordered position (see `ranges::position`).
pub(super) fn bucket(position: &str) -> Option<&str> {
    position.get(..ranges::component_len(position)?)
}

/// The least string after every string that starts with `token`.
fn successor(token: &str) -> String {
    let mut bytes = token.as_bytes().to_vec();
    // Tokens end in an ASCII digit or terminator, never the last ASCII character.
    *bytes.last_mut().expect("nonempty token") += 1;
    String::from_utf8(bytes).expect("token ends in ASCII")
}

type Link = Option<Arc<Node>>;

/// A persistent treap ordered by lower bound, augmented with each subtree's
/// greatest upper bound, so stabbing visits O(log n) nodes plus its matches.
#[derive(Debug)]
struct Node {
    lower: Arc<str>,
    upper: Arc<str>,
    values: Option<(Arc<str>, Arc<str>)>,
    reader: Arc<str>,
    priority: u64,
    max_upper: Arc<str>,
    left: Link,
    right: Link,
}

type Key<'a> = (&'a str, &'a str, Option<(&'a str, &'a str)>, &'a str);

impl Node {
    fn key(&self) -> Key<'_> {
        (
            &self.lower,
            &self.upper,
            self.values
                .as_ref()
                .map(|(lower, upper)| (&**lower, &**upper)),
            &self.reader,
        )
    }

    fn with(&self, left: Link, right: Link) -> Arc<Self> {
        Self::build(
            self.lower.clone(),
            self.upper.clone(),
            self.values.clone(),
            self.reader.clone(),
            self.priority,
            left,
            right,
        )
    }

    fn build(
        lower: Arc<str>,
        upper: Arc<str>,
        values: Option<(Arc<str>, Arc<str>)>,
        reader: Arc<str>,
        priority: u64,
        left: Link,
        right: Link,
    ) -> Arc<Self> {
        let mut max_upper = upper.clone();
        for child in [&left, &right].into_iter().flatten() {
            if child.max_upper > max_upper {
                max_upper = child.max_upper.clone();
            }
        }
        Arc::new(Self {
            lower,
            upper,
            values,
            reader,
            priority,
            max_upper,
            left,
            right,
        })
    }
}

/// Keys before `key` go left.
fn split(link: &Link, key: Key<'_>) -> (Link, Link) {
    let Some(node) = link else {
        return (None, None);
    };
    if node.key() < key {
        let (left, right) = split(&node.right, key);
        (Some(node.with(node.left.clone(), left)), right)
    } else {
        let (left, right) = split(&node.left, key);
        (left, Some(node.with(right, node.right.clone())))
    }
}

/// Every key in `left` precedes every key in `right`.
fn merge(left: Link, right: Link) -> Link {
    match (left, right) {
        (None, link) | (link, None) => link,
        (Some(left), Some(right)) => Some(if left.priority > right.priority {
            left.with(left.left.clone(), merge(left.right.clone(), Some(right)))
        } else {
            right.with(merge(Some(left), right.left.clone()), right.right.clone())
        }),
    }
}

fn remove(link: &Link, key: Key<'_>) -> Option<Link> {
    let node = link.as_ref()?;
    Some(match key.cmp(&node.key()) {
        std::cmp::Ordering::Equal => merge(node.left.clone(), node.right.clone()),
        std::cmp::Ordering::Less => Some(node.with(remove(&node.left, key)?, node.right.clone())),
        std::cmp::Ordering::Greater => {
            Some(node.with(node.left.clone(), remove(&node.right, key)?))
        }
    })
}

fn stab(link: &Link, position: &str, values: bool, readers: &mut Vec<String>) {
    let Some(node) = link else {
        return;
    };
    if *node.max_upper <= *position {
        return;
    }
    stab(&node.left, position, values, readers);
    // Value windows lie within their membership windows, so both prune alike.
    if *node.lower <= *position {
        let bounds = if values {
            node.values.as_ref().map(|(lower, upper)| (lower, upper))
        } else {
            Some((&node.lower, &node.upper))
        };
        if let Some((lower, upper)) = bounds
            && **lower <= *position
            && *position < **upper
        {
            readers.push(node.reader.to_string());
        }
        stab(&node.right, position, values, readers);
    }
}

fn priority(key: Key<'_>) -> u64 {
    // Per-process random priorities keep the expected depth logarithmic no
    // matter which bounds applications choose.
    static STATE: OnceLock<std::hash::RandomState> = OnceLock::new();
    STATE.get_or_init(std::hash::RandomState::new).hash_one(key)
}

/// The windows of one collection index, by the cells that read them.
#[derive(Clone, Debug, Default)]
pub(super) struct Windows {
    root: Link,
}

impl Windows {
    pub(super) fn insert(&mut self, window: &Window, reader: &Arc<str>) {
        let lower: Arc<str> = window.lower.as_str().into();
        let upper: Arc<str> = window.upper.as_str().into();
        let values = window.values.as_ref().map(|(lower, upper)| {
            (
                Arc::<str>::from(lower.as_str()),
                Arc::<str>::from(upper.as_str()),
            )
        });
        let key = (
            &*lower,
            &*upper,
            values.as_ref().map(|(lower, upper)| (&**lower, &**upper)),
            &**reader,
        );
        let priority = priority(key);
        let (left, right) = split(&self.root, key);
        let node = Node::build(
            lower.clone(),
            upper.clone(),
            values.clone(),
            reader.clone(),
            priority,
            None,
            None,
        );
        self.root = merge(merge(left, Some(node)), right);
    }

    pub(super) fn remove(&mut self, window: &Window, reader: &str) {
        let key = (
            window.lower.as_str(),
            window.upper.as_str(),
            window
                .values
                .as_ref()
                .map(|(lower, upper)| (lower.as_str(), upper.as_str())),
            reader,
        );
        if let Some(root) = remove(&self.root, key) {
            self.root = root;
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Readers whose result can change when a row enters, leaves or moves to
    /// `position`, or when only the value at `position` changes.
    pub(super) fn readers(&self, position: &str, values: bool, readers: &mut Vec<String>) {
        stab(&self.root, position, values, readers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(lower: &str, upper: &str, values: Option<(&str, &str)>) -> Window {
        Window {
            collection: "items".into(),
            fields: None,
            lower: lower.into(),
            upper: upper.into(),
            values: values.map(|(lower, upper)| (lower.into(), upper.into())),
        }
    }

    fn naive(windows: &[(Window, String)], position: &str, values: bool) -> Vec<String> {
        let mut readers: Vec<_> = windows
            .iter()
            .filter(|(window, _)| {
                let (lower, upper) = if values {
                    match &window.values {
                        Some((lower, upper)) => (lower.as_str(), upper.as_str()),
                        None => return false,
                    }
                } else {
                    (window.lower.as_str(), window.upper.as_str())
                };
                lower <= position && position < upper
            })
            .map(|(_, reader)| reader.clone())
            .collect();
        readers.sort();
        readers
    }

    #[test]
    fn stabbing_matches_a_linear_scan_through_inserts_removals_and_snapshots() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let point = |n: u64| format!("{n:03}");
        let mut tree = Windows::default();
        let mut live: Vec<(Window, String)> = Vec::new();
        let mut snapshots = Vec::new();
        for step in 0..3000 {
            if live.is_empty() || next(3) != 0 {
                let lower = next(200);
                let upper = lower + 1 + next(40);
                let values = (next(2) == 0).then(|| {
                    let from = lower + next(upper - lower);
                    (point(from), point(from + 1 + next(upper - from)))
                });
                let window = window(
                    &point(lower),
                    &point(upper),
                    values
                        .as_ref()
                        .map(|(lower, upper)| (lower.as_str(), upper.as_str())),
                );
                let reader = format!("cell:{}", next(50));
                if live
                    .iter()
                    .any(|entry| entry == &(window.clone(), reader.clone()))
                {
                    continue;
                }
                tree.insert(&window, &Arc::from(reader.as_str()));
                live.push((window, reader));
            } else {
                let (window, reader) = live.swap_remove(next(live.len() as u64) as usize);
                tree.remove(&window, &reader);
            }
            if step % 500 == 0 {
                snapshots.push((tree.clone(), live.clone()));
            }
            let position = point(next(260));
            for values in [false, true] {
                let mut actual = Vec::new();
                tree.readers(&position, values, &mut actual);
                actual.sort();
                assert_eq!(
                    actual,
                    naive(&live, &position, values),
                    "{position} {values}"
                );
            }
        }
        // Earlier snapshots are unaffected by later edits.
        for (tree, live) in snapshots {
            for n in 0..260 {
                let mut actual = Vec::new();
                tree.readers(&point(n), false, &mut actual);
                actual.sort();
                assert_eq!(actual, naive(&live, &point(n), false));
            }
        }
        for (window, reader) in live {
            tree.remove(&window, &reader);
        }
        assert!(tree.is_empty());
    }

    #[test]
    fn dependencies_round_trip_and_reject_malformed_windows() {
        let mut window = window("3a!", "3c!", Some(("3a!", "3b!\0")));
        window.fields = Some(vec!["tenant".into(), "score".into()]);
        assert_eq!(Window::parse(&window.dependency()), Some(window.clone()));
        let keyed = Window {
            fields: None,
            values: None,
            ..window.clone()
        };
        assert_eq!(Window::parse(&keyed.dependency()), Some(keyed));
        for malformed in [
            r#"scan:["items",null,"3b","3a",null]"#,
            r#"scan:["items",null,"3a","3a",null]"#,
            r#"scan:["items",null,"3a","3c",["3b","3d"]]"#,
            r#"scan:["items",null,"3a","3c",["3b","3b"]]"#,
            r#"scan:["items",[],"3a","3c",null]"#,
            r#"scan:["items",["a","a"],"3a","3c",null]"#,
            r#"scan:["",null,"3a","3c",null]"#,
            r#"scan:["items",null,"3a","3c"]"#,
            r#"scan:["items", null,"3a","3c",null]"#,
            "scan:{}",
            r#"collection:"items""#,
        ] {
            assert_eq!(Window::parse(malformed), None, "{malformed}");
        }
    }
}
