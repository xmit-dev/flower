//! Validate a guest's raw manifest (GUEST_ABI.md) into the HTTP registry,
//! maintenance and authorization methods, key declarations and index schema
//! Flower stores. Every guest kind goes through these same checks.
use super::{
    AuthorizationMethod, AuthorizationResult, HttpMethod, MaintenanceMethod, Manifest, MethodKind,
    QueryConsistency,
    rust_engine::{
        IndexSpec, KeyPart, Policy, ReferenceSpec, Rule, Schema, valid_version,
        validate_derived_targets, validate_targets,
    },
};
use anyhow::{Result, anyhow, bail, ensure};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

const ALGORITHMS: [&str; 7] = [
    "Ed25519",
    "P256",
    "RSA",
    "HS256",
    "A256GCM",
    "XSalsa20Poly1305",
    "X25519",
];
const USAGES: [&str; 6] = [
    "sign",
    "verify",
    "encrypt",
    "decrypt",
    "derive",
    "publicKey",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Derived,
    Method(MethodKind),
}

struct Definition<'a> {
    kind: Kind,
    consistency: QueryConsistency,
    receipt: bool,
    aggregate: Option<&'a Value>,
    access: Option<&'a Value>,
}

fn object<'a>(value: Option<&'a Value>, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("{label} must be a plain object"))
}

fn only(entries: &Map<String, Value>, allowed: &[&str]) -> bool {
    entries.keys().all(|key| allowed.contains(&key.as_str()))
}

fn text<'a>(entries: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    entries.get(key).and_then(Value::as_str)
}

fn method_kind(value: Option<&str>) -> Option<MethodKind> {
    match value? {
        "query" => Some(MethodKind::Query),
        "mutation" => Some(MethodKind::Mutation),
        "transaction" => Some(MethodKind::Transaction),
        _ => None,
    }
}

fn consistency(entries: &Map<String, Value>, query: bool) -> Result<QueryConsistency> {
    match (entries.get("consistency"), query) {
        (None, _) => Ok(QueryConsistency::Linearizable),
        (Some(value), true) if value == "linearizable" => Ok(QueryConsistency::Linearizable),
        (Some(value), true) if value == "replica-local" => Ok(QueryConsistency::ReplicaLocal),
        _ => bail!("consistency requires a query method and linearizable or replica-local"),
    }
}

/// Whether a mutation keeps retry receipts: only `receipt: false` gives them up.
fn receipt(entries: &Map<String, Value>, mutation: bool) -> Result<bool> {
    match (entries.get("receipt"), mutation) {
        (None, _) => Ok(true),
        (Some(Value::Bool(receipt)), true) => Ok(*receipt),
        _ => bail!("receipt requires a mutation method and a boolean"),
    }
}

fn index_fields(value: Option<&Value>) -> Result<Vec<String>> {
    let fields: Option<Vec<String>> = value.and_then(Value::as_array).and_then(|fields| {
        fields
            .iter()
            .map(|field| {
                field
                    .as_str()
                    .filter(|field| !field.is_empty())
                    .map(str::to_owned)
            })
            .collect()
    });
    match fields {
        Some(fields)
            if !fields.is_empty()
                && fields.iter().collect::<BTreeSet<_>>().len() == fields.len() =>
        {
            Ok(fields)
        }
        _ => bail!("Index fields must be distinct nonempty strings"),
    }
}

/// A collection's `references`: `{target, fields | key, json?}` each, where
/// `key` is true (the whole key) or how many leading components of it.
fn references(collection: &str, value: Option<&Value>) -> Result<Vec<ReferenceSpec>> {
    let references = match value {
        None => return Ok(Vec::new()),
        Some(Value::Array(references)) => references,
        Some(_) => bail!("Collection references must be an array"),
    };
    references
        .iter()
        .map(|value| {
            let entry = object(Some(value), "Reference")?;
            ensure!(
                only(entry, &["target", "fields", "key", "json"]),
                "A reference of {collection} has unknown fields"
            );
            let target = text(entry, "target").filter(|target| !target.is_empty());
            let json = match entry.get("json") {
                None => false,
                Some(Value::Bool(json)) => *json,
                Some(_) => bail!("A reference's json is a boolean"),
            };
            let key = match entry.get("key") {
                None => None,
                Some(Value::Bool(true)) => Some(KeyPart::Whole(true)),
                Some(value) => Some(KeyPart::Leading(
                    value
                        .as_u64()
                        .filter(|count| (1..=64).contains(count))
                        .map(|count| count as usize)
                        .ok_or_else(|| {
                            anyhow!("A reference's key is true or a count of leading components")
                        })?,
                )),
            };
            let fields = match entry.get("fields") {
                None => Vec::new(),
                fields => index_fields(fields)?,
            };
            let reference = ReferenceSpec {
                collection: collection.to_owned(),
                target: target
                    .ok_or_else(|| anyhow!("A reference of {collection} names its target"))?
                    .to_owned(),
                fields,
                key,
                json,
            };
            reference
                .validate()
                .map_err(|error| anyhow!("Invalid reference of {collection}: {error}"))?;
            Ok(reference)
        })
        .collect()
}

fn keys(value: Option<&Value>) -> Result<Vec<Value>> {
    let keys = match value {
        None => return Ok(Vec::new()),
        Some(Value::Array(keys)) => keys,
        Some(_) => bail!("keys must be an array"),
    };
    let mut names = BTreeSet::new();
    for value in keys {
        let key = object(Some(value), "Key declaration")?;
        let usages = key.get("usages").and_then(Value::as_array);
        let valid = key.len() == 4
            && only(key, &["kind", "name", "algorithm", "usages"])
            && text(key, "kind") == Some("key")
            && text(key, "name").is_some_and(|name| !name.is_empty() && names.insert(name))
            && text(key, "algorithm").is_some_and(|algorithm| ALGORITHMS.contains(&algorithm))
            && usages.is_some_and(|usages| {
                let distinct: Option<BTreeSet<&str>> = usages
                    .iter()
                    .map(|usage| usage.as_str().filter(|usage| USAGES.contains(usage)))
                    .collect();
                !usages.is_empty()
                    && distinct.is_some_and(|distinct| distinct.len() == usages.len())
            });
        ensure!(valid, "Invalid or duplicate key declaration");
    }
    Ok(keys.clone())
}

fn schema(app: &Map<String, Value>, definitions: &BTreeMap<&str, Definition>) -> Result<Schema> {
    let collections = match app.get("collections") {
        None => &[][..],
        Some(Value::Array(collections)) => collections,
        Some(_) => bail!("collections must be an array"),
    };
    let (mut indexes, mut identities, mut declared) =
        (Vec::new(), BTreeSet::new(), BTreeSet::new());
    let mut policies = BTreeMap::new();
    let mut declared_references = Vec::new();
    for value in collections {
        let collection = object(Some(value), "Collection declaration")?;
        let name = text(collection, "name");
        let Some(name) = name.filter(|name| {
            only(collection, &["name", "indexes", "access", "references"])
                && !name.is_empty()
                && declared.insert(*name)
        }) else {
            bail!("Invalid or duplicate collection declaration");
        };
        if let Some(access) = collection.get("access") {
            let policy = serde_json::from_value::<Policy>(access.clone())
                .map_err(|error| error.to_string())
                .and_then(|policy| policy.validate().map(|()| policy))
                .map_err(|error| anyhow!("Invalid access policy for collection {name}: {error}"))?;
            policies.insert(name.to_owned(), policy);
        }
        for (index, fields) in object(collection.get("indexes"), "Collection indexes")? {
            ensure!(!index.is_empty(), "Index names must be nonempty");
            let fields = index_fields(Some(fields))?;
            if identities.insert((name.to_owned(), fields.clone())) {
                indexes.push(IndexSpec {
                    collection: name.to_owned(),
                    fields,
                });
            }
        }
        // A reference held by fields finds its rows through an index of them.
        for reference in references(name, collection.get("references"))? {
            if let Some(index) = reference.index()
                && identities.insert((index.collection.clone(), index.fields.clone()))
            {
                indexes.push(index);
            }
            declared_references.push(reference);
        }
    }
    validate_targets(&policies).map_err(|error| anyhow!("Invalid access policy: {error}"))?;
    let mut aggregates = BTreeMap::new();
    let mut aggregate_versions = BTreeMap::new();
    for (name, definition) in definitions {
        let Some(aggregate) = definition.aggregate else {
            continue;
        };
        let metadata = object(Some(aggregate), "Aggregate metadata")?;
        let collection = text(metadata, "collection").filter(|collection| {
            definition.kind == Kind::Derived
                && only(metadata, &["collection", "fields", "version"])
                && !collection.is_empty()
        });
        let Some(collection) = collection else {
            bail!("Aggregate metadata requires a derived definition, collection, and fields");
        };
        let fields = index_fields(metadata.get("fields"))?;
        match metadata.get("version") {
            None => {}
            Some(Value::String(version)) if valid_version(version) => {
                aggregate_versions.insert((*name).to_owned(), version.clone());
            }
            Some(_) => bail!("Aggregate version must be a string of 1 to 128 characters: {name}"),
        }
        let identity = (collection.to_owned(), fields);
        ensure!(
            identities.contains(&identity),
            "Aggregate index must be declared in define({{collections}})"
        );
        aggregates.insert(
            (*name).to_owned(),
            IndexSpec {
                collection: identity.0,
                fields: identity.1,
            },
        );
    }
    let mut derived_access = BTreeMap::new();
    for (name, definition) in definitions {
        let Some(access) = definition.access else {
            continue;
        };
        ensure!(
            definition.kind == Kind::Derived,
            "Only derived definitions declare access: {name}"
        );
        let rule = serde_json::from_value::<Rule>(access.clone())
            .map_err(|error| error.to_string())
            .and_then(|rule| rule.validate_derived().map(|()| rule))
            .map_err(|error| anyhow!("Invalid access rule for derived {name}: {error}"))?;
        derived_access.insert((*name).to_owned(), rule);
    }
    validate_derived_targets(&derived_access, &policies)
        .map_err(|error| anyhow!("Invalid access rule for derived {error}"))?;
    declared_references.sort();
    declared_references.dedup();
    Ok(Schema {
        indexes,
        aggregates,
        aggregate_versions,
        policies,
        derived_access,
        references: declared_references,
    })
}

pub(super) fn validate(raw: &Value) -> Result<Manifest> {
    let app = object(Some(raw), "application")?;
    ensure!(
        only(
            app,
            &[
                "definitions",
                "http",
                "maintenance",
                "collections",
                "keys",
                "authorize"
            ]
        ),
        "Use define({{definitions, http, maintenance, collections, keys, authorize}}) to declare the application"
    );
    let keys = keys(app.get("keys"))?;
    let mut definitions = BTreeMap::new();
    for (name, value) in object(app.get("definitions"), "definitions")? {
        let definition = object(Some(value), "definition")?;
        let kind = match text(definition, "kind") {
            Some("derived") => Some(Kind::Derived),
            kind => method_kind(kind).map(Kind::Method),
        };
        let Some(kind) = kind.filter(|_| {
            !name.is_empty()
                && only(
                    definition,
                    &["kind", "consistency", "receipt", "aggregate", "access"],
                )
        }) else {
            bail!("Invalid definition: {name}");
        };
        let consistency = consistency(definition, kind == Kind::Method(MethodKind::Query))?;
        let receipt = receipt(definition, kind == Kind::Method(MethodKind::Mutation))?;
        definitions.insert(
            name.as_str(),
            Definition {
                kind,
                consistency,
                receipt,
                aggregate: definition.get("aggregate"),
                access: definition.get("access"),
            },
        );
    }
    let definition = |name: Option<&str>, kind: MethodKind| {
        name.and_then(|name| definitions.get(name))
            .filter(|definition| definition.kind == Kind::Method(kind))
    };

    let mut http = BTreeMap::new();
    for (alias, value) in object(app.get("http"), "http")? {
        let method = object(Some(value), "HTTP method")?;
        let name = text(method, "name");
        let kind = method_kind(text(method, "kind"));
        let target = kind.and_then(|kind| definition(name, kind));
        let (Some(name), Some(kind), Some(target)) = (name, kind, target) else {
            bail!("Invalid HTTP method mapping: {alias}");
        };
        ensure!(
            !alias.is_empty() && only(method, &["name", "kind", "consistency", "receipt"]),
            "Invalid HTTP method mapping: {alias}"
        );
        let mode = consistency(method, kind == MethodKind::Query)?;
        ensure!(
            mode == target.consistency,
            "HTTP consistency must match its query definition: {alias}"
        );
        let receipt = receipt(method, kind == MethodKind::Mutation)?;
        ensure!(
            receipt == target.receipt,
            "HTTP receipt must match its mutation definition: {alias}"
        );
        http.insert(
            alias.clone(),
            HttpMethod {
                name: name.to_owned(),
                kind,
                consistency: mode,
                receipt,
            },
        );
    }

    let maintenance = match app.get("maintenance") {
        None | Some(Value::Null) => None,
        method => {
            let method = object(method, "maintenance method")?;
            let name = text(method, "name");
            ensure!(
                only(method, &["name", "kind", "onError"])
                    && text(method, "kind") == Some("mutation")
                    && definition(name, MethodKind::Mutation).is_some(),
                "Maintenance must reference a mutation method"
            );
            let on_error = match method.get("onError") {
                None => None,
                handler => {
                    let handler = object(handler, "maintenance error method")?;
                    let name = text(handler, "name");
                    ensure!(
                        only(handler, &["name", "kind"])
                            && text(handler, "kind") == Some("mutation")
                            && definition(name, MethodKind::Mutation).is_some(),
                        "Maintenance onError must reference a mutation method"
                    );
                    Some(HttpMethod {
                        name: name.unwrap_or_default().to_owned(),
                        kind: MethodKind::Mutation,
                        consistency: QueryConsistency::Linearizable,
                        receipt: true,
                    })
                }
            };
            Some(MaintenanceMethod {
                name: name.unwrap_or_default().to_owned(),
                kind: MethodKind::Mutation,
                on_error,
            })
        }
    };

    let authorize = match app.get("authorize") {
        None | Some(Value::Null) => None,
        method => {
            let method = object(method, "authorization method")?;
            let name = text(method, "name");
            // "result": "decision": the hook reports whether it read the arguments.
            let result = match method.get("result") {
                None => None,
                Some(value) if value == "decision" => Some(AuthorizationResult::Decision),
                Some(_) => bail!(r#"Authorization result must be "decision""#),
            };
            ensure!(
                method.len() == 1 + usize::from(result.is_some())
                    && definition(name, MethodKind::Query).is_some_and(|definition| {
                        definition.consistency == QueryConsistency::Linearizable
                    }),
                "Authorization must reference a fresh read-only query method"
            );
            Some(AuthorizationMethod {
                name: name.unwrap_or_default().to_owned(),
                result,
            })
        }
    };

    Ok(Manifest {
        schema: schema(app, &definitions)?,
        http,
        maintenance,
        authorize,
        keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn error(raw: Value) -> String {
        validate(&raw).unwrap_err().to_string()
    }

    #[test]
    fn valid_manifests_produce_the_stored_registry() {
        let manifest = validate(&json!({
            "definitions": {
                "list": {"kind": "query"},
                "local": {"kind": "query", "consistency": "replica-local"},
                "save": {"kind": "mutation"},
                "tidy": {"kind": "mutation"},
                "plan": {"kind": "transaction"},
                "total": {"kind": "derived", "aggregate": {"collection": "orders", "fields": ["store"]}}
            },
            "http": {
                "list": {"name": "list", "kind": "query"},
                "near": {"name": "local", "kind": "query", "consistency": "replica-local"},
                "save": {"name": "save", "kind": "mutation"},
                "plan": {"name": "plan", "kind": "transaction"}
            },
            "maintenance": {"name": "tidy", "kind": "mutation", "onError": {"name": "save", "kind": "mutation"}},
            "authorize": {"name": "list"},
            "collections": [{"name": "orders", "indexes": {"byStore": ["store"], "again": ["store"]}}],
            "keys": [{"kind": "key", "name": "signing", "algorithm": "Ed25519", "usages": ["sign", "verify"]}]
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&manifest.http).unwrap(),
            json!({
                "list": {"name": "list", "kind": "query"},
                "near": {"name": "local", "kind": "query", "consistency": "replica-local"},
                "plan": {"name": "plan", "kind": "transaction"},
                "save": {"name": "save", "kind": "mutation"}
            })
        );
        assert_eq!(
            serde_json::to_value(&manifest.maintenance).unwrap(),
            json!({"name": "tidy", "kind": "mutation", "onError": {"name": "save", "kind": "mutation"}})
        );
        let authorize = manifest.authorize.unwrap();
        assert_eq!((authorize.name.as_str(), authorize.result), ("list", None));
        // A hook may return decisions, saying whether it read the arguments.
        let decisions = validate(&json!({
            "definitions": {"list": {"kind": "query"}},
            "http": {},
            "authorize": {"name": "list", "result": "decision"}
        }))
        .unwrap()
        .authorize
        .unwrap();
        assert_eq!(decisions.result, Some(AuthorizationResult::Decision));
        assert_eq!(
            serde_json::to_value(&decisions).unwrap(),
            json!({"name": "list", "result": "decision"})
        );
        assert_eq!(
            serde_json::to_value(&manifest.schema).unwrap(),
            json!({
                "indexes": [{"collection": "orders", "fields": ["store"]}],
                "aggregates": {"total": {"collection": "orders", "fields": ["store"]}}
            })
        );
        assert_eq!(manifest.keys.len(), 1);
        // A mutation may give up receipts; its alias must agree.
        let unreceipted = validate(&json!({
            "definitions": {"stream": {"kind": "mutation", "receipt": false}},
            "http": {"stream": {"name": "stream", "kind": "mutation", "receipt": false}}
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&unreceipted.http).unwrap(),
            json!({"stream": {"name": "stream", "kind": "mutation", "receipt": false}})
        );
        for (definition, alias) in [
            (
                json!({"kind": "mutation", "receipt": false}),
                json!({"name": "m", "kind": "mutation"}),
            ),
            (
                json!({"kind": "query", "receipt": false}),
                json!({"name": "m", "kind": "query", "receipt": false}),
            ),
            (
                json!({"kind": "mutation", "receipt": "no"}),
                json!({"name": "m", "kind": "mutation", "receipt": "no"}),
            ),
        ] {
            assert!(
                validate(&json!({"definitions": {"m": definition}, "http": {"m": alias}})).is_err()
            );
        }
        let minimal = validate(&json!({"definitions": {}, "http": {}})).unwrap();
        assert!(
            minimal.http.is_empty() && minimal.maintenance.is_none() && minimal.keys.is_empty()
        );
    }

    #[test]
    fn derived_access_rules_reach_the_schema() {
        let rule = json!({"eq": [{"ref": ["args"]}, {"ref": ["principal", "subject"]}]});
        let manifest = validate(&json!({
            "definitions": {"d": {"kind": "derived", "access": rule}},
            "http": {},
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&manifest.schema.derived_access["d"]).unwrap(),
            rule
        );
        // Aggregates (and SDK externals, plain derived values) carry theirs the same way.
        let manifest = validate(&json!({
            "definitions": {"sum": {"kind": "derived", "access": rule,
                "aggregate": {"collection": "orders", "fields": ["shop"]}}},
            "http": {},
            "collections": [{"name": "orders", "indexes": {"shop": ["shop"]}}],
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&manifest.schema.derived_access["sum"]).unwrap(),
            rule
        );
        assert!(manifest.schema.aggregates.contains_key("sum"));
        assert!(manifest.schema.aggregate_versions.is_empty());
    }

    #[test]
    fn aggregate_versions_reach_the_schema() {
        let manifest = validate(&json!({
            "definitions": {
                "sum": {"kind": "derived", "aggregate": {"collection": "orders", "fields": ["shop"], "version": "2"}},
                "count": {"kind": "derived", "aggregate": {"collection": "orders", "fields": ["shop"]}},
            },
            "http": {},
            "collections": [{"name": "orders", "indexes": {"shop": ["shop"]}}],
        }))
        .unwrap();
        assert_eq!(
            manifest.schema.aggregate_versions,
            BTreeMap::from([("sum".to_owned(), "2".to_owned())])
        );
        // Stored and loaded back as the schema record.
        let stored = serde_json::to_value(&manifest.schema).unwrap();
        assert_eq!(stored["aggregate_versions"], json!({"sum": "2"}));
        assert_eq!(
            serde_json::from_value::<Schema>(stored).unwrap(),
            manifest.schema
        );
    }

    #[test]
    fn invalid_manifests_fail_with_the_declaration_at_fault() {
        let definitions =
            json!({"q": {"kind": "query"}, "m": {"kind": "mutation"}, "d": {"kind": "derived"}});
        for (raw, message) in [
            (json!([]), "application must be a plain object"),
            (
                json!({"definitions": {}, "http": {}, "extra": 1}),
                "Use define(",
            ),
            (json!({"http": {}}), "definitions must be a plain object"),
            (json!({"definitions": {}}), "http must be a plain object"),
            (
                json!({"definitions": {"x": {"kind": "queryMethod"}}, "http": {}}),
                "Invalid definition: x",
            ),
            (
                json!({"definitions": {"x": {"kind": "query", "extra": 1}}, "http": {}}),
                "Invalid definition: x",
            ),
            (
                json!({"definitions": {"x": {"kind": "mutation", "consistency": "replica-local"}}, "http": {}}),
                "consistency requires",
            ),
            (
                json!({"definitions": definitions, "http": {"a": {"name": "m", "kind": "query"}}}),
                "Invalid HTTP method mapping: a",
            ),
            (
                json!({"definitions": definitions, "http": {"a": {"name": "missing", "kind": "query"}}}),
                "Invalid HTTP method mapping: a",
            ),
            (
                json!({"definitions": definitions, "http": {"a": {"name": "d", "kind": "query"}}}),
                "Invalid HTTP method mapping: a",
            ),
            (
                json!({"definitions": definitions, "http": {"": {"name": "q", "kind": "query"}}}),
                "Invalid HTTP method mapping: ",
            ),
            (
                json!({"definitions": definitions, "http": {"a": {"name": "q", "kind": "query", "consistency": "replica-local"}}}),
                "HTTP consistency must match",
            ),
            (
                json!({"definitions": definitions, "http": {}, "maintenance": {"name": "q", "kind": "mutation"}}),
                "Maintenance must reference",
            ),
            (
                json!({"definitions": definitions, "http": {}, "maintenance": {"name": "m", "kind": "mutation", "onError": null}}),
                "maintenance error method must be a plain object",
            ),
            (
                json!({"definitions": definitions, "http": {}, "maintenance": {"name": "m", "kind": "mutation", "onError": {"name": "q", "kind": "mutation"}}}),
                "Maintenance onError must reference",
            ),
            (
                json!({"definitions": definitions, "http": {}, "authorize": {"name": "m"}}),
                "Authorization must reference",
            ),
            (
                json!({"definitions": definitions, "http": {}, "authorize": {"name": "q", "result": "principal"}}),
                "Authorization result must be",
            ),
            (
                json!({"definitions": definitions, "http": {}, "authorize": {"name": "q", "extra": true}}),
                "Authorization must reference",
            ),
            (
                json!({"definitions": definitions, "http": {}, "keys": {}}),
                "keys must be an array",
            ),
            (
                json!({"definitions": definitions, "http": {}, "keys": [{"kind": "key", "name": "k", "algorithm": "Ed25519", "usages": ["sign", "sign"]}]}),
                "Invalid or duplicate key declaration",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": {}}),
                "collections must be an array",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a", "indexes": {}}, {"name": "a", "indexes": {}}]}),
                "Invalid or duplicate collection declaration",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a"}]}),
                "Collection indexes must be a plain object",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a", "indexes": {"i": []}}]}),
                "Index fields must be distinct nonempty strings",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a", "indexes": {}, "access": {"read": {"exists": {"ref": ["next", "x"]}}}}]}),
                "Invalid access policy for collection a: read: next.x cannot be used here",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a", "indexes": {}, "access": {"read": true}}]}),
                "Invalid access policy for collection a",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a", "indexes": {}, "access": {"read": {"readable": ["b", {"ref": ["key"]}]}}}, {"name": "b", "indexes": {}}]}),
                "Invalid access policy: a: readable(\"b\") needs a collection with an access policy",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [
                    {"name": "a", "indexes": {}, "access": {"read": {"readable": ["b", {"ref": ["key"]}]}}},
                    {"name": "b", "indexes": {}, "access": {"read": {"readable": ["c", {"ref": ["key"]}]}}},
                    {"name": "c", "indexes": {}, "access": {"read": {"const": true}}},
                ]}),
                "Invalid access policy: a: readable(\"b\") names a collection whose read rule uses readable itself",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a", "indexes": {}, "access": {"read": {"readable": ["", {"ref": ["key"]}]}}}]}),
                "Invalid access policy for collection a: read: readable needs a collection name",
            ),
            (
                json!({"definitions": definitions, "http": {}, "collections": [{"name": "a", "indexes": {}, "access": {"insert": {"readable": ["a", {"ref": ["row", "x"]}]}}}]}),
                "Invalid access policy for collection a: insert: row.x cannot be used here",
            ),
            (
                json!({"definitions": {"d": {"kind": "derived", "access": {"readable": ["a", {"ref": ["args"]}]}}}, "http": {}}),
                "Invalid access rule for derived d: readable(\"a\") needs a collection with an access policy",
            ),
            (
                json!({"definitions": {"d": {"kind": "derived", "access": {"readable": ["a", {"ref": ["args"]}]}}}, "http": {}, "collections": [
                    {"name": "a", "indexes": {}, "access": {"read": {"readable": ["b", {"ref": ["key"]}]}}},
                    {"name": "b", "indexes": {}, "access": {"read": {"const": true}}},
                ]}),
                "Invalid access rule for derived d: readable(\"a\") names a collection whose read rule uses readable itself",
            ),
            (
                json!({"definitions": {"d": {"kind": "derived", "access": {"readable": ["a", {"ref": ["key"]}]}}}, "http": {}, "collections": [
                    {"name": "a", "indexes": {}, "access": {"read": {"const": true}}},
                ]}),
                "Invalid access rule for derived d: access: key cannot be used here",
            ),
            (
                json!({"definitions": {"q": {"kind": "query", "access": {"const": true}}}, "http": {}}),
                "Only derived definitions declare access: q",
            ),
            (
                json!({"definitions": {"d": {"kind": "derived", "access": {"exists": {"ref": ["row", "x"]}}}}, "http": {}}),
                "Invalid access rule for derived d: access: row.x cannot be used here",
            ),
            (
                json!({"definitions": {"t": {"kind": "query", "aggregate": {"collection": "a", "fields": ["x"]}}}, "http": {}}),
                "Aggregate metadata requires",
            ),
            (
                json!({"definitions": {"t": {"kind": "derived", "aggregate": {"collection": "a", "fields": ["x"]}}}, "http": {}}),
                "Aggregate index must be declared",
            ),
            (
                json!({"definitions": {"t": {"kind": "derived", "aggregate": {"collection": "a", "fields": ["x"], "version": 2}}}, "http": {}, "collections": [{"name": "a", "indexes": {"x": ["x"]}}]}),
                "Aggregate version must be a string of 1 to 128 characters: t",
            ),
            (
                json!({"definitions": {"t": {"kind": "derived", "aggregate": {"collection": "a", "fields": ["x"], "version": ""}}}, "http": {}, "collections": [{"name": "a", "indexes": {"x": ["x"]}}]}),
                "Aggregate version must be a string of 1 to 128 characters: t",
            ),
        ] {
            let actual = error(raw.clone());
            assert!(actual.contains(message), "{raw}: {actual}");
        }
    }
}
