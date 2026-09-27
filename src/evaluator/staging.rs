//! Native index-build primitives shared by resumable deployment and normal writes.
//! Index identity is its canonical collection/field tuple; active schema selects
//! which completed versions are visible to application queries.
pub(crate) use super::rust_engine::{IndexSpec, Schema};
use super::*;

pub(crate) const INDEXES: &str = "deployment:indexes";
pub(crate) const JOB: &str = "deployment:job";
const PLAN: &str = "deployment:plan";
const ACTIVE: &str = "reactive:active";

pub(crate) fn maintaining_graph(data: &Records) -> bool {
    data.get(JOB).is_some_and(|job| {
        job["generation"].is_string()
            && matches!(job["phase"].as_str(), Some("rebuilding" | "ready"))
    })
}

pub(crate) fn validate_generation(generation: &str) -> Result<()> {
    anyhow::ensure!(
        Records::valid_graph_generation(generation),
        "INPUT_INVALID: malformed reactive graph generation"
    );
    Ok(())
}

fn validate_target(data: &Records, input: &Value, generation: &str) -> Result<()> {
    validate_generation(generation)?;
    let job = data.get(JOB).context("staged deployment job is missing")?;
    anyhow::ensure!(
        job["generation"] == generation
            && job["requestId"] == input["requestId"]
            && job["bundleHash"].is_string()
            && job["bundleHash"] == input["bundle"]["hash"],
        "DEPLOYMENT_CONFLICT: target code does not match the staged reactive graph"
    );
    Ok(())
}

pub(crate) fn schema(value: Option<&Value>) -> Result<Schema> {
    rust_engine::staged_schema(value).map_err(Into::into)
}
pub(crate) fn source_prefix(collection: &str) -> String {
    format!(
        "source:[{},",
        serde_json::to_string(collection).expect("collection encodes")
    )
}
pub(crate) fn entries(
    spec: &IndexSpec,
    id: &str,
    value: &Value,
) -> Result<BTreeMap<String, Value>> {
    rust_engine::staged_entries(spec, id, value).map_err(Into::into)
}
pub(crate) fn prefixes(spec: &IndexSpec) -> [String; 2] {
    rust_engine::staged_prefixes(spec)
}

/// Analyze only declarations in an isolated empty database. No historical roots
/// or source rows are copied/backfilled here; publication remains a later step.
pub(crate) fn analyze(input: Value) -> Result<Schema> {
    let evaluation = prepare(input)?;
    schema(evaluation.puts.get("schema"))
}

/// Validate declarations and return the code's activation metadata, without
/// carrying any existing roots or source rows into the evaluator.
pub(crate) fn prepare(input: Value) -> Result<Evaluation> {
    evaluate_at(Records::default(), input, 0)
}

/// Build selected roots and their dependency closures inside an unpublished
/// graph. Completed roots retain their outcomes and accumulator state; the
/// target manifest is transient until the service atomically activates it.
#[cfg(test)]
pub(crate) fn graph_page(
    data: Records,
    input: Value,
    generation: &str,
    roots: Vec<Value>,
    now: u64,
) -> Result<Evaluation> {
    graph_page_with_timeout(
        data,
        input,
        generation,
        roots,
        now,
        config::settings()?.evaluation_timeout,
    )
}

pub(crate) fn graph_page_with_timeout(
    data: Records,
    mut input: Value,
    generation: &str,
    roots: Vec<Value>,
    now: u64,
    timeout: Duration,
) -> Result<Evaluation> {
    validate_mutation(&input)?;
    validate_target(&data, &input, generation)?;
    input["materialize"] = Value::Array(roots);
    let view = data.graph_view(Some(generation));
    let prefix = format!("graph:{generation}:");
    let mut result = evaluate_selected(view, input, "graph", timeout, Some(now))?;
    retain_graph(&mut result, &prefix);
    Ok(result)
}

/// The service holds the writer and checks its ready job. New jobs already own
/// a maintained graph, so cutover only refreshes time/key dependencies and
/// switches metadata and the graph pointer. Persisted legacy jobs retain their
/// original whole-graph activation path.
pub(crate) fn activate(data: Records, mut input: Value, now: u64) -> Result<Evaluation> {
    validate_mutation(&input)?;
    let generation = data
        .get(JOB)
        .and_then(|job| job.get("generation"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(generation) = generation {
        validate_target(&data, &input, &generation)?;
        anyhow::ensure!(
            data.active_graph() != Some(generation.as_str()),
            "DEPLOYMENT_CONFLICT: staged generation is already active"
        );
        let job = data.get(JOB).expect("generation came from job");
        anyhow::ensure!(
            job["phase"] == "ready" && job["requestId"] == input["requestId"],
            "DEPLOYMENT_CONFLICT: staged deployment is not ready for this request"
        );
        input["$keysChanged"] = Value::Bool(true);
        let mut result = evaluate_selected(
            data.graph_view(Some(&generation)),
            input,
            "graph",
            config::settings()?.evaluation_timeout,
            Some(now),
        )?;
        result.puts.insert(ACTIVE.into(), json!(generation));
        anyhow::ensure!(
            evaluation_bytes(&result)? <= config::settings()?.result_max_bytes,
            "activation metadata exceeds FLOWER_RESULT_MAX_BYTES"
        );
        return Ok(result);
    }
    input["$stagedDeployment"] = Value::Bool(true);
    evaluate_inner(
        data,
        input,
        "deployment",
        config::settings()?.evaluation_timeout,
        Some(now),
    )
}

fn retain_graph(evaluation: &mut Evaluation, prefix: &str) {
    evaluation.puts.retain(|key, _| key.starts_with(prefix));
    evaluation.deletes.retain(|key| key.starts_with(prefix));
    evaluation.query_cacheable = false;
    evaluation.query_certificate = None;
    evaluation.mutation_certificate = None;
}

/// Replay only the active mutation's final source/root changes, never its
/// method body, against the old source snapshot and the target callbacks. Both
/// graph patches publish in the same command. A failed target build is fenced
/// from activation while the active application can continue serving.
pub(super) fn maintain(
    data: &Records,
    active: &mut Evaluation,
    keys_changed: bool,
    now: Option<u64>,
    timeout: Duration,
) -> Result<()> {
    let Some(job) = data.get(JOB).filter(|job| {
        job["generation"].is_string()
            && matches!(job["phase"].as_str(), Some("rebuilding" | "ready"))
    }) else {
        return Ok(());
    };
    active.mutation_certificate = None;
    let generation = job["generation"].as_str().expect("checked generation");
    let shadow = (|| -> Result<Evaluation> {
        validate_generation(generation)?;
        anyhow::ensure!(
            data.active_graph() != Some(generation),
            "staged generation is already active"
        );
        anyhow::ensure!(
            !timeout.is_zero(),
            "shadow graph evaluation deadline exceeded"
        );
        let bundle = data
            .get(PLAN)
            .and_then(|plan| plan.get("bundle"))
            .filter(|bundle| !bundle.is_null())
            .context("staged deployment bundle is missing")?;
        anyhow::ensure!(
            job["bundleHash"].is_string() && job["bundleHash"] == bundle["hash"],
            "staged bundle does not match its reactive graph"
        );
        let root_prefix = data.graph_key("root:");
        let mut roots = Vec::new();
        let mut removed_roots = Vec::new();
        let mut writes = Vec::new();
        for (key, value) in &active.puts {
            if let Some(encoded) = key.strip_prefix("source:") {
                let (collection, row): (String, String) = serde_json::from_str(encoded)?;
                writes.push(json!({"collection":collection,"key":row,"value":value}));
            } else if key.starts_with(&root_prefix) {
                roots.push(value.clone());
            }
        }
        for key in &active.deletes {
            if let Some(encoded) = key.strip_prefix("source:") {
                let (collection, row): (String, String) = serde_json::from_str(encoded)?;
                writes.push(json!({"collection":collection,"key":row,"delete":true}));
            } else if key.starts_with(&root_prefix)
                && let Some(root) = data.get_raw_shared(key)
            {
                removed_roots.push((**root).clone());
            }
        }
        let mut view = data.graph_view(Some(generation));
        if keys_changed && let Some(catalog) = active.puts.get("managedKeys") {
            view.insert("managedKeys".into(), catalog.clone());
        }
        let prefix = format!("graph:{generation}:");
        let mut result = evaluate_selected(
            view,
            json!({"requestId":job["requestId"],"bundle":bundle,
                "writes":writes,"materialize":roots,"unmaterialize":removed_roots,
                "$keysChanged":keys_changed}),
            "graph",
            timeout,
            now,
        )?;
        retain_graph(&mut result, &prefix);
        // Graph identities cannot overlap the active generation. Reserve the
        // complete combined patch before extending it, avoiding a rollback copy.
        let mut combined = evaluation_bytes(active)?;
        for (index, (key, value)) in result.puts.iter().enumerate() {
            combined = combined
                .saturating_add(json_byte_len(key)?)
                .saturating_add(1)
                .saturating_add(json_byte_len(value)?)
                .saturating_add(usize::from(!active.puts.is_empty() || index != 0));
        }
        for (index, key) in result.deletes.iter().enumerate() {
            combined = combined
                .saturating_add(json_byte_len(key)?)
                .saturating_add(usize::from(!active.deletes.is_empty() || index != 0));
        }
        anyhow::ensure!(
            combined <= config::settings()?.result_max_bytes,
            "combined active and staged graphs exceed FLOWER_RESULT_MAX_BYTES"
        );
        Ok(result)
    })();
    match shadow {
        Ok(shadow) => {
            active.puts.extend(shadow.puts);
            active.deletes.extend(shadow.deletes);
            active.evaluated.extend(shadow.evaluated);
        }
        Err(error) => {
            let mut failed = job.clone();
            failed["phase"] = json!("failed");
            // A callback error cannot make the durable status itself unbounded.
            failed["error"] = json!(error.to_string().chars().take(1024).collect::<String>());
            active.puts.insert(JOB.into(), failed);
        }
    }
    anyhow::ensure!(
        evaluation_bytes(active)? <= config::settings()?.result_max_bytes,
        "active evaluation and staged deployment status exceed FLOWER_RESULT_MAX_BYTES"
    );
    Ok(())
}

pub(crate) fn schema_bytes(schema: &Schema) -> usize {
    rust_engine::staged_schema_bytes(schema)
}

#[cfg(test)]
mod tests;
