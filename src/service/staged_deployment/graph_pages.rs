//! Adaptive root pages amortize evaluator setup and Raft commits. Every page
//! remains one atomic patch; a failed batch retries only its first root.
use super::*;

#[derive(Serialize)]
struct GraphPatch<'a> {
    puts: &'a BTreeMap<String, Value>,
    deletes: &'a [String],
}

fn next_size(
    roots: usize,
    bytes: usize,
    elapsed: Duration,
    target: Duration,
    max_bytes: usize,
) -> usize {
    let growth = roots.saturating_mul(2).max(1);
    let by_time = (roots as u128).saturating_mul(target.as_nanos()) / elapsed.as_nanos().max(1);
    let by_bytes = (roots as u128).saturating_mul(max_bytes as u128) / (bytes as u128).max(1);
    growth
        .min(by_time.min(usize::MAX as u128) as usize)
        .min(by_bytes.min(usize::MAX as u128) as usize)
        .max(1)
}

fn page_target(timeout: Duration) -> Result<Duration, ApiError> {
    Ok(tuning::settings()
        .map_err(unavailable)?
        .deployment_page_time
        .min(timeout))
}

pub(super) async fn advance_graph(
    app: &App,
    state: &Snapshot,
    job: Job,
    max_bytes: usize,
    permit: admission::Permit,
) -> Result<Value, ApiError> {
    let settings = evaluator::config::settings().map_err(unavailable)?;
    let timeout = settings.evaluation_timeout;
    let target = page_target(timeout)?;
    let transaction_limit = app.consensus.command_payload_limit();
    let max_bytes = max_bytes.min(transaction_limit);
    let input_limit = settings.rust_memory_bytes;
    let base = state.clone();
    let now = app.clock.sample(state).map_err(unavailable)?;
    let (job, command, _permit) = tokio::task::spawn_blocking(move || {
        prepare_page(
            &base,
            job,
            max_bytes,
            transaction_limit,
            input_limit,
            now,
            timeout,
            target,
        )
        .map(|(job, command)| (job, command, permit))
    })
    .await
    .map_err(|error| unavailable(error.into()))??;
    let committed = persist(app, state, command).await?;
    Ok(json!({"revision":committed["revision"],"value":status(&job)}))
}

pub(super) fn prepare_page(
    base: &Snapshot,
    job: Job,
    max_bytes: usize,
    transaction_limit: usize,
    input_limit: usize,
    now: u64,
    timeout: Duration,
    target: Duration,
) -> Result<(Job, Commit), ApiError> {
    let prefix = job.base_generation.as_ref().map_or_else(
        || "root:".to_owned(),
        |generation| format!("graph:{generation}:root:"),
    );
    let bound = job.graph_cursor.as_deref().map_or(
        std::ops::Bound::Included(prefix.as_str()),
        std::ops::Bound::Excluded,
    );
    let wanted = job.graph_page_roots.max(1).min(
        max_bytes
            .checked_div(job.graph_root_bytes)
            .unwrap_or(usize::MAX)
            .max(1),
    );
    let mut entries = base
        .data
        .range::<(std::ops::Bound<&str>, std::ops::Bound<&str>)>((
            bound,
            std::ops::Bound::Unbounded,
        ))
        .take_while(|(key, _)| key.starts_with(&prefix))
        .peekable();
    let mut roots = Vec::new();
    let mut input_bytes = 0usize;
    // Selection is bounded independently of evaluator output. A single large
    // root retains the ordinary memory allowance; extra roots share a quarter.
    let selection_limit = max_bytes.min(input_limit / 4);
    while roots.len() < wanted {
        let Some((key, root)) = entries.peek() else {
            break;
        };
        let cost = key.len().saturating_add(admission::input_bytes(root));
        if roots.is_empty() && cost > input_limit {
            return Err(error(
                "one materialized root exceeds FLOWER_RUST_MEMORY_BYTES",
            ));
        }
        if !roots.is_empty() && input_bytes.saturating_add(cost) > selection_limit {
            break;
        }
        input_bytes = input_bytes.saturating_add(cost);
        roots.push(((*key).clone(), (*root).clone()));
        entries.next();
    }
    let exhausted = entries.peek().is_none();
    if roots.is_empty() {
        let mut next = job;
        next.phase = Phase::Ready;
        let command = command(base, &next, BTreeMap::new(), vec![], false);
        return Ok((next, command));
    }
    let generation = job
        .generation
        .as_deref()
        .ok_or_else(|| error("staged graph generation is missing"))?;
    let bundle = base
        .data
        .get(PLAN)
        .and_then(|plan| plan.get("bundle"))
        .filter(|bundle| !bundle.is_null())
        .ok_or_else(|| error("staged bundle is missing"))?;
    let attempt = |count: usize,
                   allowance: Duration,
                   retry: bool|
     -> Result<(Job, Commit), ApiError> {
        let started = Instant::now();
        let evaluation = staging::graph_page_with_timeout(
            base.data.clone(),
            json!({"requestId":job.request_id,"bundle":bundle}),
            generation,
            roots[..count]
                .iter()
                .map(|(_, value)| value.clone())
                .collect(),
            now,
            allowance,
        )
        .map_err(evaluation_error)?;
        let bytes = crate::consensus::encoded_json_len(&GraphPatch {
            puts: &evaluation.puts,
            deletes: &evaluation.deletes,
        })
        .map_err(|reason| invalid(reason.into()))?;
        if bytes > max_bytes {
            return Err(error(
                "materialized root page exceeds maxBytes; increase the page budget for a single root",
            ));
        }
        let mut next = job.clone();
        next.graph_cursor = Some(roots[count - 1].0.clone());
        next.rebuilt_roots = next
            .rebuilt_roots
            .checked_add(count as u64)
            .filter(|count| *count <= 9_007_199_254_740_991)
            .ok_or_else(|| error("deployment progress counter exhausted"))?;
        next.graph_root_bytes = bytes.div_ceil(count);
        next.graph_page_roots = if retry {
            1
        } else {
            next_size(count, bytes, started.elapsed(), target, max_bytes)
        };
        if exhausted && count == roots.len() {
            next.phase = Phase::Ready;
        }
        let command = command(base, &next, evaluation.puts, evaluation.deletes, false);
        if crate::consensus::encoded_json_len(&command).map_err(|reason| invalid(reason.into()))?
            > transaction_limit
        {
            return Err(error(
                "materialized root page exceeds FLOWER_TRANSACTION_MAX_BYTES",
            ));
        }
        Ok((next, command))
    };
    // A batch gets its own soft preparation window. If estimates were wrong,
    // one first-root fallback gets the full evaluation allowance. No repeated
    // halving/reexecution loop can hold the writer for many full deadlines.
    let count = roots.len();
    match attempt(count, if count == 1 { timeout } else { target }, false) {
        Ok(page) => Ok(page),
        Err(_) if count > 1 => attempt(1, timeout, true),
        Err(reason) => Err(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_page_target_worker() {
        let Ok(expected) = std::env::var("FLOWER_PAGE_CONFIG_TEST_EXPECTED") else {
            return;
        };
        let timeout = evaluator::config::settings().unwrap().evaluation_timeout;
        let target = page_target(timeout);
        if expected == "invalid" {
            assert!(
                target
                    .unwrap_err()
                    .message
                    .contains("FLOWER_DEPLOYMENT_PAGE_MS")
            );
        } else {
            assert_eq!(
                target.unwrap(),
                Duration::from_millis(expected.parse().unwrap())
            );
            assert_eq!(
                tuning::settings().unwrap().writer_batch_time,
                Duration::from_millis(
                    std::env::var("FLOWER_WRITER_BATCH_MS")
                        .unwrap()
                        .parse()
                        .unwrap()
                )
            );
        }
    }

    #[test]
    fn deployment_page_setting_is_independent_and_capped_by_evaluation_timeout() {
        let check = |page: Option<&str>, writer: &str, timeout: &str, expected: &str| {
            // Settings are cached at startup. Fresh processes exercise the real
            // environment parser without racing other tests over global state.
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            for (name, _) in std::env::vars_os() {
                if name.to_string_lossy().starts_with("FLOWER_") {
                    command.env_remove(name);
                }
            }
            command
                .args([
                    "--exact",
                    "service::staged_deployment::graph_pages::tests::configured_page_target_worker",
                    "--nocapture",
                ])
                .env("FLOWER_PAGE_CONFIG_TEST_EXPECTED", expected)
                .env("FLOWER_WRITER_BATCH_MS", writer)
                .env("FLOWER_EVALUATION_TIMEOUT_MS", timeout);
            if let Some(page) = page {
                command.env("FLOWER_DEPLOYMENT_PAGE_MS", page);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "page={page:?}, writer={writer}, timeout={timeout}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        };
        check(None, "7", "5000", "200");
        check(Some("20"), "333", "5000", "20");
        check(Some("1"), "333", "5000", "1");
        check(Some("500"), "7", "5000", "500");
        check(Some("6000"), "7", "50", "50");
        for invalid in ["0", "-1", "1.5", "", "18446744073709551616"] {
            check(Some(invalid), "333", "5000", "invalid");
        }
    }

    #[test]
    fn adaptive_roots_respect_work_and_byte_estimates() {
        let target = Duration::from_millis(20);
        assert_eq!(next_size(4, 100, Duration::from_millis(2), target, 1000), 8);
        assert_eq!(
            next_size(4, 100, Duration::from_millis(40), target, 1000),
            2
        );
        assert_eq!(next_size(4, 100, Duration::from_millis(2), target, 75), 3);
        assert_eq!(next_size(1, 1000, Duration::from_secs(1), target, 1), 1);
    }
}
