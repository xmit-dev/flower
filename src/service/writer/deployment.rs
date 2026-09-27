//! Optimistic online preparation. Only a revision-validated cutover enters the
//! writer lane; an explicit blocking option trades write availability for progress.
use super::*;

pub(super) fn online(input: &PendingInput) -> Result<bool, ApiError> {
    let state = input.state.lock().expect("pending input");
    let body = state
        .body
        .as_ref()
        .ok_or_else(|| unavailable(anyhow::anyhow!("deployment canceled")))?;
    Ok(body.value.get("preparation").and_then(Value::as_str) != Some("blocking"))
}

pub(super) async fn stage(app: &App, input: &PendingInput) -> Result<Option<Value>, ApiError> {
    // Waiting preparations retain their request bytes, but no database roots.
    let admission = admit(app, input, true).await?;
    let request_id = input.request_id();
    let state = app
        .consensus
        .read_for(request_id.as_deref())
        .await
        .map_err(unavailable)?;
    let now = app.clock.sample(&state).map_err(unavailable)?;
    // Some(now) skips the serial evaluation semaphore. Global Control admission
    // still bounds native work and memory across all logical databases.
    let candidate =
        prepare_candidate_admitted(app, &state, input, true, Some(now), None, Some(admission))
            .await?;
    let prepared = candidate.prepared;
    if prepared.command.is_none() {
        return Ok(Some(prepared.response));
    }
    {
        let mut state = input.state.lock().expect("pending input");
        state.staged = Some(prepared);
        // Native preparation has finished. Until cutover starts, cancellation
        // can promptly release both the queued request and its candidate.
        state.begun = false;
    }
    // candidate's execution permit is dropped here, before waiting for writer.
    // Prepared retains the encrypted/code/index output's byte reservation.
    Ok(None)
}
