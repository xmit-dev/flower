//! Independent arrival clock with bounded client-side concurrency. Saturated
//! arrivals are counted as driver drops, never silently deferred or omitted.
use super::*;
use metrics::Histogram;
#[derive(Default)]
pub(super) struct Counters {
    offered: u64,
    dispatched: u64,
    dropped: u64,
    completed: u64,
    failed: u64,
    lag: Histogram,
}
impl Counters {
    pub(super) fn snapshot(&self) -> Value {
        json!({"offered":self.offered,"dispatched":self.dispatched,
        "driverDropped":self.dropped,"completed":self.completed,"failed":self.failed,"schedulingLagMs":self.lag.snapshot()})
    }
}
fn due(start: Instant, sequence: u64, rate: f64) -> Instant {
    start + Duration::from_secs_f64(sequence as f64 / rate)
}
pub(super) async fn run(client: Arc<Client>, started: Instant, deadline: Instant) {
    let rate = client.config.offered_rate;
    let total = (deadline.duration_since(started).as_secs_f64() * rate).ceil() as u64;
    let mut next = 0u64;
    let mut active = tokio::task::JoinSet::new();
    while next < total || !active.is_empty() {
        if next == total {
            if let Some(result) = active.join_next().await {
                result.expect("arrival task failed");
            }
            continue;
        }
        let scheduled = due(started, next, rate);
        tokio::select! {
            result=active.join_next(),if !active.is_empty()=>{if let Some(result)=result {result.expect("arrival task failed");}},
            _=tokio::time::sleep_until(scheduled.into())=>{
                // Drain already-finished work before deciding the driver is full.
                while let Some(result)=active.try_join_next() {result.expect("arrival task failed");}
                let now=Instant::now();
                if active.len()>=client.config.concurrency {
                    // Account all due arrivals in O(1), even at extreme rates.
                    let end=((now.duration_since(started).as_secs_f64()*rate).floor() as u64).saturating_add(1).min(total);
                    let dropped=end.saturating_sub(next).max(1);
                    let mut state=client.state.lock().unwrap();state.offered.offered+=dropped;state.offered.dropped+=dropped;
                    next=next.saturating_add(dropped).min(total);
                } else {
                    {let mut state=client.state.lock().unwrap();state.offered.offered+=1;state.offered.dispatched+=1;
                    state.offered.lag.record(now.saturating_duration_since(scheduled).as_secs_f64()*1000.0);}
                    let id=next;next+=1;let client=client.clone();
                    active.spawn(async move {
                        let mut random=Random::new(&format!("{}:arrival:{id}",client.config.seed));
                        let success=customer(client.clone(),id as usize,&mut random,Some(scheduled)).await;
                        let mut state=client.state.lock().unwrap();
                        if success {state.offered.completed+=1;}else {state.offered.failed+=1;}
                    });
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_schedule_never_moves_when_responses_are_late() {
        let start = Instant::now();
        assert_eq!(due(start, 10, 100.0) - start, Duration::from_millis(100));
        assert_eq!(due(start, 11, 100.0) - start, Duration::from_millis(110));
        let late = start + Duration::from_secs(3);
        assert_eq!(
            late.duration_since(due(start, 11, 100.0)),
            Duration::from_millis(2890)
        );
    }
}
