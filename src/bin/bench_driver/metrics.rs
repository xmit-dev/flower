use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::OnceLock;

const MAX_MS: f64 = 86_400_000.0;
fn bounds() -> &'static [f64] {
    static BOUNDS: OnceLock<Vec<f64>> = OnceLock::new();
    BOUNDS.get_or_init(|| {
        let mut values = vec![0.0, 0.001];
        while *values.last().unwrap() < MAX_MS {
            values.push((values.last().unwrap() * 1.01).min(MAX_MS));
        }
        values.push(f64::INFINITY);
        values
    })
}
#[derive(Clone)]
pub struct Histogram {
    buckets: Vec<u64>,
    samples: u64,
    min: f64,
    max: f64,
}
impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: vec![0; bounds().len()],
            samples: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}
impl Histogram {
    pub fn record(&mut self, value: f64) {
        assert!(value.is_finite() && value >= 0.0);
        self.buckets[bounds().partition_point(|bound| *bound < value)] += 1;
        self.samples += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
    fn percentile(&self, percent: f64) -> Option<f64> {
        if self.samples == 0 {
            return None;
        }
        let rank = (percent / 100.0 * self.samples as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return Some(bounds()[index].min(self.max));
            }
        }
        Some(self.max)
    }
    pub fn snapshot(&self) -> Value {
        json!({"buckets": self.buckets, "samples": self.samples, "invalidSamples": 0,
            "overflowSamples": self.buckets.last(), "approximate": true,
            "min": (self.samples > 0).then_some(self.min), "max": (self.samples > 0).then_some(self.max),
            "p50": self.percentile(50.0), "p95": self.percentile(95.0), "p99": self.percentile(99.0)})
    }
}
#[derive(Default)]
pub struct Counters {
    attempts: u64,
    successes: u64,
    failures: u64,
    retries: u64,
    duplicates: u64,
    latency: Histogram,
    errors: BTreeMap<String, u64>,
}
impl Counters {
    fn record(&mut self, ms: f64, ok: bool, status: &str, retry: bool, duplicate: bool) {
        self.attempts += 1;
        if ok {
            self.successes += 1;
        } else {
            self.failures += 1;
            *self.errors.entry(status.to_owned()).or_default() += 1;
        }
        self.retries += u64::from(retry);
        self.duplicates += u64::from(duplicate);
        self.latency.record(ms);
    }
    fn snapshot(&self, duration: f64, logical: bool) -> Value {
        let rate = if duration > 0.0 {
            self.successes as f64 / (duration / 1000.0)
        } else {
            0.0
        };
        if logical {
            json!({"count": self.attempts, "completed": self.successes, "failed": self.failures,
                "duplicates": self.duplicates, "throughputPerSecond": rate, "latencyMs": self.latency.snapshot()})
        } else {
            json!({"attempts": self.attempts, "successes": self.successes, "failures": self.failures,
                "retries": self.retries, "duplicates": self.duplicates, "throughputPerSecond": rate,
                "attemptsPerSecond": if duration > 0.0 { self.attempts as f64 / (duration / 1000.0) } else { 0.0 },
                "errors": self.errors, "latencyMs": self.latency.snapshot()})
        }
    }
}
#[derive(Default)]
struct Series {
    all: Counters,
    methods: BTreeMap<String, Counters>,
}
impl Series {
    fn record(
        &mut self,
        name: &str,
        ms: f64,
        ok: bool,
        status: &str,
        retry: bool,
        duplicate: bool,
    ) {
        self.all.record(ms, ok, status, retry, duplicate);
        self.methods
            .entry(name.to_owned())
            .or_default()
            .record(ms, ok, status, retry, duplicate);
    }
    fn snapshot(&self, duration: f64, logical: bool) -> Value {
        let mut result = self.all.snapshot(duration, logical);
        result["perMethod"] = self
            .methods
            .iter()
            .map(|(name, counters)| (name.clone(), counters.snapshot(duration, logical)))
            .collect::<serde_json::Map<_, _>>()
            .into();
        result
    }
}
#[derive(Default)]
pub struct Stats {
    attempts: Series,
    operations: Series,
}
impl Stats {
    pub fn attempt(
        &mut self,
        name: &str,
        ms: f64,
        ok: bool,
        status: &str,
        retry: bool,
        duplicate: bool,
    ) {
        self.attempts.record(name, ms, ok, status, retry, duplicate);
    }
    pub fn operation(&mut self, name: &str, ms: f64, ok: bool, duplicate: bool) {
        self.operations
            .record(name, ms, ok, "network", false, duplicate);
    }
    pub fn snapshot(&self, duration: f64) -> Value {
        let mut result = self.attempts.snapshot(duration, false);
        result["durationMs"] = json!(duration);
        result["histogram"] = json!({"unit": "milliseconds", "percentiles": "nearest-rank logarithmic bucket upper bounds",
            "relativeBucketWidth": 1.01_f64 - 1.0, "minPositiveMs": 0.001, "maxBoundedMs": MAX_MS, "bucketCount": bounds().len()});
        result["operations"] = self.operations.snapshot(duration, true);
        result["timerLatenessMs"] = Histogram::default().snapshot();
        result
    }
}

pub struct Random(u32);
impl Random {
    pub fn new(seed: &str) -> Self {
        let mut state = 0x811c_9dc5_u32;
        for ch in seed.chars() {
            state = (state ^ u32::from(ch)).wrapping_mul(0x0100_0193);
        }
        Self(state)
    }
    pub fn next(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x6d2b_79f5);
        let mut value = (self.0 ^ (self.0 >> 15)).wrapping_mul(self.0 | 1);
        value ^= value.wrapping_add((value ^ (value >> 7)).wrapping_mul(value | 61));
        f64::from(value ^ (value >> 14)) / 4_294_967_296.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histograms_cover_zero_submicrosecond_and_overflow_samples() {
        let mut h = Histogram::default();
        for value in [0.0, 0.0, 0.0001, 0.0002, 0.001, 0.001, 1.0, 1.0, 1e9, 2e9] {
            h.record(value);
        }
        assert_eq!(h.percentile(20.0), Some(0.0));
        assert_eq!(h.percentile(30.0), Some(0.001));
        assert_eq!(h.percentile(90.0), Some(2e9));
        assert_eq!(h.buckets.last(), Some(&2));
    }
    #[test]
    fn random_unicode_seed_matches_javascript_codepoint_semantics() {
        let mut random = Random::new("pizza-🌸");
        // Generated by bench/metrics.mjs createRandom using Unicode code points.
        let expected = [
            0.25823454186320305,
            0.30264041805639863,
            0.8295244711916894,
            0.45414901175536215,
            0.8598633082583547,
            0.41006043110974133,
            0.9161705092992634,
            0.2261476602870971,
        ];
        for value in expected {
            assert_eq!(random.next(), value);
        }
    }
}
