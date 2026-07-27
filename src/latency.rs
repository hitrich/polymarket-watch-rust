use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

const DEFAULT_MAX_SAMPLES_PER_METRIC: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub samples: usize,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone)]
pub struct LatencyRecorder {
    max_samples_per_metric: usize,
    samples: BTreeMap<String, VecDeque<u64>>,
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_SAMPLES_PER_METRIC)
    }
}

impl LatencyRecorder {
    pub fn new(max_samples_per_metric: usize) -> Self {
        Self {
            max_samples_per_metric: max_samples_per_metric.max(1),
            samples: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, name: impl Into<String>, value_us: u64) {
        let values = self.samples.entry(name.into()).or_default();
        values.push_back(value_us);
        while values.len() > self.max_samples_per_metric {
            values.pop_front();
        }
    }

    pub fn p95(&self, name: &str) -> Option<u64> {
        percentile(self.samples.get(name)?, 95)
    }

    pub fn summary(&self, name: &str) -> Option<LatencySummary> {
        let values = self.samples.get(name)?;
        Some(LatencySummary {
            samples: values.len(),
            p50_us: percentile(values, 50)?,
            p95_us: percentile(values, 95)?,
            p99_us: percentile(values, 99)?,
            max_us: values.iter().copied().max()?,
        })
    }

    pub fn summaries(&self) -> BTreeMap<String, LatencySummary> {
        self.samples
            .keys()
            .filter_map(|name| self.summary(name).map(|summary| (name.clone(), summary)))
            .collect()
    }
}

fn percentile(values: &VecDeque<u64>, pct: u64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let rank = (pct.clamp(1, 100) * u64::try_from(sorted.len()).ok()?).div_ceil(100);
    let index = usize::try_from(rank.saturating_sub(1)).ok()?;
    sorted.get(index).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_nearest_rank_percentiles() {
        let mut recorder = LatencyRecorder::default();
        for value in 1..=100 {
            recorder.record("risk", value);
        }
        let summary = recorder.summary("risk").unwrap();
        assert_eq!(summary.p50_us, 50);
        assert_eq!(summary.p95_us, 95);
        assert_eq!(summary.p99_us, 99);
        assert_eq!(summary.max_us, 100);
    }

    #[test]
    fn bounds_memory_per_metric() {
        let mut recorder = LatencyRecorder::new(3);
        for value in 1..=10 {
            recorder.record("risk", value);
        }
        let summary = recorder.summary("risk").unwrap();
        assert_eq!(summary.samples, 3);
        assert_eq!(summary.p50_us, 9);
        assert_eq!(summary.max_us, 10);
    }
}
