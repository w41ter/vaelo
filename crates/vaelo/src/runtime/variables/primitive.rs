// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::Serialize;

use super::arena::{self, GaugeSlot, Slot, SlotRange};

const LATENCY_BUCKETS: usize = 65;
const LATENCY_SLOT_COUNT: u16 = 3 + LATENCY_BUCKETS as u16;
const LATENCY_COUNT_OFFSET: u32 = 0;
const LATENCY_TOTAL_US_OFFSET: u32 = 1;
const LATENCY_MAX_US_OFFSET: u32 = 2;
const LATENCY_BUCKETS_OFFSET: u32 = 3;

#[derive(Clone, Copy, Debug)]
pub struct Counter {
    pub(in crate::runtime::variables) slot: Slot,
}

impl Counter {
    pub fn new() -> Self {
        Self { slot: arena::allocate_slot() }
    }

    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    #[inline]
    pub fn add(&self, value: u64) {
        arena::add(self.slot, value);
    }

    pub fn get(&self) -> u64 {
        arena::sum(self.slot)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Balance {
    pub(in crate::runtime::variables) slot: Slot,
}

impl Balance {
    pub fn new() -> Self {
        Self { slot: arena::allocate_slot() }
    }

    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    #[inline]
    pub fn dec(&self) {
        self.add(-1);
    }

    #[inline]
    pub fn add(&self, value: i64) {
        arena::add_signed(self.slot, value);
    }

    pub fn get(&self) -> i64 {
        arena::sum_signed(self.slot)
    }
}

impl Default for Balance {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Gauge {
    slot: GaugeSlot,
}

impl Gauge {
    pub fn new(value: i64) -> Self {
        Self { slot: arena::allocate_gauge(value) }
    }

    #[inline]
    pub fn set(&self, value: i64) {
        arena::store_gauge(self.slot, value);
    }

    pub fn get(&self) -> i64 {
        arena::load_gauge(self.slot)
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new(0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MaxRecorder {
    slot: Slot,
}

impl MaxRecorder {
    pub fn new() -> Self {
        Self { slot: arena::allocate_slot() }
    }

    #[inline]
    pub fn record(&self, value: u64) {
        arena::max_on_shard(arena::current_shard(), self.slot, value);
    }

    pub fn get(&self) -> u64 {
        arena::max(self.slot)
    }
}

impl Default for MaxRecorder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LatencyRecorder {
    pub(in crate::runtime::variables) slots: SlotRange,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self { slots: arena::allocate_slots(LATENCY_SLOT_COUNT) }
    }

    #[inline]
    pub fn record(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let shard = arena::current_shard();
        arena::add_on_shard(shard, self.slots.slot(LATENCY_COUNT_OFFSET), 1);
        arena::add_on_shard(shard, self.slots.slot(LATENCY_TOTAL_US_OFFSET), micros);
        arena::max_on_shard(shard, self.slots.slot(LATENCY_MAX_US_OFFSET), micros);
        arena::add_on_shard(
            shard,
            self.slots.slot(LATENCY_BUCKETS_OFFSET + bucket_index(micros) as u32),
            1,
        );
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        self.cumulative().snapshot()
    }

    pub(crate) fn cumulative(&self) -> LatencyCumulative {
        LatencyCumulative {
            count: arena::sum(self.slots.slot(LATENCY_COUNT_OFFSET)),
            total_us: arena::sum(self.slots.slot(LATENCY_TOTAL_US_OFFSET)),
            max_us: arena::max(self.slots.slot(LATENCY_MAX_US_OFFSET)),
            buckets: (0..LATENCY_BUCKETS)
                .map(|index| arena::sum(self.slots.slot(LATENCY_BUCKETS_OFFSET + index as u32)))
                .collect(),
        }
    }
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencySnapshot {
    pub count: u64,
    pub total_us: u64,
    pub average_us: u64,
    pub max_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LatencyCumulative {
    count: u64,
    total_us: u64,
    max_us: u64,
    buckets: Vec<u64>,
}

impl LatencyCumulative {
    pub(crate) fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            count: self.count,
            total_us: self.total_us,
            average_us: average(self.total_us, self.count),
            max_us: self.max_us,
            p50_us: percentile(&self.buckets, self.count, 0.50),
            p90_us: percentile(&self.buckets, self.count, 0.90),
            p99_us: percentile(&self.buckets, self.count, 0.99),
            p999_us: percentile(&self.buckets, self.count, 0.999),
        }
    }

    pub(crate) fn window_since(&self, previous: Option<&Self>) -> LatencySnapshot {
        let Some(previous) = previous else {
            return LatencySnapshot::default();
        };
        let count = self.count.saturating_sub(previous.count);
        let total_us = self.total_us.saturating_sub(previous.total_us);
        let buckets = subtract_buckets(&self.buckets, &previous.buckets);
        LatencySnapshot {
            count,
            total_us,
            average_us: average(total_us, count),
            max_us: histogram_max(&buckets),
            p50_us: percentile(&buckets, count, 0.50),
            p90_us: percentile(&buckets, count, 0.90),
            p99_us: percentile(&buckets, count, 0.99),
            p999_us: percentile(&buckets, count, 0.999),
        }
    }
}

#[inline]
pub(crate) fn average(total: u64, count: u64) -> u64 {
    total.checked_div(count).unwrap_or(0)
}

#[inline]
fn bucket_index(value: u64) -> usize {
    if value == 0 { 0 } else { (u64::BITS - value.leading_zeros()) as usize }
}

fn bucket_upper_bound(index: usize) -> u64 {
    match index {
        0 => 0,
        1..64 => (1_u64 << index) - 1,
        _ => u64::MAX,
    }
}

fn subtract_buckets(current: &[u64], previous: &[u64]) -> Vec<u64> {
    current
        .iter()
        .enumerate()
        .map(|(index, value)| value.saturating_sub(previous.get(index).copied().unwrap_or(0)))
        .collect()
}

fn histogram_max(buckets: &[u64]) -> u64 {
    buckets.iter().rposition(|samples| *samples > 0).map(bucket_upper_bound).unwrap_or(0)
}

fn percentile(buckets: &[u64], count: u64, ratio: f64) -> u64 {
    if count == 0 {
        return 0;
    }
    let rank = ((count as f64) * ratio).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (index, samples) in buckets.iter().copied().enumerate() {
        seen += samples;
        if seen >= rank {
            return bucket_upper_bound(index);
        }
    }
    u64::MAX
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn counter_and_balance_merge_shards() {
        let counter = Counter::new();
        counter.inc();
        counter.add(4);
        assert_eq!(counter.get(), 5);

        let balance = Balance::new();
        balance.inc();
        balance.add(3);
        balance.dec();
        assert_eq!(balance.get(), 3);
    }

    #[test]
    fn gauge_reports_the_latest_single_value() {
        let gauge = Gauge::new(7);
        let cloned = gauge;
        assert_eq!(cloned.get(), 7);
        gauge.set(-3);
        assert_eq!(cloned.get(), -3);
    }

    #[test]
    fn counter_merges_fixed_tls_shards_across_threads() {
        let counter = Arc::new(Counter::new());
        let threads = (0..8)
            .map(|_| {
                let counter = counter.clone();
                thread::spawn(move || {
                    for _ in 0..1_000 {
                        counter.inc();
                    }
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().unwrap();
        }

        assert_eq!(counter.get(), 8_000);
    }

    #[test]
    fn latency_snapshot_reports_average_max_and_percentiles() {
        let recorder = LatencyRecorder::new();
        recorder.record(Duration::from_micros(10));
        recorder.record(Duration::from_micros(20));
        recorder.record(Duration::from_micros(100));

        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.count, 3);
        assert_eq!(snapshot.average_us, 43);
        assert_eq!(snapshot.max_us, 100);
        assert!(snapshot.p50_us >= 20);
        assert!(snapshot.p99_us >= 100);
    }

    #[test]
    fn latency_window_uses_histogram_delta() {
        let mut previous_buckets = vec![0; LATENCY_BUCKETS];
        let mut current_buckets = vec![0; LATENCY_BUCKETS];
        previous_buckets[bucket_index(10)] = 1;
        current_buckets[bucket_index(10)] = 1;
        current_buckets[bucket_index(200)] = 2;

        let previous =
            LatencyCumulative { count: 1, total_us: 10, max_us: 10, buckets: previous_buckets };
        let current =
            LatencyCumulative { count: 3, total_us: 410, max_us: 200, buckets: current_buckets };

        let window = current.window_since(Some(&previous));
        assert_eq!(window.count, 2);
        assert_eq!(window.average_us, 200);
        assert!(window.max_us >= 200);
        assert!(window.p99_us >= 200);
    }
}
