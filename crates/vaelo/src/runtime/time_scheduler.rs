// SPDX-License-Identifier: Apache-2.0

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::foundation::intrusive_forward_list::{
    IntrusiveForwardList, IntrusiveForwardListAdapter, IntrusiveForwardListLink,
};
use crate::transport::inflight::CallContext;
use crate::{Resource, ResourceId, StaticResource};

const TIMER_BUCKETS: usize = 64;
const NEAR_WINDOW: Duration = Duration::from_secs(60);
const WHEEL_TICK: Duration = Duration::from_secs(1);
const WHEEL_SLOTS: usize = 256;
const WAKEUP_SENT: u64 = 0;
const NO_WAKEUP: u64 = u64::MAX;

static TIME_SCHEDULER: LazyLock<TimeScheduler> = LazyLock::new(TimeScheduler::new);

pub(crate) struct TimeScheduler {
    origin: Instant,
    buckets: [TimerBucket; TIMER_BUCKETS],
    wake_lock: Mutex<()>,
    wake: Condvar,
    thread_started: AtomicBool,
}

struct TimerBucket {
    list: IntrusiveForwardList<TimerBucketAdapter>,
    wakeup_tick: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct TimerBucketAdapter;

struct TimerThread {
    near_heap: BinaryHeap<Reverse<TimerEntry>>,
    wheel: TimerWheel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TimerEntry {
    deadline: Instant,
    context_id: ResourceId<CallContext>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WheelEntry {
    timer: TimerEntry,
    remaining_rounds: usize,
}

struct TimerWheel {
    slots: Box<[Vec<WheelEntry>]>,
    cursor: usize,
    next_tick: Instant,
    entries: usize,
}

impl TimeScheduler {
    pub(crate) fn global() -> &'static Self {
        &TIME_SCHEDULER
    }

    pub(crate) fn register(context: Resource<CallContext>) {
        let Some(deadline) = context.deadline() else {
            return;
        };
        if !context.is_active() {
            return;
        }

        let scheduler = Self::global();
        scheduler.ensure_thread();
        let wakeup_tick = scheduler.encode_instant(deadline);
        let bucket = &scheduler.buckets[bucket_index(context.id())];
        bucket.push(context);

        if bucket.relax_wakeup(wakeup_tick) {
            scheduler.notify_thread();
        }
    }

    fn new() -> Self {
        let origin = Instant::now();
        Self {
            origin,
            buckets: std::array::from_fn(|_| TimerBucket::new()),
            wake_lock: Mutex::new(()),
            wake: Condvar::new(),
            thread_started: AtomicBool::new(false),
        }
    }

    fn ensure_thread(&'static self) {
        if self
            .thread_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            std::thread::Builder::new()
                .name("time-scheduler".into())
                .spawn(move || TimerThread::new(self.origin).run(self))
                .expect("failed to spawn time scheduler");
        }
    }

    fn encode_instant(&self, instant: Instant) -> u64 {
        instant
            .checked_duration_since(self.origin)
            .unwrap_or_default()
            .as_nanos()
            .saturating_add(1)
            .min(u64::MAX as u128 - 1) as u64
    }

    fn wait_for_work(&self, wait_until: Option<Instant>) {
        let guard = self.wake_lock.lock().expect("time scheduler wake mutex poisoned");
        if self.publish_bucket_wakeups(wait_until) {
            return;
        }
        match wait_until {
            Some(deadline) => {
                let timeout = deadline.saturating_duration_since(Instant::now());
                let wait_result = self
                    .wake
                    .wait_timeout(guard, timeout)
                    .expect("time scheduler wake condvar wait failed");
                drop(wait_result);
            }
            None => {
                let wait_result =
                    self.wake.wait(guard).expect("time scheduler wake condvar wait failed");
                drop(wait_result);
            }
        }
    }

    fn notify_thread(&self) {
        let _guard = self.wake_lock.lock().expect("time scheduler wake mutex poisoned");
        self.wake.notify_one();
    }

    fn publish_bucket_wakeups(&self, wait_until: Option<Instant>) -> bool {
        let wait_tick = wait_until.map_or(NO_WAKEUP, |instant| self.encode_instant(instant));
        self.buckets.iter().any(|bucket| bucket.publish_wakeup(wait_tick))
    }
}

impl TimerBucket {
    fn new() -> Self {
        Self { list: IntrusiveForwardList::new(), wakeup_tick: AtomicU64::new(NO_WAKEUP) }
    }

    fn push(&self, context: Resource<CallContext>) {
        self.list.push(context);
    }

    fn take(&self) -> impl Iterator<Item = Resource<CallContext>> + '_ {
        self.list.take()
    }

    fn prepare_drain(&self) {
        self.wakeup_tick.store(NO_WAKEUP, Ordering::Release);
    }

    fn relax_wakeup(&self, tick: u64) -> bool {
        let mut old = self.wakeup_tick.load(Ordering::Acquire);
        loop {
            if old == WAKEUP_SENT || tick >= old {
                return false;
            }
            match self.wakeup_tick.compare_exchange_weak(
                old,
                WAKEUP_SENT,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => old = actual,
            }
        }
    }

    fn publish_wakeup(&self, tick: u64) -> bool {
        let mut old = self.wakeup_tick.load(Ordering::Acquire);
        loop {
            if old == WAKEUP_SENT {
                return true;
            }
            if old == tick {
                return false;
            }
            match self.wakeup_tick.compare_exchange_weak(
                old,
                tick,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return false,
                Err(actual) => old = actual,
            }
        }
    }
}

impl IntrusiveForwardListAdapter for TimerBucketAdapter {
    type Value = CallContext;

    fn link(value: &Self::Value) -> &IntrusiveForwardListLink<Self::Value> {
        value.timer_link()
    }
}

impl TimerThread {
    fn new(now: Instant) -> Self {
        Self { near_heap: BinaryHeap::new(), wheel: TimerWheel::new(now) }
    }

    fn run(mut self, scheduler: &'static TimeScheduler) {
        loop {
            let now = Instant::now();
            self.drain_buckets(scheduler, now);
            self.fire_expired(now);

            let wait_until = self.next_wait_until();
            scheduler.wait_for_work(wait_until);
        }
    }

    fn drain_buckets(&mut self, scheduler: &TimeScheduler, now: Instant) {
        for bucket in &scheduler.buckets {
            bucket.prepare_drain();
            for context in bucket.take() {
                let Some(deadline) = context.deadline() else {
                    continue;
                };
                if !context.is_active() {
                    continue;
                }
                self.insert_entry(TimerEntry { deadline, context_id: context.id() }, now);
            }
        }
    }

    fn insert_entry(&mut self, entry: TimerEntry, now: Instant) {
        if entry.deadline <= now {
            complete_timeout(entry);
            return;
        }

        if entry.deadline.saturating_duration_since(now) <= NEAR_WINDOW {
            self.near_heap.push(Reverse(entry));
        } else {
            self.wheel.insert(entry, now);
        }
    }

    fn fire_expired(&mut self, now: Instant) {
        let mut expired = Vec::new();
        let mut near = Vec::new();
        while matches!(self.near_heap.peek(), Some(Reverse(entry)) if entry.deadline <= now) {
            let entry = self
                .near_heap
                .pop()
                .expect("near timer heap must contain entry after successful peek")
                .0;
            expired.push(entry);
        }

        while self.wheel.should_advance(now) {
            self.wheel.advance(now, &mut expired, &mut near);
        }
        for entry in near.drain(..) {
            self.near_heap.push(Reverse(entry));
        }

        for entry in expired {
            complete_timeout(entry);
        }
    }

    fn next_wait_until(&self) -> Option<Instant> {
        let heap_deadline = self.near_heap.peek().map(|entry| entry.0.deadline);
        match (heap_deadline, self.wheel.has_entries()) {
            (Some(deadline), true) => Some(deadline.min(self.wheel.next_tick)),
            (Some(deadline), false) => Some(deadline),
            (None, true) => Some(self.wheel.next_tick),
            (None, false) => None,
        }
    }
}

impl TimerWheel {
    fn new(now: Instant) -> Self {
        let mut slots = Vec::with_capacity(WHEEL_SLOTS);
        slots.resize_with(WHEEL_SLOTS, Vec::new);
        Self { slots: slots.into_boxed_slice(), cursor: 0, next_tick: now + WHEEL_TICK, entries: 0 }
    }

    fn should_advance(&self, now: Instant) -> bool {
        self.entries != 0 && self.next_tick <= now
    }

    fn has_entries(&self) -> bool {
        self.entries != 0
    }

    fn insert(&mut self, timer: TimerEntry, now: Instant) {
        if self.entries == 0 && self.next_tick <= now {
            self.next_tick = now + WHEEL_TICK;
        }
        let ticks = timer
            .deadline
            .saturating_duration_since(now)
            .as_nanos()
            .div_ceil(WHEEL_TICK.as_nanos())
            .max(1) as usize;
        let offset = ((ticks - 1) % WHEEL_SLOTS) + 1;
        let remaining_rounds = (ticks - 1) / WHEEL_SLOTS;
        let slot = (self.cursor + offset) % WHEEL_SLOTS;
        self.slots[slot].push(WheelEntry { timer, remaining_rounds });
        self.entries += 1;
    }

    fn advance(&mut self, now: Instant, expired: &mut Vec<TimerEntry>, near: &mut Vec<TimerEntry>) {
        self.cursor = (self.cursor + 1) % WHEEL_SLOTS;
        self.next_tick += WHEEL_TICK;

        let mut slot = std::mem::take(&mut self.slots[self.cursor]);
        self.entries -= slot.len();
        for mut entry in slot.drain(..) {
            if !is_timer_live(entry.timer.context_id) {
                continue;
            }
            if entry.remaining_rounds > 0 {
                entry.remaining_rounds -= 1;
                self.slots[self.cursor].push(entry);
                self.entries += 1;
                continue;
            }
            if entry.timer.deadline <= now {
                expired.push(entry.timer);
            } else if entry.timer.deadline.saturating_duration_since(now) <= NEAR_WINDOW {
                near.push(entry.timer);
            } else {
                self.insert(entry.timer, now);
            }
        }
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.context_id.as_u64().cmp(&other.context_id.as_u64()))
    }
}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn bucket_index(id: ResourceId<CallContext>) -> usize {
    id.as_u64() as usize & (TIMER_BUCKETS - 1)
}

fn is_timer_live(id: ResourceId<CallContext>) -> bool {
    CallContext::resource_pool().get(id).is_some_and(|context| context.is_active())
}

fn complete_timeout(entry: TimerEntry) {
    let Some(context) = CallContext::resource_pool().get(entry.context_id) else {
        return;
    };
    let _ = context.complete_deadline_exceeded();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_wakeup_is_sent_once_per_published_wait() {
        let bucket = TimerBucket::new();

        assert!(!bucket.publish_wakeup(100));
        assert!(!bucket.relax_wakeup(150));
        assert!(bucket.relax_wakeup(50));
        assert!(!bucket.relax_wakeup(25));
        assert!(bucket.publish_wakeup(100));

        bucket.prepare_drain();
        assert!(!bucket.publish_wakeup(100));
        assert!(bucket.relax_wakeup(75));
        assert!(!bucket.relax_wakeup(50));
    }

    #[test]
    fn pending_tick_prevents_indefinite_wait() {
        let bucket = TimerBucket::new();

        assert!(bucket.relax_wakeup(50));
        assert!(bucket.publish_wakeup(NO_WAKEUP));
        bucket.prepare_drain();
        assert!(!bucket.publish_wakeup(NO_WAKEUP));
    }
}
