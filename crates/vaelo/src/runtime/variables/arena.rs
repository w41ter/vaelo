// SPDX-License-Identifier: Apache-2.0

use std::ptr;
use std::sync::atomic::{AtomicI64, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};

const SHARDS: usize = 128;
const SEGMENT_SLOTS: usize = 1024;
const MAX_SEGMENTS: usize = 1024;
const MAX_SLOTS: usize = SEGMENT_SLOTS * MAX_SEGMENTS;

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
static ARENA: LazyLock<VariableArena> = LazyLock::new(VariableArena::new);
static GAUGE_ARENA: LazyLock<GaugeArena> = LazyLock::new(GaugeArena::new);

thread_local! {
    static SHARD_INDEX: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
}

#[derive(Debug)]
struct VariableArena {
    next_slot: AtomicU32,
    shards: Box<[Shard]>,
    capacity_lock: Mutex<()>,
}

impl VariableArena {
    fn new() -> Self {
        Self {
            next_slot: AtomicU32::new(0),
            shards: (0..SHARDS).map(|_| Shard::new()).collect::<Vec<_>>().into_boxed_slice(),
            capacity_lock: Mutex::new(()),
        }
    }

    fn allocate_slot(&self) -> Slot {
        self.allocate_slots(1).slot(0)
    }

    fn allocate_slots(&self, width: u16) -> SlotRange {
        assert!(width > 0, "variable slot ranges must not be empty");
        let base = self.next_slot.fetch_add(width as u32, Ordering::Relaxed);
        let required = base as usize + width as usize;
        assert!(
            required <= MAX_SLOTS,
            "variables arena exhausted: required {required} slots, max {MAX_SLOTS}",
        );
        self.ensure_capacity(required);
        SlotRange { base, width }
    }

    fn ensure_capacity(&self, required_slots: usize) {
        let required_segments = required_slots.div_ceil(SEGMENT_SLOTS);
        let _guard = self.capacity_lock.lock().expect("variables arena mutex poisoned");
        for shard in self.shards.iter() {
            shard.ensure_segments(required_segments);
        }
    }

    #[inline]
    fn add(&self, slot: Slot, value: u64) {
        let shard = current_shard();
        self.add_on_shard(shard, slot, value);
    }

    #[inline]
    fn add_signed(&self, slot: Slot, value: i64) {
        let shard = current_shard();
        self.add_on_shard(shard, slot, value as u64);
    }

    #[inline]
    fn add_on_shard(&self, shard: usize, slot: Slot, value: u64) {
        self.shards[shard].atomic(slot).fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    fn max_on_shard(&self, shard: usize, slot: Slot, value: u64) {
        let atomic = self.shards[shard].atomic(slot);
        let mut current = atomic.load(Ordering::Relaxed);
        while value > current {
            match atomic.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn sum(&self, slot: Slot) -> u64 {
        self.shards.iter().map(|shard| shard.atomic(slot).load(Ordering::Relaxed)).sum()
    }

    fn sum_signed(&self, slot: Slot) -> i64 {
        self.shards.iter().fold(0_u64, |total, shard| {
            total.wrapping_add(shard.atomic(slot).load(Ordering::Relaxed))
        }) as i64
    }

    fn max(&self, slot: Slot) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.atomic(slot).load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
    }
}

#[derive(Debug)]
struct Shard {
    segments: Box<[AtomicPtr<Segment>]>,
}

impl Shard {
    fn new() -> Self {
        Self {
            segments: (0..MAX_SEGMENTS)
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn ensure_segments(&self, required_segments: usize) {
        for index in 0..required_segments {
            let current = self.segments[index].load(Ordering::Acquire);
            if current.is_null() {
                let segment = Box::into_raw(Box::new(Segment::new()));
                self.segments[index].store(segment, Ordering::Release);
            }
        }
    }

    #[inline]
    fn atomic(&self, slot: Slot) -> &AtomicU64 {
        let index = slot.index as usize;
        let segment_index = index / SEGMENT_SLOTS;
        let offset = index % SEGMENT_SLOTS;
        debug_assert!(segment_index < MAX_SEGMENTS);
        let segment = self.segments[segment_index].load(Ordering::Acquire);
        debug_assert!(!segment.is_null(), "variable segment must be published before use");
        // SAFETY: Segments are fully initialized before publication, never moved,
        // and never freed while variables may be accessed. Slot handles are only
        // returned after `VariableArena::ensure_capacity` publishes all required
        // shard segments.
        unsafe { (*segment).slot(offset) }
    }
}

#[derive(Debug)]
struct Segment {
    slots: Box<[AtomicU64]>,
}

impl Segment {
    fn new() -> Self {
        Self {
            slots: (0..SEGMENT_SLOTS)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline]
    fn slot(&self, offset: usize) -> &AtomicU64 {
        &self.slots[offset]
    }
}

#[derive(Debug)]
struct GaugeArena {
    next_slot: AtomicU32,
    segments: Box<[AtomicPtr<GaugeSegment>]>,
    capacity_lock: Mutex<()>,
}

impl GaugeArena {
    fn new() -> Self {
        Self {
            next_slot: AtomicU32::new(0),
            segments: (0..MAX_SEGMENTS)
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            capacity_lock: Mutex::new(()),
        }
    }

    fn allocate_slot(&self, value: i64) -> GaugeSlot {
        let index = self.next_slot.fetch_add(1, Ordering::Relaxed);
        let required = index as usize + 1;
        assert!(
            required <= MAX_SLOTS,
            "gauge arena exhausted: required {required} slots, max {MAX_SLOTS}",
        );
        self.ensure_capacity(required);
        let slot = GaugeSlot { index };
        self.store(slot, value);
        slot
    }

    fn ensure_capacity(&self, required_slots: usize) {
        let required_segments = required_slots.div_ceil(SEGMENT_SLOTS);
        let _guard = self.capacity_lock.lock().expect("gauge arena mutex poisoned");
        for index in 0..required_segments {
            let current = self.segments[index].load(Ordering::Acquire);
            if current.is_null() {
                let segment = Box::into_raw(Box::new(GaugeSegment::new()));
                self.segments[index].store(segment, Ordering::Release);
            }
        }
    }

    fn store(&self, slot: GaugeSlot, value: i64) {
        self.atomic(slot).store(value, Ordering::Relaxed);
    }

    fn load(&self, slot: GaugeSlot) -> i64 {
        self.atomic(slot).load(Ordering::Relaxed)
    }

    #[inline]
    fn atomic(&self, slot: GaugeSlot) -> &AtomicI64 {
        let index = slot.index as usize;
        let segment_index = index / SEGMENT_SLOTS;
        let offset = index % SEGMENT_SLOTS;
        debug_assert!(segment_index < MAX_SEGMENTS);
        let segment = self.segments[segment_index].load(Ordering::Acquire);
        debug_assert!(!segment.is_null(), "gauge segment must be published before use");
        // SAFETY: Gauge segments are process-lifetime allocations published
        // before their slots are returned, mirroring `VariableArena`.
        unsafe { (*segment).slot(offset) }
    }
}

#[derive(Debug)]
struct GaugeSegment {
    slots: Box<[AtomicI64]>,
}

impl GaugeSegment {
    fn new() -> Self {
        Self {
            slots: (0..SEGMENT_SLOTS)
                .map(|_| AtomicI64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline]
    fn slot(&self, offset: usize) -> &AtomicI64 {
        &self.slots[offset]
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::runtime::variables) struct Slot {
    index: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::runtime::variables) struct SlotRange {
    base: u32,
    width: u16,
}

impl SlotRange {
    #[inline]
    pub(in crate::runtime::variables) fn slot(self, offset: u32) -> Slot {
        debug_assert!(offset < self.width as u32);
        Slot { index: self.base + offset }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::runtime::variables) struct GaugeSlot {
    index: u32,
}

pub(in crate::runtime::variables) fn allocate_slot() -> Slot {
    ARENA.allocate_slot()
}

pub(in crate::runtime::variables) fn allocate_slots(width: u16) -> SlotRange {
    ARENA.allocate_slots(width)
}

#[inline]
pub(in crate::runtime::variables) fn add(slot: Slot, value: u64) {
    ARENA.add(slot, value);
}

#[inline]
pub(in crate::runtime::variables) fn add_signed(slot: Slot, value: i64) {
    ARENA.add_signed(slot, value);
}

#[inline]
pub(in crate::runtime::variables) fn add_on_shard(shard: usize, slot: Slot, value: u64) {
    ARENA.add_on_shard(shard, slot, value);
}

#[inline]
pub(in crate::runtime::variables) fn max_on_shard(shard: usize, slot: Slot, value: u64) {
    ARENA.max_on_shard(shard, slot, value);
}

pub(in crate::runtime::variables) fn sum(slot: Slot) -> u64 {
    ARENA.sum(slot)
}

pub(in crate::runtime::variables) fn sum_signed(slot: Slot) -> i64 {
    ARENA.sum_signed(slot)
}

pub(in crate::runtime::variables) fn max(slot: Slot) -> u64 {
    ARENA.max(slot)
}

pub(in crate::runtime::variables) fn allocate_gauge(value: i64) -> GaugeSlot {
    GAUGE_ARENA.allocate_slot(value)
}

pub(in crate::runtime::variables) fn store_gauge(slot: GaugeSlot, value: i64) {
    GAUGE_ARENA.store(slot, value);
}

pub(in crate::runtime::variables) fn load_gauge(slot: GaugeSlot) -> i64 {
    GAUGE_ARENA.load(slot)
}

#[inline]
pub(in crate::runtime::variables) fn current_shard() -> usize {
    SHARD_INDEX.with(|index| *index)
}
