// SPDX-License-Identifier: Apache-2.0

//! Blocked list of generation-checked IDs with lazy removal.
//!
//! Completion only has to make an ID logically stale. New insertions reuse
//! stale slots and occasional compaction drops the remaining stale entries.
//! The collection is intentionally not thread-safe; callers can put a short
//! critical section around insertion and detach the whole list before running
//! potentially expensive callbacks.

use std::marker::PhantomData;

use super::resource_pool::{Resource, ResourceId, StaticResource};

const EMPTY: u64 = u64::MAX;
const BLOCK_SIZE: usize = 63;
const PROBE_COUNT: usize = 4;
const DEFAULT_INITIAL_GC_CAPACITY: usize = 4 * 1024;
const DEFAULT_MAX_CAPACITY: usize = 100_000;

/// Defines when an addressable resource is still a member of the logical list.
///
/// Resource generation checks reject recycled slots before `is_live` is
/// called. An adapter may additionally check a monotonic operation state or a
/// connection generation.
pub(crate) trait StaleIdListAdapter {
    type Value: StaticResource;

    fn is_live(&self, value: &Self::Value) -> bool;

    fn resolve(&self, id: ResourceId<Self::Value>) -> Option<Resource<Self::Value>> {
        Self::Value::resource_pool().get(id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StaleIdListError {
    CapacityExceeded,
}

/// An ID list optimized for frequent insertion, asynchronous invalidation and
/// infrequent exhaustive traversal.
///
/// The list does not own resources. A live resource must be kept alive by its
/// operation, while generation checks make IDs left by recycled resources
/// harmless. Logical removal never mutates this list.
pub(crate) struct StaleIdList<A: StaleIdListAdapter> {
    // Blocks must keep stable addresses while intrusive readers hold pointers.
    #[allow(clippy::vec_box)]
    blocks: Vec<Box<IdBlock>>,
    cursor: usize,
    next_gc_capacity: usize,
    max_capacity: usize,
    adapter: A,
    marker: PhantomData<fn() -> A::Value>,
}

// 63 IDs plus alignment padding occupy the same 512 bytes as bthread's block
// containing 63 IDs and a next pointer.
#[repr(align(64))]
struct IdBlock {
    ids: [u64; BLOCK_SIZE],
}

impl IdBlock {
    fn new() -> Self {
        Self { ids: [EMPTY; BLOCK_SIZE] }
    }
}

impl<A: StaleIdListAdapter> StaleIdList<A> {
    pub(crate) fn new(adapter: A) -> Self {
        Self::with_limits(adapter, DEFAULT_INITIAL_GC_CAPACITY, DEFAULT_MAX_CAPACITY)
    }

    fn with_limits(adapter: A, initial_gc_capacity: usize, max_capacity: usize) -> Self {
        assert!(initial_gc_capacity > 0);
        assert!(max_capacity >= BLOCK_SIZE);
        Self {
            blocks: vec![Box::new(IdBlock::new())],
            cursor: 0,
            next_gc_capacity: initial_gc_capacity.min(max_capacity),
            max_capacity,
            adapter,
            marker: PhantomData,
        }
    }

    /// Exchanges the complete lists, including their adapters and GC state.
    ///
    /// Moving the adapter together with the IDs preserves stateful liveness
    /// predicates, such as the socket generation associated with a detached
    /// pending-call snapshot.
    pub(crate) fn swap(&mut self, other: &mut Self) {
        std::mem::swap(self, other);
    }

    /// Adds an ID, normally replacing one of at most four stale slots.
    pub(crate) fn add(&mut self, id: ResourceId<A::Value>) -> Result<(), StaleIdListError> {
        let raw = id.as_u64();
        assert_ne!(raw, EMPTY, "ResourceId collides with stale-list empty value");
        let mut attempted_gc = false;

        loop {
            for _ in 0..PROBE_COUNT {
                let position = self.cursor;
                self.advance();
                let old = self.get(position);
                if old == EMPTY || !self.is_live_raw(old) {
                    self.set(position, raw);
                    return Ok(());
                }
            }

            if !attempted_gc && self.capacity() >= self.next_gc_capacity {
                let before = self.capacity();
                self.gc();
                attempted_gc = true;
                if self.capacity() * 4 > before {
                    self.next_gc_capacity =
                        self.next_gc_capacity.saturating_mul(2).min(self.max_capacity);
                }
                continue;
            }

            self.grow()?;
            let position = self.cursor;
            self.set(position, raw);
            self.advance();
            return Ok(());
        }
    }

    /// Visits IDs which are live when their slot is inspected.
    ///
    /// The list itself is not synchronized. For exhaustive failure handling,
    /// detach it under the producer's lock and traverse it after unlocking.
    pub(crate) fn for_each_live(&self, mut visit: impl FnMut(Resource<A::Value>)) {
        for block in &self.blocks {
            for &raw in &block.ids {
                if raw == EMPTY {
                    continue;
                }
                if let Some(value) = self.resolve_raw(raw)
                    && self.adapter.is_live(&value)
                {
                    visit(value);
                }
            }
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.blocks.len() * BLOCK_SIZE
    }

    fn grow(&mut self) -> Result<(), StaleIdListError> {
        if self.capacity().saturating_add(BLOCK_SIZE) > self.max_capacity {
            return Err(StaleIdListError::CapacityExceeded);
        }
        self.blocks.push(Box::new(IdBlock::new()));
        self.cursor = (self.blocks.len() - 1) * BLOCK_SIZE;
        Ok(())
    }

    fn gc(&mut self) {
        let mut live = Vec::new();
        for block in &self.blocks {
            for &raw in &block.ids {
                if raw != EMPTY && self.is_live_raw(raw) {
                    live.push(raw);
                }
            }
        }

        let max_blocks = self.max_capacity / BLOCK_SIZE;
        let desired_slots = live.len().saturating_mul(2).max(1);
        let required_blocks = desired_slots.div_ceil(BLOCK_SIZE).min(max_blocks);
        let mut blocks = Vec::with_capacity(required_blocks);
        blocks.resize_with(required_blocks, || Box::new(IdBlock::new()));

        let spacing = usize::from(live.len().saturating_mul(2) <= required_blocks * BLOCK_SIZE) + 1;
        let mut position = 0;
        for raw in live {
            blocks[position / BLOCK_SIZE].ids[position % BLOCK_SIZE] = raw;
            position += spacing;
        }
        self.blocks = blocks;
        self.cursor = position % self.capacity();
    }

    fn is_live_raw(&self, raw: u64) -> bool {
        self.resolve_raw(raw).is_some_and(|value| self.adapter.is_live(&value))
    }

    fn resolve_raw(&self, raw: u64) -> Option<Resource<A::Value>> {
        self.adapter.resolve(resource_id(raw))
    }

    fn advance(&mut self) {
        self.cursor += 1;
        if self.cursor == self.capacity() {
            self.cursor = 0;
        }
    }

    fn get(&self, position: usize) -> u64 {
        self.blocks[position / BLOCK_SIZE].ids[position % BLOCK_SIZE]
    }

    fn set(&mut self, position: usize, raw: u64) {
        self.blocks[position / BLOCK_SIZE].ids[position % BLOCK_SIZE] = raw;
    }
}

impl<A: StaleIdListAdapter + Default> Default for StaleIdList<A> {
    fn default() -> Self {
        Self::new(A::default())
    }
}

fn resource_id<T>(raw: u64) -> ResourceId<T> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::foundation::resource_pool::{ResourceLocal, ResourcePool, ResourcePoolConfig};

    struct Entry {
        live: AtomicBool,
    }

    impl Default for Entry {
        fn default() -> Self {
            Self { live: AtomicBool::new(false) }
        }
    }

    static POOL: LazyLock<ResourcePool<Entry>> =
        LazyLock::new(|| ResourcePool::new(ResourcePoolConfig { max_items: 64 * 1024 }));
    thread_local! {
        static LOCAL: RefCell<ResourceLocal<Entry>> =
            RefCell::new(ResourceLocal::new(&POOL));
    }

    impl StaticResource for Entry {
        fn resource_pool() -> &'static ResourcePool<Self> {
            &POOL
        }

        fn try_with_resource_local<R>(
            operation: impl FnOnce(&mut ResourceLocal<Self>) -> R,
        ) -> Option<R> {
            LOCAL.try_with(|local| operation(&mut local.borrow_mut())).ok()
        }
    }

    #[derive(Default)]
    struct Live;

    impl StaleIdListAdapter for Live {
        type Value = Entry;

        fn is_live(&self, value: &Entry) -> bool {
            value.live.load(Ordering::Acquire)
        }
    }

    fn entry() -> Resource<Entry> {
        let value = POOL.acquire().unwrap();
        value.live.store(true, Ordering::Release);
        value
    }

    #[test]
    fn block_has_expected_cache_friendly_size() {
        assert_eq!(std::mem::size_of::<IdBlock>(), 512);
    }

    #[test]
    fn traversal_only_visits_logically_live_resources() {
        let mut list = StaleIdList::new(Live);
        let live = entry();
        let completed = entry();
        list.add(live.id()).unwrap();
        list.add(completed.id()).unwrap();
        completed.live.store(false, Ordering::Release);

        let mut visited = Vec::new();
        list.for_each_live(|value| visited.push(value.id()));
        assert_eq!(visited, [live.id()]);
    }

    #[test]
    fn insertion_reuses_stale_slots_without_growing() {
        let mut list = StaleIdList::new(Live);
        for _ in 0..1_000 {
            let value = entry();
            list.add(value.id()).unwrap();
            value.live.store(false, Ordering::Release);
        }
        assert_eq!(list.capacity(), BLOCK_SIZE);
    }

    #[test]
    fn recycled_resource_generation_invalidates_old_id() {
        let mut list = StaleIdList::new(Live);
        let old = entry();
        let old_id = old.id();
        list.add(old_id).unwrap();
        drop(old);

        let replacement = entry();
        assert_ne!(replacement.id(), old_id);
        list.add(replacement.id()).unwrap();

        let mut visited = Vec::new();
        list.for_each_live(|value| visited.push(value.id()));
        assert_eq!(visited, [replacement.id()]);
    }

    #[test]
    fn gc_drops_stale_ids_and_keeps_space_between_live_ids() {
        let mut list = StaleIdList::with_limits(Live, BLOCK_SIZE, BLOCK_SIZE * 8);
        let values: Vec<_> = (0..BLOCK_SIZE).map(|_| entry()).collect();
        for value in &values {
            list.add(value.id()).unwrap();
        }
        for value in values.iter().skip(4) {
            value.live.store(false, Ordering::Release);
        }

        let extra = entry();
        list.add(extra.id()).unwrap();
        assert_eq!(list.capacity(), BLOCK_SIZE);

        let mut visited = Vec::new();
        list.for_each_live(|value| visited.push(value.id()));
        assert_eq!(visited.len(), 5);
        assert!(visited.contains(&extra.id()));
    }

    #[test]
    fn reports_capacity_exhaustion_when_all_ids_stay_live() {
        let mut list = StaleIdList::with_limits(Live, BLOCK_SIZE * 8, BLOCK_SIZE);
        let values: Vec<_> = (0..=BLOCK_SIZE).map(|_| entry()).collect();
        for value in values.iter().take(BLOCK_SIZE) {
            list.add(value.id()).unwrap();
        }
        assert_eq!(list.add(values[BLOCK_SIZE].id()), Err(StaleIdListError::CapacityExceeded));
    }

    #[test]
    fn swap_exchanges_all_list_state() {
        let mut populated = StaleIdList::new(Live);
        let value = entry();
        populated.add(value.id()).unwrap();
        let mut empty = StaleIdList::new(Live);

        populated.swap(&mut empty);

        let mut old_values = Vec::new();
        populated.for_each_live(|entry| old_values.push(entry.id()));
        assert!(old_values.is_empty());
        let mut detached_values = Vec::new();
        empty.for_each_live(|entry| detached_values.push(entry.id()));
        assert_eq!(detached_values, [value.id()]);
    }
}
