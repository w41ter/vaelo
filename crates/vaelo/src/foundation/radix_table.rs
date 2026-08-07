// SPDX-License-Identifier: Apache-2.0

//! Concurrent append-only radix table.

use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

const RADIX_BITS: u32 = 9;
const RADIX_ENTRIES: usize = 1 << RADIX_BITS;
const RADIX_MASK: usize = RADIX_ENTRIES - 1;
const LEVELS: u32 = 3;
const CAPACITY: usize = 1 << (RADIX_BITS * LEVELS);

struct RadixNode<T> {
    entries: [AtomicPtr<T>; RADIX_ENTRIES],
    // Make the node inherit T's Send and Sync requirements. AtomicPtr alone
    // does not express that shared references to published values are exposed.
    marker: PhantomData<T>,
}

impl<T> RadixNode<T> {
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            marker: PhantomData,
        }
    }
}

impl<T> Drop for RadixNode<T> {
    fn drop(&mut self) {
        for entry in &mut self.entries {
            let value = *entry.get_mut();
            if !value.is_null() {
                // SAFETY: every non-null entry was produced by Box::into_raw,
                // won publication exactly once, and cannot be removed.
                unsafe { drop(Box::from_raw(value)) };
            }
        }
    }
}

type Leaf<T> = RadixNode<T>;
type Middle<T> = RadixNode<Leaf<T>>;
type Root<T> = RadixNode<Middle<T>>;

/// A sparse, concurrent append-only table indexed by 27-bit integers.
///
/// Values are atomically published and remain at a stable address for the
/// lifetime of the table. Concurrent insertion at the same index has exactly
/// one winner; each losing caller retains ownership of its value.
pub(crate) struct RadixTable<T> {
    root: Root<T>,
}

impl<T> RadixTable<T> {
    pub(crate) fn new() -> Self {
        Self { root: RadixNode::new() }
    }

    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        if index >= CAPACITY {
            return None;
        }

        let middle = self.root.entries[root_index(index)].load(Ordering::Acquire);
        if middle.is_null() {
            return None;
        }
        // SAFETY: published nodes are immutable and remain allocated until
        // the table is exclusively dropped.
        let leaf = unsafe { &*middle }.entries[middle_index(index)].load(Ordering::Acquire);
        if leaf.is_null() {
            return None;
        }
        let value = unsafe { &*leaf }.entries[leaf_index(index)].load(Ordering::Acquire);
        // SAFETY: as with nodes, published values cannot be removed.
        unsafe { value.as_ref() }
    }

    pub(crate) fn insert(&self, index: usize, value: T) -> Result<&T, T> {
        assert!(index < CAPACITY, "radix table index out of bounds");

        let middle = ensure_node(&self.root.entries[root_index(index)]);
        let leaf = ensure_node(&middle.entries[middle_index(index)]);
        let candidate = Box::into_raw(Box::new(value));
        match leaf.entries[leaf_index(index)].compare_exchange(
            ptr::null_mut(),
            candidate,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // SAFETY: this insertion published candidate, whose lifetime
                // is now tied to the table.
                Ok(unsafe { &*candidate })
            }
            Err(_) => {
                // SAFETY: candidate was not published and remains ours.
                Err(*unsafe { Box::from_raw(candidate) })
            }
        }
    }
}

fn ensure_node<T>(entry: &AtomicPtr<RadixNode<T>>) -> &RadixNode<T> {
    let mut node = entry.load(Ordering::Acquire);
    if node.is_null() {
        let candidate = Box::into_raw(Box::new(RadixNode::new()));
        match entry.compare_exchange(
            ptr::null_mut(),
            candidate,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => node = candidate,
            Err(actual) => {
                // SAFETY: the candidate lost publication and is unreachable
                // from the table.
                unsafe { drop(Box::from_raw(candidate)) };
                node = actual;
            }
        }
    }
    // SAFETY: node was either observed after publication or published by this
    // call, and append-only nodes live until the table is dropped.
    unsafe { &*node }
}

const fn root_index(index: usize) -> usize {
    (index >> (RADIX_BITS * 2)) & RADIX_MASK
}

const fn middle_index(index: usize) -> usize {
    (index >> RADIX_BITS) & RADIX_MASK
}

const fn leaf_index(index: usize) -> usize {
    index & RADIX_MASK
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use super::{CAPACITY, RadixTable};

    #[test]
    fn stores_sparse_values() {
        let table = RadixTable::new();
        for index in [0, 511, 512, 512 * 512, CAPACITY - 1] {
            assert_eq!(table.insert(index, index), Ok(&index));
        }
        for index in [0, 511, 512, 512 * 512, CAPACITY - 1] {
            assert_eq!(table.get(index), Some(&index));
        }
        assert_eq!(table.get(CAPACITY), None);
    }

    #[test]
    fn publishes_once_under_concurrent_insertion() {
        const THREADS: usize = 8;
        let table = RadixTable::new();
        let barrier = Arc::new(Barrier::new(THREADS));
        let winners = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for value in 0..THREADS {
                let barrier = barrier.clone();
                let table = &table;
                let winners = &winners;
                scope.spawn(move || {
                    barrier.wait();
                    if table.insert(123_456, value).is_ok() {
                        winners.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });

        assert_eq!(winners.load(Ordering::Relaxed), 1);
        assert!(table.get(123_456).is_some());
    }

    #[test]
    fn drops_published_and_rejected_values() {
        #[derive(Debug)]
        struct CountDrops(Arc<AtomicUsize>);

        impl Drop for CountDrops {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        {
            let table = RadixTable::new();
            table.insert(1, CountDrops(drops.clone())).unwrap();
            assert!(table.insert(1, CountDrops(drops.clone())).is_err());
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
