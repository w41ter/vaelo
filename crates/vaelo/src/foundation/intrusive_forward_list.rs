// SPDX-License-Identifier: Apache-2.0

//! Allocation-free intrusive forward list for push-and-take workloads.
//!
//! The list owns one [`Resource`] reference per linked node. `take` atomically
//! detaches the current chain and returns an iterator which reconstructs those
//! references as nodes are consumed.

use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

use super::resource_pool::{Resource, ResourceId, StaticResource};

const NULL: u64 = u64::MAX;

/// One singly-linked membership hook embedded in a ResourcePool object.
///
/// An object may contain any number of links. An adapter instance selects which
/// one a particular list uses.
pub(crate) struct IntrusiveForwardListLink<T> {
    next: AtomicU64,
    marker: PhantomData<fn() -> T>,
}

impl<T> IntrusiveForwardListLink<T> {
    pub(crate) const fn new() -> Self {
        Self { next: AtomicU64::new(NULL), marker: PhantomData }
    }
}

impl<T> Default for IntrusiveForwardListLink<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Adapts an intrusive value and one of its embedded links to a forward list.
pub(crate) trait IntrusiveForwardListAdapter: Send + Sync + 'static {
    type Value: StaticResource;

    fn link(value: &Self::Value) -> &IntrusiveForwardListLink<Self::Value>;

    /// Reconstructs the reference transferred to the list by `push`.
    ///
    /// # Safety
    /// The id must come from a node in a chain uniquely detached by `take`.
    unsafe fn reclaim(id: ResourceId<Self::Value>) -> Resource<Self::Value> {
        unsafe { Self::Value::resource_pool().reclaim_owned_id(id) }
    }
}

/// Allocation-free intrusive forward list optimized for concurrent push and
/// batch take.
pub(crate) struct IntrusiveForwardList<A: IntrusiveForwardListAdapter> {
    head: AtomicU64,
    marker: PhantomData<fn() -> A>,
}

impl<A: IntrusiveForwardListAdapter> IntrusiveForwardList<A> {
    pub(crate) const fn new() -> Self {
        Self { head: AtomicU64::new(NULL), marker: PhantomData }
    }

    /// Inserts at the front and transfers the supplied Resource reference to
    /// the list.
    ///
    /// The caller is responsible for ensuring the selected link is not already
    /// linked. This list deliberately stores no membership state in the hook.
    pub(crate) fn push(&self, node: Resource<A::Value>) {
        let link = A::link(&node);
        let raw = node.id().as_u64();
        assert_ne!(raw, NULL, "ResourceId collides with intrusive forward-list null value");
        let link = NonNull::from(link);
        let _ = node.into_owned_id();
        let link = unsafe { link.as_ref() };

        let mut old_head = self.head.load(Ordering::Acquire);
        loop {
            link.next.store(old_head, Ordering::Relaxed);
            match self.head.compare_exchange_weak(
                old_head,
                raw,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => old_head = actual,
            }
        }
    }

    /// Atomically detaches the current chain and returns an iterator over it.
    ///
    /// Concurrent pushes either appear in this taken chain or remain linked in
    /// the list for a later `take`.
    pub(crate) fn take(&self) -> IntrusiveForwardListTake<A> {
        IntrusiveForwardListTake {
            current: self.head.swap(NULL, Ordering::AcqRel),
            marker: PhantomData,
        }
    }
}

impl<A: IntrusiveForwardListAdapter> Default for IntrusiveForwardList<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: IntrusiveForwardListAdapter> Drop for IntrusiveForwardList<A> {
    fn drop(&mut self) {
        for _ in self.take() {}
    }
}

pub(crate) struct IntrusiveForwardListTake<A: IntrusiveForwardListAdapter> {
    current: u64,
    marker: PhantomData<fn() -> A>,
}

impl<A: IntrusiveForwardListAdapter> Iterator for IntrusiveForwardListTake<A> {
    type Item = Resource<A::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == NULL {
            return None;
        }

        let id = id(self.current);
        let node = unsafe { A::reclaim(id) };
        let link = A::link(&node);
        self.current = link.next.load(Ordering::Acquire);
        link.next.store(NULL, Ordering::Relaxed);
        Some(node)
    }
}

impl<A: IntrusiveForwardListAdapter> Drop for IntrusiveForwardListTake<A> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

fn id<T>(raw: u64) -> ResourceId<T> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::foundation::resource_pool::{ResourceLocal, ResourcePool, ResourcePoolConfig};

    struct Entry {
        ready: IntrusiveForwardListLink<Self>,
        retry: IntrusiveForwardListLink<Self>,
        value: AtomicUsize,
    }

    impl Default for Entry {
        fn default() -> Self {
            Self {
                ready: IntrusiveForwardListLink::new(),
                retry: IntrusiveForwardListLink::new(),
                value: AtomicUsize::new(0),
            }
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
    struct Ready;
    impl IntrusiveForwardListAdapter for Ready {
        type Value = Entry;

        fn link(value: &Entry) -> &IntrusiveForwardListLink<Entry> {
            &value.ready
        }
    }

    #[derive(Default)]
    struct Retry;
    impl IntrusiveForwardListAdapter for Retry {
        type Value = Entry;

        fn link(value: &Entry) -> &IntrusiveForwardListLink<Entry> {
            &value.retry
        }
    }

    fn entry(value: usize) -> Resource<Entry> {
        let entry = POOL.acquire().expect("intrusive forward-list test pool exhausted");
        entry.value.store(value, Ordering::Relaxed);
        entry
    }

    #[test]
    fn adapter_owns_value_type_and_selects_one_of_multiple_links() {
        let ready = IntrusiveForwardList::<Ready>::new();
        let retry = IntrusiveForwardList::<Retry>::new();
        assert_eq!(std::mem::size_of_val(&ready), std::mem::size_of::<u64>());
        let node = entry(1);
        ready.push(node.clone());
        retry.push(node.clone());

        let ready_nodes: Vec<_> = ready.take().collect();
        assert_eq!(ready_nodes.len(), 1);
        assert_eq!(ready_nodes[0].id(), node.id());

        let retry_nodes: Vec<_> = retry.take().collect();
        assert_eq!(retry_nodes.len(), 1);
        assert_eq!(retry_nodes[0].id(), node.id());
    }

    #[test]
    fn take_detaches_the_current_chain() {
        let list = IntrusiveForwardList::<Ready>::new();
        let owners: Vec<_> = (0..1_000)
            .map(|value| {
                let node = entry(value);
                list.push(node.clone());
                node
            })
            .collect();

        let mut values: Vec<_> =
            list.take().map(|node| node.value.load(Ordering::Relaxed)).collect();
        values.sort_unstable();
        assert_eq!(values, (0..1_000).collect::<Vec<_>>());
        assert!(list.take().next().is_none());
        assert_eq!(owners.len(), 1_000);
    }

    #[test]
    fn dropped_take_releases_unvisited_nodes() {
        let list = IntrusiveForwardList::<Ready>::new();
        let owners: Vec<_> = (0..128)
            .map(|value| {
                let node = entry(value);
                list.push(node.clone());
                node
            })
            .collect();

        drop(list.take());
        assert!(list.take().next().is_none());
        assert_eq!(owners.len(), 128);
    }

    #[test]
    fn concurrent_push_and_take_preserve_all_owners() {
        const THREADS: usize = 4;
        const ITEMS: usize = 4_000;
        let list = std::sync::Arc::new(IntrusiveForwardList::<Ready>::new());
        let mut workers = Vec::new();
        for thread in 0..THREADS {
            let list = list.clone();
            workers.push(std::thread::spawn(move || {
                let mut owners = Vec::new();
                for index in 0..ITEMS {
                    let node = entry(thread * ITEMS + index);
                    list.push(node.clone());
                    owners.push(node);
                }
                owners
            }));
        }

        let mut taken = 0;
        while workers.iter().any(|worker| !worker.is_finished()) {
            taken += list.take().count();
            std::thread::yield_now();
        }
        let owners: Vec<_> = workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("intrusive forward-list worker panicked"))
            .collect();
        taken += list.take().count();

        assert_eq!(taken, THREADS * ITEMS);
        assert!(list.take().next().is_none());
        assert_eq!(owners.len(), THREADS * ITEMS);
    }
}
