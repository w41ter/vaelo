// SPDX-License-Identifier: Apache-2.0

//! Read-optimized concurrent hash map based on bucket-local copy-on-write.
//!
//! Each bucket publishes an immutable `hashbrown::HashMap` snapshot. Readers
//! pin an epoch guard, load one bucket pointer, and look up in that immutable
//! map without taking a lock. Writers lock only the target bucket, clone that
//! bucket's snapshot, mutate the clone, publish it, and retire the old
//! snapshot.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard};

use crossbeam_epoch::{self as epoch, Atomic, Owned};
use hashbrown::HashMap;
use hashbrown::hash_map::RawEntryMut;

pub(crate) const DEFAULT_BUCKETS: usize = 64;

pub(crate) struct CowHashMap<K, V, S = RandomState, const N: usize = DEFAULT_BUCKETS> {
    buckets: [Bucket<K, V, S>; N],
    bucket_mask: usize,
    hash_builder: S,
}

struct Bucket<K, V, S> {
    write: Mutex<()>,
    table: Atomic<BucketTable<K, V, S>>,
}

struct BucketTable<K, V, S> {
    entries: HashMap<K, V, S>,
}

impl<K, V, const N: usize> CowHashMap<K, V, RandomState, N>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(crate) fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl<K, V, S, const N: usize> CowHashMap<K, V, S, N>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    pub(crate) fn with_hasher(hash_builder: S) -> Self {
        assert!(N > 0, "CowHashMap requires at least one bucket");
        assert!(N.is_power_of_two(), "CowHashMap bucket count must be a power of two");
        Self {
            buckets: std::array::from_fn(|_| Bucket::new(hash_builder.clone())),
            bucket_mask: N - 1,
            hash_builder,
        }
    }

    /// Returns an existing value or inserts a freshly created one.
    ///
    /// The creator runs while the target bucket's writer lock is held. That
    /// preserves exactly-once creation per key and matches the current session
    /// cache behavior, where creation is already performed under a map lock.
    pub(crate) fn get_or_try_insert_with<E>(
        &self,
        key: K,
        create: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, E> {
        let hash = self.hash(&key);
        if let Some(value) = self.get_hashed_with(hash, &key, Clone::clone) {
            return Ok(value);
        }

        let bucket = self.bucket(hash);
        let _lock = bucket.lock_writer();
        let guard = epoch::pin();
        let old = bucket.load(&guard);
        if let Some(value) = old.get(hash, &key) {
            return Ok(value.clone());
        }

        let value = create()?;
        let mut entries = old.entries.clone();
        let previous = insert_hashed(&mut entries, hash, key, value.clone());
        debug_assert!(previous.is_none());
        bucket.install(entries, &guard);
        Ok(value)
    }

    pub(crate) fn remove(&self, key: &K) -> Option<V> {
        self.remove_if(key, |_| true)
    }

    /// Removes `key` only when the current value still matches `predicate`.
    ///
    /// Connection maintenance uses this to ensure that a stale observation
    /// cannot remove a replacement installed under the same key.
    pub(crate) fn remove_if(&self, key: &K, predicate: impl FnOnce(&V) -> bool) -> Option<V> {
        let hash = self.hash(key);
        let bucket = self.bucket(hash);
        let _lock = bucket.lock_writer();
        let guard = epoch::pin();
        let old = bucket.load(&guard);
        let value = old.get(hash, key)?;
        if !predicate(value) {
            return None;
        }

        let mut entries = old.entries.clone();
        let removed = entries.remove(key);
        debug_assert!(removed.is_some());
        bucket.install(entries, &guard);
        removed
    }

    /// Visits one immutable bucket snapshot without taking its writer lock.
    ///
    /// The visitor must not retain borrowed keys or values. Mutations based on
    /// an observation should happen after this method returns and use
    /// [`remove_if`](Self::remove_if) to validate the observed identity.
    pub(crate) fn visit_bucket(&self, index: usize, mut visitor: impl FnMut(&K, &V)) {
        let guard = epoch::pin();
        let table = self.buckets[index & self.bucket_mask].load(&guard);
        for (key, value) in &table.entries {
            visitor(key, value);
        }
    }

    fn get_hashed_with<R>(&self, hash: u64, key: &K, read: impl FnOnce(&V) -> R) -> Option<R> {
        let guard = epoch::pin();
        let table = self.bucket(hash).load(&guard);
        table.get(hash, key).map(read)
    }

    fn bucket(&self, hash: u64) -> &Bucket<K, V, S> {
        &self.buckets[hash as usize & self.bucket_mask]
    }

    fn hash(&self, key: &K) -> u64 {
        self.hash_builder.hash_one(key)
    }
}

impl<K, V, S> Bucket<K, V, S>
where
    S: BuildHasher + Clone,
{
    fn new(hash_builder: S) -> Self {
        Self {
            write: Mutex::new(()),
            table: Atomic::new(BucketTable { entries: HashMap::with_hasher(hash_builder) }),
        }
    }

    fn load<'g>(&self, guard: &'g epoch::Guard) -> &'g BucketTable<K, V, S> {
        let table = self.table.load(Ordering::Acquire, guard);
        debug_assert!(!table.is_null());
        unsafe { table.deref() }
    }

    fn lock_writer(&self) -> MutexGuard<'_, ()> {
        self.write
            .lock()
            .expect("CowHashMap bucket writer mutex poisoned while preparing a new snapshot")
    }

    fn install(&self, entries: HashMap<K, V, S>, guard: &epoch::Guard) {
        let new = Owned::new(BucketTable { entries });
        let old = self.table.swap(new, Ordering::AcqRel, guard);
        unsafe { guard.defer_destroy(old) };
    }
}

impl<K, V, S> Drop for Bucket<K, V, S> {
    fn drop(&mut self) {
        let table = mem::replace(&mut self.table, Atomic::null());
        unsafe {
            if let Some(table) = table.try_into_owned() {
                drop(table);
            }
        }
    }
}

impl<K, V, S> BucketTable<K, V, S>
where
    K: Eq,
{
    fn get(&self, hash: u64, key: &K) -> Option<&V> {
        self.entries
            .raw_entry()
            .from_hash(hash, |candidate| candidate == key)
            .map(|(_, value)| value)
    }
}

fn insert_hashed<K, V, S>(entries: &mut HashMap<K, V, S>, hash: u64, key: K, value: V) -> Option<V>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    match entries.raw_entry_mut().from_hash(hash, |candidate| candidate == &key) {
        RawEntryMut::Occupied(mut entry) => Some(entry.insert(value)),
        RawEntryMut::Vacant(entry) => {
            entry.insert_hashed_nocheck(hash, key, value);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, Hasher};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::CowHashMap;
    use crate::transport::connection::ConnectionKey;

    #[derive(Clone)]
    struct ConstantHasherBuilder;

    impl BuildHasher for ConstantHasherBuilder {
        type Hasher = ConstantHasher;

        fn build_hasher(&self) -> Self::Hasher {
            ConstantHasher
        }
    }

    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    #[test]
    #[should_panic(expected = "CowHashMap requires at least one bucket")]
    fn rejects_zero_buckets() {
        let _ = CowHashMap::<i32, i32, _, 0>::new();
    }

    #[test]
    #[should_panic(expected = "CowHashMap bucket count must be a power of two")]
    fn rejects_non_power_of_two_buckets() {
        let _ = CowHashMap::<i32, i32, _, 3>::new();
    }

    #[test]
    fn handles_hash_collisions_inside_bucket() {
        let map = CowHashMap::<_, _, ConstantHasherBuilder, 1>::with_hasher(ConstantHasherBuilder);
        for key in 0..64 {
            assert_eq!(map.get_or_try_insert_with(key, || Ok::<_, ()>(key * 10)), Ok(key * 10));
        }
        for key in 0..64 {
            assert_eq!(
                map.get_or_try_insert_with(key, || panic!("existing key must skip creator")),
                Ok::<_, ()>(key * 10)
            );
        }
    }

    #[test]
    fn failed_create_is_retried() {
        let map = CowHashMap::<_, _, _, 4>::new();
        let created = AtomicUsize::new(0);

        let failed: Result<i32, &str> = map.get_or_try_insert_with("session", || {
            created.fetch_add(1, Ordering::Relaxed);
            Err("create failed")
        });

        assert_eq!(failed, Err("create failed"));
        assert_eq!(created.load(Ordering::Relaxed), 1);

        let created_after_retry: Result<_, &str> = map.get_or_try_insert_with("session", || {
            created.fetch_add(1, Ordering::Relaxed);
            Ok(7)
        });

        assert_eq!(created_after_retry, Ok(7));
        assert_eq!(created.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn existing_value_skips_creator() {
        let map = CowHashMap::<_, _, _, 4>::new();
        let created = AtomicUsize::new(0);

        assert_eq!(map.get_or_try_insert_with("session", || Ok::<_, ()>(7)), Ok(7));
        let value: Result<_, ()> = map.get_or_try_insert_with("session", || {
            created.fetch_add(1, Ordering::Relaxed);
            Ok(8)
        });

        assert_eq!(value, Ok(7));
        assert_eq!(created.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn remove_publishes_a_snapshot_without_the_entry() {
        let map = CowHashMap::<_, _, _, 4>::new();
        assert_eq!(map.get_or_try_insert_with("session", || Ok::<_, ()>(7)), Ok(7));
        assert_eq!(map.remove(&"session"), Some(7));
        assert_eq!(map.remove(&"session"), None);
        assert_eq!(map.get_or_try_insert_with("session", || Ok::<_, ()>(8)), Ok(8));
    }

    #[test]
    fn connection_key_can_be_inserted_and_read_back() {
        let remote: SocketAddr =
            "127.0.0.1:8080".parse().expect("test connection key address should parse");
        let key = ConnectionKey { remote, endpoint_id: 7, tag: 42, slot: 0, connect_timeout: None };
        let same_key = key;
        let different_tag = ConnectionKey { tag: 43, ..key };

        let map = CowHashMap::<ConnectionKey, &'static str, _, 8>::new();
        assert_eq!(map.get_or_try_insert_with(key, || Ok::<_, ()>("session")), Ok("session"));
        assert_eq!(
            map.get_or_try_insert_with(same_key, || panic!("same key must reuse session")),
            Ok::<_, ()>("session")
        );
        assert_eq!(map.get_or_try_insert_with(different_tag, || Ok::<_, ()>("other")), Ok("other"));
    }

    #[test]
    fn creates_once_for_same_key() {
        let map = Arc::new(CowHashMap::<_, _, _, 8>::new());
        let created = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let map = map.clone();
                let created = created.clone();
                scope.spawn(move || {
                    let value: Result<_, ()> = map.get_or_try_insert_with("session", || {
                        created.fetch_add(1, Ordering::Relaxed);
                        Ok(Arc::new(7))
                    });
                    assert_eq!(
                        *value
                            .expect("CowHashMap get_or_try_insert_with test should create a value"),
                        7
                    );
                });
            }
        });

        assert_eq!(created.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn conditional_remove_preserves_replacement_identity() {
        let map = CowHashMap::<_, _, _, 4>::new();
        assert_eq!(map.get_or_try_insert_with("session", || Ok::<_, ()>(7)), Ok(7));
        assert_eq!(map.remove_if(&"session", |value| *value == 8), None);
        assert_eq!(
            map.get_or_try_insert_with("session", || panic!("entry must remain")),
            Ok::<_, ()>(7)
        );
        assert_eq!(map.remove_if(&"session", |value| *value == 7), Some(7));
    }

    #[test]
    fn visits_one_bucket_snapshot() {
        let map = CowHashMap::<_, _, _, 4>::new();
        for key in 0..16 {
            map.get_or_try_insert_with(key, || Ok::<_, ()>(key * 2)).unwrap();
        }
        let mut visited = 0;
        for bucket in 0..4 {
            map.visit_bucket(bucket, |key, value| {
                assert_eq!(*value, *key * 2);
                visited += 1;
            });
        }
        assert_eq!(visited, 16);
    }

    #[test]
    fn supports_concurrent_creators() {
        const WRITERS: usize = 4;
        const KEYS_PER_WRITER: usize = 256;
        let map = Arc::new(CowHashMap::<_, _, _, 32>::new());

        std::thread::scope(|scope| {
            for writer in 0..WRITERS {
                let map = map.clone();
                scope.spawn(move || {
                    for key in 0..KEYS_PER_WRITER {
                        let key = writer * KEYS_PER_WRITER + key;
                        assert_eq!(map.get_or_try_insert_with(key, || Ok::<_, ()>(key)), Ok(key));
                    }
                });
            }
            for _ in 0..4 {
                let map = map.clone();
                scope.spawn(move || {
                    for _ in 0..16 {
                        for key in 0..(WRITERS * KEYS_PER_WRITER) {
                            let _ = map.get_or_try_insert_with(key, || Ok::<_, ()>(key));
                        }
                    }
                });
            }
        });

        for key in 0..(WRITERS * KEYS_PER_WRITER) {
            assert_eq!(
                map.get_or_try_insert_with(key, || panic!("all keys must exist")),
                Ok::<_, ()>(key)
            );
        }
    }
}
