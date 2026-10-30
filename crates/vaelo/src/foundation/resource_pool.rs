// SPDX-License-Identifier: Apache-2.0

//! Bounded, generation-checked resource storage.
//!
//! Resources live in 16 KiB blocks addressed through two 16 KiB pointer
//! directories. Freed slot indices move between threads in intrusive 1 KiB
//! chunks; the common acquire/recycle paths therefore require no mutex.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hasher};
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit, size_of};
use std::ops::Deref;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use crossbeam_epoch::{self as epoch, Atomic, Guard, Shared};

use super::{FreeChunk, FreeChunkTable, FreeList};

const PAGE_BYTES: usize = 16 * 1024;
const DIRECTORY_ENTRIES: usize = PAGE_BYTES / size_of::<AtomicPtr<()>>();
const NO_BLOCK: u32 = u32::MAX;

const GENERATION_MASK: u64 = u32::MAX as u64;
const REFERENCES_SHIFT: u32 = 32;
const REFERENCES_BITS: u32 = 29;
const MAX_REFERENCES: u32 = (1 << REFERENCES_BITS) - 1;
const REFERENCES_MASK: u64 = (MAX_REFERENCES as u64) << REFERENCES_SHIFT;
const STATE_SHIFT: u32 = 61;
const STATE_MASK: u64 = 0b11 << STATE_SHIFT;

// A FRESH slot's active token is owned by its block. A FREE slot's token is
// owned by exactly one FreeChunk entry or by the final owner immediately before
// it recycles the slot. ALLOCATED slots are protected by their reference count.
const STATE_FRESH: u64 = 0;
const STATE_ALLOCATED: u64 = 1;
const STATE_FREE: u64 = 2;

const BLOCK_ACTIVE: u8 = 0;

static NEXT_GENERATION_SEED: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug)]
pub struct ResourcePoolConfig {
    pub max_items: usize,
}

impl Default for ResourcePoolConfig {
    fn default() -> Self {
        Self { max_items: DIRECTORY_ENTRIES * DIRECTORY_ENTRIES }
    }
}

#[repr(transparent)]
pub struct ResourceId<T> {
    raw: u64,
    marker: PhantomData<fn() -> T>,
}

impl<T> ResourceId<T> {
    pub const fn from_parts(index: u32, generation: u32) -> Self {
        Self { raw: ((generation as u64) << 32) | index as u64, marker: PhantomData }
    }

    pub const fn index(self) -> u32 {
        self.raw as u32
    }

    pub const fn generation(self) -> u32 {
        (self.raw >> 32) as u32
    }

    pub const fn as_u64(self) -> u64 {
        self.raw
    }
}

impl<T> Copy for ResourceId<T> {}
impl<T> Clone for ResourceId<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> PartialEq for ResourceId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}
impl<T> Eq for ResourceId<T> {}
impl<T> std::hash::Hash for ResourceId<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}
impl<T> std::fmt::Debug for ResourceId<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ResourceId").field(&self.raw).finish()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ResourcePoolError {
    Exhausted,
    ReferenceOverflow,
}

pub trait StaticResource: Sized + Send + Sync + 'static {
    fn resource_pool() -> &'static ResourcePool<Self>;

    /// Returns `None` when the thread-local cache is being or has been
    /// destroyed. Resource destruction can run during TLS teardown, so callers
    /// must be able to fall back to the pool's global free list.
    #[doc(hidden)]
    fn try_with_resource_local<R>(
        operation: impl FnOnce(&mut ResourceLocal<Self>) -> R,
    ) -> Option<R>;
}

struct Slot<T> {
    metadata: AtomicU64,
    value: UnsafeCell<MaybeUninit<T>>,
}

struct AcquiredSlot<T> {
    index: u32,
    generation: u32,
    slot: NonNull<Slot<T>>,
}

impl<T> AcquiredSlot<T> {
    fn id(&self) -> ResourceId<T> {
        ResourceId::from_parts(self.index, self.generation.wrapping_add(1))
    }
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send + Sync> Sync for Slot<T> {}

#[repr(C)]
struct Block<T> {
    state: AtomicU8,
    active_slots: AtomicU32,
    next_slot: AtomicU32,
    len: u32,
    slots_offset: u32,
    layout_align: u32,
    marker: PhantomData<Slot<T>>,
}

impl<T> Block<T> {
    fn slots(&self) -> &[Slot<T>] {
        // SAFETY: the allocation layout reserves `len` initialized slots at
        // slots_offset for the entire lifetime of this header.
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).cast::<u8>().add(self.slots_offset as usize).cast(),
                self.len as usize,
            )
        }
    }
}

unsafe impl<T: Send> Send for Block<T> {}
unsafe impl<T: Send + Sync> Sync for Block<T> {}

static FREE_CHUNKS: LazyLock<FreeChunkTable> = LazyLock::new(FreeChunkTable::new);

#[doc(hidden)]
pub struct ResourceLocal<T: StaticResource> {
    chunk_index: u32,
    chunk: &'static FreeChunk,
    list: Arc<FreeList>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: StaticResource> ResourceLocal<T> {
    #[doc(hidden)]
    pub fn new(pool: &'static ResourcePool<T>) -> Self {
        let chunk_index = FREE_CHUNKS.acquire();
        let chunk = FREE_CHUNKS.get(chunk_index);
        Self { chunk_index, chunk, list: pool.free_list.clone(), _marker: PhantomData }
    }
}

impl<T: StaticResource> Drop for ResourceLocal<T> {
    fn drop(&mut self) {
        if self.chunk.is_empty() {
            FREE_CHUNKS.release(self.chunk_index);
        } else {
            self.list.push(self.chunk_index);
        }
    }
}

#[repr(C)]
struct BlockBucket<T> {
    blocks: [Atomic<Block<T>>; DIRECTORY_ENTRIES],
}

impl<T> BlockBucket<T> {
    fn new() -> Box<Self> {
        Box::new(Self { blocks: std::array::from_fn(|_| Atomic::null()) })
    }
}

const _: () = assert!(size_of::<BlockBucket<()>>() == PAGE_BYTES);

#[repr(C)]
struct RootBucket<T> {
    buckets: [AtomicPtr<BlockBucket<T>>; DIRECTORY_ENTRIES],
}

impl<T> RootBucket<T> {
    fn new() -> Self {
        Self { buckets: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())) }
    }
}

const _: () = assert!(size_of::<RootBucket<()>>() == PAGE_BYTES);

pub struct ResourcePool<T> {
    config: ResourcePoolConfig,
    root: RootBucket<T>,
    slots_per_block: u32,
    max_blocks: u32,
    next_block: AtomicU32,
    allocation_block: AtomicU32,
    block_allocation: Mutex<()>,
    released_blocks: Mutex<Vec<u32>>,
    free_list: Arc<FreeList>,
}

unsafe impl<T: Send> Send for ResourcePool<T> {}
unsafe impl<T: Send + Sync> Sync for ResourcePool<T> {}

impl ResourcePool<()> {
    pub fn global<T: StaticResource>() -> &'static ResourcePool<T> {
        T::resource_pool()
    }
}

impl<T: StaticResource> ResourcePool<T> {
    pub fn new(config: ResourcePoolConfig) -> Self {
        assert!(config.max_items > 0 && config.max_items <= u32::MAX as usize);
        let (slots_offset, slots_per_block, _) = block_layout::<T>();
        assert!(slots_offset < PAGE_BYTES && slots_per_block > 0);
        let max_blocks = config.max_items.div_ceil(slots_per_block) as u32;
        assert!(max_blocks as usize <= DIRECTORY_ENTRIES * DIRECTORY_ENTRIES);
        Self {
            config,
            root: RootBucket::new(),
            slots_per_block: slots_per_block as u32,
            max_blocks,
            next_block: AtomicU32::new(0),
            allocation_block: AtomicU32::new(NO_BLOCK),
            block_allocation: Mutex::new(()),
            released_blocks: Mutex::new(Vec::new()),
            free_list: Arc::new(FreeList::new(&FREE_CHUNKS)),
        }
    }

    /// Acquires a slot and publishes its id before a valid `T` is written.
    ///
    /// # Safety
    /// The caller must fully initialize the slot before exposing its
    /// `ResourceId`, calling `ResourcePool::get` with that id, or otherwise
    /// allowing code to dereference the resource.
    pub unsafe fn acquire_uninit(&'static self) -> Result<UninitResource<T>, ResourcePoolError> {
        self.assert_canonical();
        let acquired = match self.acquire_recycled() {
            Some(acquired) => acquired,
            None => self.acquire_fresh()?,
        };
        publish_acquired(&acquired);
        Ok(UninitResource { slot: acquired.slot, id: acquired.id() })
    }

    pub fn acquire_with_init(
        &'static self,
        init: impl FnOnce() -> T,
    ) -> Result<Resource<T>, ResourcePoolError> {
        self.assert_canonical();
        let acquired = match self.acquire_recycled() {
            Some(acquired) => acquired,
            None => self.acquire_fresh()?,
        };
        let guard = AcquiredSlotGuard::<T>::new(acquired);
        let value = init();
        Ok(guard.write(value))
    }

    pub fn get(&'static self, id: ResourceId<T>) -> Option<Resource<T>> {
        self.assert_canonical();
        let guard = epoch::pin();
        let (block, slot_index) = self.block_for_index(id.index(), &guard)?;
        let block_ref = unsafe { block.deref() };
        let slot = &block_ref.slots()[slot_index];
        let mut old = slot.metadata.load(Ordering::Acquire);
        loop {
            if state(old) != STATE_ALLOCATED || generation(old) != id.generation() {
                return None;
            }
            let references = references(old);
            if references == MAX_REFERENCES {
                return None;
            }
            match slot.metadata.compare_exchange_weak(
                old,
                with_references(old, references + 1),
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Resource { slot: NonNull::from(slot), id });
                }
                Err(actual) => old = actual,
            }
        }
    }

    /// Reconstructs a reference previously transferred with
    /// `Resource::into_owned_id` without incrementing the slot count.
    ///
    /// # Safety
    /// `id` must represent exactly one live, uniquely transferred reference,
    /// and each transferred reference must be reconstructed exactly once.
    pub(crate) unsafe fn reclaim_owned_id(&'static self, id: ResourceId<T>) -> Resource<T> {
        self.assert_canonical();
        let guard = epoch::pin();
        let (block, slot_index) =
            self.block_for_index(id.index(), &guard).expect("intrusive resource id outside pool");
        let slot = &unsafe { block.deref() }.slots()[slot_index];
        let metadata = slot.metadata.load(Ordering::Acquire);
        assert!(
            state(metadata) == STATE_ALLOCATED && generation(metadata) == id.generation(),
            "intrusive resource reference was recycled"
        );
        Resource { slot: NonNull::from(slot), id }
    }

    pub fn capacity(&self) -> usize {
        let blocks = self.next_block.load(Ordering::Relaxed) as usize;
        (blocks * self.slots_per_block as usize).min(self.config.max_items)
    }

    pub fn max_items(&self) -> usize {
        self.config.max_items
    }

    fn acquire_recycled(&self) -> Option<AcquiredSlot<T>> {
        let index = match self.try_with_local_cache(|local| {
            if let Some(index) = local.chunk.pop() {
                return Some(index);
            }
            let full_chunk_index = self.free_list.pop()?;
            let full_chunk = FREE_CHUNKS.get(full_chunk_index);
            FREE_CHUNKS.release(local.chunk_index);
            local.chunk_index = full_chunk_index;
            local.chunk = full_chunk;
            local.chunk.pop()
        }) {
            Some(index) => index?,
            None => self.acquire_recycled_without_local()?,
        };

        // This FreeChunk entry owns an active token, so the block cannot be
        // unlinked until this thread allocates the slot.
        let guard = unsafe { epoch::unprotected() };
        let Some((block, slot_index)) = self.block_for_index(index, guard) else {
            unreachable!("a FreeChunk token outlived its block");
        };
        let block_ref = unsafe { block.deref() };
        let slot = &block_ref.slots()[slot_index];
        let metadata = slot.metadata.load(Ordering::Acquire);
        assert_eq!(state(metadata), STATE_FREE, "FreeChunk entry does not own a free slot");
        Some(AcquiredSlot { index, generation: generation(metadata), slot: NonNull::from(slot) })
    }

    fn acquire_fresh(&self) -> Result<AcquiredSlot<T>, ResourcePoolError> {
        loop {
            let block_index = self.allocation_block.load(Ordering::Acquire);
            if block_index == NO_BLOCK {
                self.provision_allocation_block()?;
                continue;
            }
            let guard = epoch::pin();
            let Some(block) = self.block_at(block_index, &guard) else {
                self.allocation_block
                    .compare_exchange(block_index, NO_BLOCK, Ordering::AcqRel, Ordering::Relaxed)
                    .ok();
                continue;
            };
            let block_ref = unsafe { block.deref() };
            let slot_index = block_ref.next_slot.fetch_add(1, Ordering::Relaxed);
            if slot_index >= block_ref.len {
                self.allocation_block
                    .compare_exchange(block_index, NO_BLOCK, Ordering::AcqRel, Ordering::Relaxed)
                    .ok();
                continue;
            }
            let slot = &block_ref.slots()[slot_index as usize];
            let metadata = slot.metadata.load(Ordering::Acquire);
            if state(metadata) != STATE_FRESH {
                continue;
            }
            let index = block_index * self.slots_per_block + slot_index;
            return Ok(AcquiredSlot {
                index,
                generation: generation(metadata),
                slot: NonNull::from(slot),
            });
        }
    }

    fn provision_allocation_block(&self) -> Result<(), ResourcePoolError> {
        let _lock = self.block_allocation.lock().unwrap();
        let current = self.allocation_block.load(Ordering::Acquire);
        if current != NO_BLOCK {
            let guard = epoch::pin();
            if let Some(block) = self.block_at(current, &guard) {
                let block = unsafe { block.deref() };
                if block.state.load(Ordering::Acquire) == BLOCK_ACTIVE
                    && block.next_slot.load(Ordering::Relaxed) < block.len
                {
                    return Ok(());
                }
            }
        }
        let block_index = if let Some(index) = self.released_blocks.lock().unwrap().pop() {
            index
        } else {
            let index = self.next_block.load(Ordering::Relaxed);
            if index >= self.max_blocks {
                return Err(ResourcePoolError::Exhausted);
            }
            self.next_block.store(index + 1, Ordering::Relaxed);
            index
        };
        self.install_block(block_index);
        self.allocation_block.store(block_index, Ordering::Release);
        Ok(())
    }

    fn install_block(&self, block_index: u32) {
        let bucket = self.ensure_bucket(block_index as usize / DIRECTORY_ENTRIES);
        let entry = &bucket.blocks[block_index as usize % DIRECTORY_ENTRIES];
        let len = self
            .slots_per_block
            .min((self.config.max_items as u32).saturating_sub(block_index * self.slots_per_block));
        let block = allocate_block::<T>(len, random_generation(block_index));
        let raw = block.as_ptr();
        let guard = epoch::pin();
        if entry
            .compare_exchange(
                Shared::null(),
                Shared::from(raw as *const Block<T>),
                Ordering::Release,
                Ordering::Acquire,
                &guard,
            )
            .is_err()
        {
            unsafe { destroy_block(block) };
        }
    }

    fn ensure_bucket(&self, root_index: usize) -> &BlockBucket<T> {
        let entry = &self.root.buckets[root_index];
        let mut raw = entry.load(Ordering::Acquire);
        if raw.is_null() {
            let candidate = Box::into_raw(BlockBucket::new());
            match entry.compare_exchange(
                ptr::null_mut(),
                candidate,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => raw = candidate,
                Err(actual) => {
                    unsafe {
                        drop(Box::from_raw(candidate));
                    }
                    raw = actual;
                }
            }
        }
        unsafe { &*raw }
    }

    fn block_at<'g>(&self, block_index: u32, guard: &'g Guard) -> Option<Shared<'g, Block<T>>> {
        let bucket =
            self.root.buckets[block_index as usize / DIRECTORY_ENTRIES].load(Ordering::Acquire);
        if bucket.is_null() {
            return None;
        }
        let block = unsafe { &*bucket }.blocks[block_index as usize % DIRECTORY_ENTRIES]
            .load(Ordering::Acquire, guard);
        (!block.is_null()).then_some(block)
    }

    fn block_for_index<'g>(
        &self,
        index: u32,
        guard: &'g Guard,
    ) -> Option<(Shared<'g, Block<T>>, usize)> {
        if index as usize >= self.config.max_items {
            return None;
        }
        let block_index = block_index(index, self.slots_per_block);
        let slot_index = (index % self.slots_per_block) as usize;
        let block = self.block_at(block_index, guard)?;
        (slot_index < unsafe { block.deref() }.len as usize).then_some((block, slot_index))
    }

    fn try_with_local_cache<R>(
        &self,
        operation: impl FnOnce(&mut ResourceLocal<T>) -> R,
    ) -> Option<R> {
        T::try_with_resource_local(operation)
    }

    fn acquire_recycled_without_local(&self) -> Option<u32> {
        // FreeList may contain partial chunks flushed by ResourceLocal::drop or
        // recycle's TLS-teardown fallback. Return the remainder to the list so
        // every free-slot token continues to have exactly one owner.
        let chunk_index = self.free_list.pop()?;
        let chunk = FREE_CHUNKS.get(chunk_index);
        let index = chunk.pop().expect("FreeList contained an empty FreeChunk");
        if chunk.is_empty() {
            FREE_CHUNKS.release(chunk_index);
        } else {
            self.free_list.push(chunk_index);
        }
        Some(index)
    }

    fn assert_canonical(&'static self) {
        assert!(
            ptr::eq(self, T::resource_pool()),
            "Resource<T> requires T to be bound to this ResourcePool"
        );
    }

    fn recycle(&self, index: u32) {
        if self
            .try_with_local_cache(|local| {
                local.chunk.push(index);
                if local.chunk.is_full() {
                    local.list.push(local.chunk_index);
                    local.chunk_index = FREE_CHUNKS.acquire();
                    local.chunk = FREE_CHUNKS.get(local.chunk_index);
                    debug_assert!(local.chunk.is_empty());
                }
            })
            .is_none()
        {
            // A Resource can be the payload of another TLS destructor. Do not
            // lose its free-slot token merely because this resource type's TLS
            // cache has already been destroyed.
            let chunk_index = FREE_CHUNKS.acquire();
            FREE_CHUNKS.get(chunk_index).push(index);
            self.free_list.push(chunk_index);
        }
    }
}

impl<T: StaticResource + Default> ResourcePool<T> {
    pub fn acquire(&'static self) -> Result<Resource<T>, ResourcePoolError> {
        self.acquire_with_init(T::default)
    }
}

impl<T> Drop for ResourcePool<T> {
    fn drop(&mut self) {
        let guard = epoch::pin();
        for root_entry in &mut self.root.buckets {
            let bucket_ptr = *root_entry.get_mut();
            if bucket_ptr.is_null() {
                continue;
            }
            let bucket = unsafe { Box::from_raw(bucket_ptr) };
            for entry in &bucket.blocks {
                let block = entry.load(Ordering::Relaxed, &guard);
                if block.is_null() {
                    continue;
                }
                unsafe { destroy_block(NonNull::new_unchecked(block.as_raw() as *mut Block<T>)) };
            }
        }
    }
}

struct AcquiredSlotGuard<T: StaticResource> {
    acquired: AcquiredSlot<T>,
    armed: bool,
}

impl<T: StaticResource> AcquiredSlotGuard<T> {
    fn new(acquired: AcquiredSlot<T>) -> Self {
        Self { acquired, armed: true }
    }

    fn write(mut self, value: T) -> Resource<T> {
        let slot = unsafe { self.acquired.slot.as_ref() };
        unsafe { (*slot.value.get()).write(value) };
        publish_acquired(&self.acquired);
        self.armed = false;
        Resource { slot: self.acquired.slot, id: self.acquired.id() }
    }
}

impl<T: StaticResource> Drop for AcquiredSlotGuard<T> {
    fn drop(&mut self) {
        if self.armed {
            release_acquired(&self.acquired);
            T::resource_pool().recycle(self.acquired.index);
        }
    }
}

pub struct UninitResource<T: StaticResource> {
    slot: NonNull<Slot<T>>,
    id: ResourceId<T>,
}

unsafe impl<T: StaticResource> Send for UninitResource<T> {}
unsafe impl<T: StaticResource> Sync for UninitResource<T> {}

impl<T: StaticResource> UninitResource<T> {
    pub fn id(&self) -> ResourceId<T> {
        self.id
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        let slot = unsafe { self.slot.as_ref() };
        unsafe { (*slot.value.get()).as_mut_ptr() }
    }

    pub fn write(self, value: T) -> Resource<T> {
        let this = ManuallyDrop::new(self);
        let slot = unsafe { this.slot.as_ref() };
        unsafe { (*slot.value.get()).write(value) };
        Resource { slot: this.slot, id: this.id }
    }

    /// Marks this slot initialized after the caller has written a valid `T`
    /// through `as_mut_ptr`.
    ///
    /// # Safety
    /// The slot must contain a fully initialized `T`, and this method must be
    /// called at most once for this resource.
    pub unsafe fn assume_init(self) -> Resource<T> {
        let this = ManuallyDrop::new(self);
        Resource { slot: this.slot, id: this.id }
    }
}

impl<T: StaticResource> Drop for UninitResource<T> {
    fn drop(&mut self) {
        let slot = unsafe { self.slot.as_ref() };
        release_uninit(slot, self.id.generation());
        T::resource_pool().recycle(self.id.index());
    }
}

pub struct Resource<T: StaticResource> {
    slot: NonNull<Slot<T>>,
    id: ResourceId<T>,
}

unsafe impl<T: StaticResource> Send for Resource<T> {}
unsafe impl<T: StaticResource> Sync for Resource<T> {}

impl<T: StaticResource> Resource<T> {
    pub fn id(&self) -> ResourceId<T> {
        self.id
    }

    /// Transfers one counted reference into an intrusive data structure.
    /// The receiver must eventually reconstruct and drop it with
    /// `ResourcePool::reclaim_owned_id`.
    pub(crate) fn into_owned_id(self) -> ResourceId<T> {
        let id = self.id;
        std::mem::forget(self);
        id
    }

    /// Returns mutable access to an unpublished resource.
    ///
    /// # Safety
    /// The caller must guarantee that the resource ID has never been published
    /// and cannot concurrently be passed to `ResourcePool::get`. A reference
    /// count of one is checked, but is not sufficient by itself because a
    /// copied ID could otherwise be upgraded concurrently.
    pub(crate) unsafe fn get_mut_unchecked(&mut self) -> Option<&mut T> {
        let slot = unsafe { self.slot.as_ref() };
        let metadata = slot.metadata.load(Ordering::Acquire);
        if state(metadata) != STATE_ALLOCATED
            || generation(metadata) != self.id.generation()
            || references(metadata) != 1
        {
            return None;
        }
        // SAFETY: upheld by the caller in addition to the checked unique
        // Resource handle.
        Some(unsafe { &mut *(*slot.value.get()).as_mut_ptr() })
    }
}

impl<T: StaticResource> Clone for Resource<T> {
    fn clone(&self) -> Self {
        let slot = unsafe { self.slot.as_ref() };
        increment_reference(slot, self.id.generation()).expect("resource reference count overflow");
        Self { slot: self.slot, id: self.id }
    }
}

impl<T: StaticResource> Deref for Resource<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        let slot = unsafe { self.slot.as_ref() };
        unsafe { &*(*slot.value.get()).as_ptr() }
    }
}

impl<T: StaticResource> Drop for Resource<T> {
    fn drop(&mut self) {
        let slot = unsafe { self.slot.as_ref() };
        let mut old = slot.metadata.load(Ordering::Relaxed);
        loop {
            debug_assert_eq!(state(old), STATE_ALLOCATED);
            debug_assert_eq!(generation(old), self.id.generation());
            let count = references(old);
            if count == 1 {
                let free = pack(generation(old), 0, STATE_FREE);
                match slot.metadata.compare_exchange_weak(
                    old,
                    free,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        unsafe { (*slot.value.get()).assume_init_drop() };
                        T::resource_pool().recycle(self.id.index());
                        return;
                    }
                    Err(actual) => old = actual,
                }
            } else {
                match slot.metadata.compare_exchange_weak(
                    old,
                    with_references(old, count - 1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(actual) => old = actual,
                }
            }
        }
    }
}

fn publish_acquired<T>(acquired: &AcquiredSlot<T>) {
    let slot = unsafe { acquired.slot.as_ref() };
    let old = slot.metadata.load(Ordering::Acquire);
    debug_assert!(matches!(state(old), STATE_FRESH | STATE_FREE));
    debug_assert_eq!(generation(old), acquired.generation);
    slot.metadata.store(pack(acquired.id().generation(), 1, STATE_ALLOCATED), Ordering::Release);
}

fn release_acquired<T>(acquired: &AcquiredSlot<T>) {
    let slot = unsafe { acquired.slot.as_ref() };
    let old = slot.metadata.load(Ordering::Acquire);
    debug_assert!(matches!(state(old), STATE_FRESH | STATE_FREE));
    debug_assert_eq!(generation(old), acquired.generation);
    slot.metadata.store(pack(acquired.generation, 0, STATE_FREE), Ordering::Release);
}

fn release_uninit<T>(slot: &Slot<T>, expected_generation: u32) {
    let old = slot.metadata.load(Ordering::Acquire);
    debug_assert_eq!(state(old), STATE_ALLOCATED);
    debug_assert_eq!(generation(old), expected_generation);
    debug_assert_eq!(references(old), 1);
    slot.metadata.store(pack(expected_generation, 0, STATE_FREE), Ordering::Release);
}

fn increment_reference<T>(
    slot: &Slot<T>,
    expected_generation: u32,
) -> Result<(), ResourcePoolError> {
    let mut old = slot.metadata.load(Ordering::Acquire);
    loop {
        if state(old) != STATE_ALLOCATED || generation(old) != expected_generation {
            return Err(ResourcePoolError::Exhausted);
        }
        let count = references(old);
        if count == MAX_REFERENCES {
            return Err(ResourcePoolError::ReferenceOverflow);
        }
        match slot.metadata.compare_exchange_weak(
            old,
            with_references(old, count + 1),
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(()),
            Err(actual) => old = actual,
        }
    }
}

fn block_layout<T>() -> (usize, usize, Layout) {
    let header = Layout::new::<Block<T>>();
    let slot = Layout::new::<Slot<T>>();
    assert!(slot.size() > 0, "Slot<T> must not be zero-sized");
    let slots_offset = header.size().next_multiple_of(slot.align());
    assert!(slots_offset < PAGE_BYTES, "resource is too large for a block");
    let count = (PAGE_BYTES - slots_offset) / slot.size();
    let align = header.align().max(slot.align());
    assert!(
        align <= PAGE_BYTES && PAGE_BYTES.is_multiple_of(align),
        "resource alignment exceeds block layout"
    );
    (slots_offset, count, Layout::from_size_align(PAGE_BYTES, align).unwrap())
}

fn allocate_block<T>(len: u32, seed: u32) -> NonNull<Block<T>> {
    let (slots_offset, capacity, layout) = block_layout::<T>();
    assert!(len as usize <= capacity);
    let raw = unsafe { alloc(layout) };
    if raw.is_null() {
        handle_alloc_error(layout);
    }
    let block = raw.cast::<Block<T>>();
    unsafe {
        block.write(Block {
            state: AtomicU8::new(BLOCK_ACTIVE),
            active_slots: AtomicU32::new(len),
            next_slot: AtomicU32::new(0),
            len,
            slots_offset: slots_offset as u32,
            layout_align: layout.align() as u32,
            marker: PhantomData,
        });
        let slots = raw.add(slots_offset).cast::<Slot<T>>();
        for index in 0..len as usize {
            slots.add(index).write(Slot {
                metadata: AtomicU64::new(pack(seed, 0, STATE_FRESH)),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        NonNull::new_unchecked(block)
    }
}

unsafe fn destroy_block<T>(block: NonNull<Block<T>>) {
    let block_ref = unsafe { block.as_ref() };
    for slot in block_ref.slots() {
        let metadata = slot.metadata.load(Ordering::Relaxed);
        if state(metadata) == STATE_ALLOCATED {
            unsafe { (*slot.value.get()).assume_init_drop() };
        }
        unsafe { ptr::drop_in_place(slot as *const Slot<T> as *mut Slot<T>) };
    }
    let layout = Layout::from_size_align(PAGE_BYTES, block_ref.layout_align as usize).unwrap();
    unsafe {
        ptr::drop_in_place(block.as_ptr());
        dealloc(block.as_ptr().cast(), layout);
    }
}

fn random_generation(block_index: u32) -> u32 {
    let nonce = NEXT_GENERATION_SEED.fetch_add(1, Ordering::Relaxed);
    let state = std::collections::hash_map::RandomState::new();
    state.hash_one((block_index, nonce)) as u32
}

const fn block_index(index: u32, slots_per_block: u32) -> u32 {
    index / slots_per_block
}

const fn pack(generation: u32, references: u32, state: u64) -> u64 {
    generation as u64 | ((references as u64) << REFERENCES_SHIFT) | (state << STATE_SHIFT)
}
const fn generation(metadata: u64) -> u32 {
    (metadata & GENERATION_MASK) as u32
}
const fn references(metadata: u64) -> u32 {
    ((metadata & REFERENCES_MASK) >> REFERENCES_SHIFT) as u32
}
const fn state(metadata: u64) -> u64 {
    (metadata & STATE_MASK) >> STATE_SHIFT
}
const fn with_references(metadata: u64, references: u32) -> u64 {
    (metadata & !REFERENCES_MASK) | ((references as u64) << REFERENCES_SHIFT)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct TeardownResource;

    static TEARDOWN_POOL: LazyLock<ResourcePool<TeardownResource>> =
        LazyLock::new(|| ResourcePool::new(ResourcePoolConfig { max_items: 1 }));
    static TEARDOWN_LOCAL_AVAILABLE: AtomicBool = AtomicBool::new(true);

    thread_local! {
        static TEARDOWN_LOCAL: RefCell<ResourceLocal<TeardownResource>> =
            RefCell::new(ResourceLocal::new(&TEARDOWN_POOL));
        static EXIT_RESOURCE: RefCell<ExitResource> = const { RefCell::new(ExitResource(None)) };
    }

    impl StaticResource for TeardownResource {
        fn resource_pool() -> &'static ResourcePool<Self> {
            &TEARDOWN_POOL
        }

        fn try_with_resource_local<R>(
            operation: impl FnOnce(&mut ResourceLocal<Self>) -> R,
        ) -> Option<R> {
            if !TEARDOWN_LOCAL_AVAILABLE.load(Ordering::Acquire) {
                return None;
            }
            TEARDOWN_LOCAL.try_with(|local| operation(&mut local.borrow_mut())).ok()
        }
    }

    struct ExitResource(Option<Resource<TeardownResource>>);

    impl Drop for ExitResource {
        fn drop(&mut self) {
            TEARDOWN_LOCAL_AVAILABLE.store(false, Ordering::Release);
            drop(self.0.take());
        }
    }

    #[test]
    fn fixed_layouts_have_the_requested_sizes() {
        assert_eq!(size_of::<RootBucket<usize>>(), PAGE_BYTES);
        assert_eq!(size_of::<BlockBucket<usize>>(), PAGE_BYTES);
        let (_, slots, layout) = block_layout::<usize>();
        assert!(slots > 0);
        assert_eq!(layout.size(), PAGE_BYTES);
    }

    #[test]
    fn packed_metadata_preserves_limits_and_generation_wraps() {
        let metadata = pack(u32::MAX, MAX_REFERENCES, STATE_ALLOCATED);
        assert_eq!(generation(metadata), u32::MAX);
        assert_eq!(references(metadata), MAX_REFERENCES);
        assert_eq!(state(metadata), STATE_ALLOCATED);
        assert_eq!(generation(metadata).wrapping_add(1), 0);
    }

    #[test]
    fn thread_exit_recycles_without_an_available_local_cache() {
        TEARDOWN_LOCAL_AVAILABLE.store(true, Ordering::Release);
        let original = std::thread::spawn(|| {
            let resource = TEARDOWN_POOL.acquire_with_init(|| TeardownResource).unwrap();
            let id = resource.id();
            EXIT_RESOURCE.with(|slot| slot.borrow_mut().0 = Some(resource));
            id
        })
        .join()
        .unwrap();

        assert!(!TEARDOWN_LOCAL_AVAILABLE.load(Ordering::Acquire));
        let recycled = TEARDOWN_POOL.acquire_with_init(|| TeardownResource).unwrap();
        assert_eq!(recycled.id().index(), original.index());
        assert_ne!(recycled.id().generation(), original.generation());

        drop(recycled);
        TEARDOWN_LOCAL_AVAILABLE.store(true, Ordering::Release);
    }
}
