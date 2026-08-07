// SPDX-License-Identifier: Apache-2.0

//! Segmented network buffers backed by a bounded, static-friendly block pool.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::cell::RefCell;
use std::io::{IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex};

use bytes::buf::UninitSlice;
use bytes::{Buf, BufMut};

const IO_BLOCK_BYTES: usize = 16 * 1024;
const LOCAL_TRANSFER_BATCH: usize = 16;
const LOCAL_HIGH_WATERMARK: usize = 32;
const CENTRAL_CACHE_BLOCKS: usize = 1024;
const MAX_BLOCK_REFERENCES: u32 = u32::MAX - 1;
static IO_BLOCK_POOL: LazyLock<IoBlockPool> = LazyLock::new(IoBlockPool::new);

thread_local! {
    static LOCAL_BLOCK_CACHE: RefCell<LocalBlockCache> = RefCell::new(LocalBlockCache::new());
}

#[repr(C)]
struct IoBlock {
    references: AtomicU32,
}

unsafe impl Send for IoBlock {}
unsafe impl Sync for IoBlock {}

impl IoBlock {
    unsafe fn data(&self) -> *mut MaybeUninit<u8> {
        // SAFETY: the tail allocation stores bytes immediately after IoBlock.
        unsafe {
            (ptr::from_ref(self).cast::<u8>() as *mut u8).add(std::mem::size_of::<IoBlock>()).cast()
        }
    }

    unsafe fn bytes(&self, offset: u16, len: u16) -> &[u8] {
        let offset = offset as usize;
        let len = len as usize;
        debug_assert!(offset + len <= IO_BLOCK_BYTES);
        // SAFETY: IoBlockRef only exposes bytes committed by a builder.
        unsafe { std::slice::from_raw_parts(self.data().add(offset).cast::<u8>(), len) }
    }

    unsafe fn bytes_mut_from<'a>(pointer: NonNull<Self>, offset: u16, len: u16) -> &'a mut [u8] {
        let offset = offset as usize;
        let len = len as usize;
        debug_assert!(offset + len <= IO_BLOCK_BYTES);
        // SAFETY: callers reserve uniquely owned block ranges before exposing
        // them to vectored reads.
        unsafe {
            std::slice::from_raw_parts_mut(pointer.as_ref().data().add(offset).cast::<u8>(), len)
        }
    }
}

#[derive(Clone, Copy)]
struct ActiveBlock {
    pointer: usize,
    offset: u16,
}

struct LocalBlockCache {
    free: Vec<usize>,
    active: Option<ActiveBlock>,
}

impl LocalBlockCache {
    fn new() -> Self {
        Self { free: Vec::with_capacity(LOCAL_HIGH_WATERMARK), active: None }
    }
}

impl Drop for LocalBlockCache {
    fn drop(&mut self) {
        for pointer in self.free.drain(..) {
            let pointer = block_pointer(pointer);
            // SAFETY: cached free blocks have no live references.
            unsafe { deallocate_block(pointer) };
        }
        if let Some(active) = self.active.take() {
            let pointer = block_pointer(active.pointer);
            release_active_reference(pointer);
        }
    }
}

struct CentralCache {
    free: Vec<usize>,
}

struct IoBlockPool {
    central: Mutex<CentralCache>,
}

impl IoBlockPool {
    fn new() -> Self {
        Self {
            central: Mutex::new(CentralCache { free: Vec::with_capacity(CENTRAL_CACHE_BLOCKS) }),
        }
    }

    fn acquire_empty(&'static self) -> NonNull<IoBlock> {
        LOCAL_BLOCK_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if cache.free.is_empty() {
                self.refill_local(&mut cache);
            }
            let pointer = cache.free.pop().expect("IoBlock refill did not provide a block");
            prepare_acquired(pointer)
        })
    }

    fn acquire_append(&'static self) -> ActiveBlock {
        LOCAL_BLOCK_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(active) = cache.active.take() {
                return active;
            }
            if cache.free.is_empty() {
                self.refill_local(&mut cache);
            }
            let pointer = cache.free.pop().expect("IoBlock refill did not provide a block");
            let block = prepare_acquired(pointer);
            ActiveBlock { pointer: block.as_ptr() as usize, offset: 0 }
        })
    }

    fn release_append(&'static self, active: ActiveBlock) {
        if active.offset as usize >= IO_BLOCK_BYTES {
            let pointer = block_pointer(active.pointer);
            release_block_reference(pointer);
            return;
        }
        let result = LOCAL_BLOCK_CACHE.try_with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(existing) = cache.active.replace(active) {
                let pointer = block_pointer(existing.pointer);
                release_active_reference(pointer);
            }
        });
        if result.is_err() {
            let pointer = block_pointer(active.pointer);
            release_active_reference(pointer);
        }
    }

    fn recycle_empty(&'static self, pointer: NonNull<IoBlock>) {
        let address = pointer.as_ptr() as usize;
        let result = LOCAL_BLOCK_CACHE.try_with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.free.push(address);
            if cache.free.len() > LOCAL_HIGH_WATERMARK {
                let split_at = cache.free.len() - LOCAL_TRANSFER_BATCH;
                let batch = cache.free.split_off(split_at);
                self.cache_central(batch);
            }
        });
        if result.is_err() {
            self.cache_central(vec![address]);
        }
    }

    fn refill_local(&'static self, local: &mut LocalBlockCache) {
        {
            let mut central = self.central.lock().expect("IoBlock central cache mutex poisoned");
            let count = LOCAL_TRANSFER_BATCH.min(central.free.len());
            let split_at = central.free.len() - count;
            local.free.extend(central.free.split_off(split_at));
        }
        while local.free.len() < LOCAL_TRANSFER_BATCH {
            local.free.push(allocate_block().as_ptr() as usize);
        }
    }

    fn cache_central(&'static self, batch: Vec<usize>) {
        let mut overflow = Vec::new();
        {
            let mut central = self.central.lock().expect("IoBlock central cache mutex poisoned");
            for address in batch {
                if central.free.len() < CENTRAL_CACHE_BLOCKS {
                    central.free.push(address);
                } else {
                    overflow.push(address);
                }
            }
        }
        for address in overflow {
            let pointer = block_pointer(address);
            // SAFETY: no cache retained this block and no references remain.
            unsafe { deallocate_block(pointer) };
        }
    }
}

fn block_pointer(address: usize) -> NonNull<IoBlock> {
    NonNull::new(address as *mut IoBlock).expect("IoBlock cache stored a null block pointer")
}

fn checked_block_range_value(value: usize) -> u16 {
    debug_assert!(value <= IO_BLOCK_BYTES);
    value.try_into().expect("IoBlock range value exceeds fixed block size")
}

fn prepare_acquired(pointer: usize) -> NonNull<IoBlock> {
    let pointer = NonNull::new(pointer as *mut IoBlock).expect("cached null IoBlock");
    // SAFETY: cached blocks have no live references.
    unsafe { pointer.as_ref() }.references.store(1, Ordering::Relaxed);
    pointer
}

fn block_layout() -> (Layout, usize) {
    let (layout, offset) = Layout::new::<IoBlock>()
        .extend(
            Layout::array::<MaybeUninit<u8>>(IO_BLOCK_BYTES).expect("IoBlock capacity overflow"),
        )
        .expect("IoBlock layout overflow");
    (layout.pad_to_align(), offset)
}

fn allocate_block() -> NonNull<IoBlock> {
    let (layout, _) = block_layout();
    // SAFETY: layout is non-zero because it includes IoBlock.
    let allocation = unsafe { alloc(layout) };
    if allocation.is_null() {
        handle_alloc_error(layout);
    }
    let pointer = allocation.cast::<IoBlock>();
    // SAFETY: allocation is aligned and large enough for the header.
    unsafe {
        pointer.write(IoBlock { references: AtomicU32::new(0) });
        NonNull::new_unchecked(pointer)
    }
}

unsafe fn deallocate_block(pointer: NonNull<IoBlock>) {
    // SAFETY: caller guarantees exclusive ownership of an allocated block.
    let (layout, _) = block_layout();
    unsafe {
        ptr::drop_in_place(pointer.as_ptr());
        dealloc(pointer.as_ptr().cast(), layout);
    }
}

struct IoBlockRef {
    block: NonNull<IoBlock>,
    offset: u16,
    len: u16,
}

unsafe impl Send for IoBlockRef {}
unsafe impl Sync for IoBlockRef {}

impl IoBlockRef {
    fn from_owned(block: NonNull<IoBlock>, offset: usize, len: usize) -> Self {
        debug_assert!(offset + len <= IO_BLOCK_BYTES);
        let offset = checked_block_range_value(offset);
        let len = checked_block_range_value(len);
        Self { block, offset, len }
    }

    fn from_borrowed(block: NonNull<IoBlock>, offset: usize, len: usize) -> Self {
        debug_assert!(offset + len <= IO_BLOCK_BYTES);
        let offset = checked_block_range_value(offset);
        let len = checked_block_range_value(len);
        increment_block_reference(block);
        Self { block, offset, len }
    }

    fn offset(&self) -> usize {
        self.offset as usize
    }

    fn len(&self) -> usize {
        self.len as usize
    }

    fn end(&self) -> usize {
        self.offset() + self.len()
    }

    fn spare_len(&self) -> usize {
        IO_BLOCK_BYTES - self.end()
    }

    fn is_unique(&self) -> bool {
        unsafe { self.block.as_ref() }.references.load(Ordering::Acquire) == 1
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: this reference owns one block reference and its range was
        // committed by an IoBufBuilder.
        unsafe { self.block.as_ref().bytes(self.offset, self.len) }
    }
}

impl Clone for IoBlockRef {
    fn clone(&self) -> Self {
        increment_block_reference(self.block);
        Self { block: self.block, offset: self.offset, len: self.len }
    }
}

impl Drop for IoBlockRef {
    fn drop(&mut self) {
        release_block_reference(self.block);
    }
}

fn release_block_reference(pointer: NonNull<IoBlock>) {
    // SAFETY: caller owns one reference.
    let block = unsafe { pointer.as_ref() };
    if block.references.fetch_sub(1, Ordering::AcqRel) == 1 {
        IO_BLOCK_POOL.recycle_empty(pointer);
    }
}

fn release_active_reference(pointer: NonNull<IoBlock>) {
    // SAFETY: caller owns the TLS active reference.
    let block = unsafe { pointer.as_ref() };
    if block.references.fetch_sub(1, Ordering::AcqRel) == 1 {
        // SAFETY: the exiting or displaced active cache exclusively owns this block.
        unsafe { deallocate_block(pointer) };
    }
}

fn increment_block_reference(pointer: NonNull<IoBlock>) {
    // SAFETY: caller holds an existing reference while incrementing.
    let references = &unsafe { pointer.as_ref() }.references;
    references
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            (count < MAX_BLOCK_REFERENCES).then_some(count + 1)
        })
        .expect("IoBlock reference count overflow");
}

#[derive(Default)]
enum BlockRefs {
    #[default]
    Empty,
    One(IoBlockRef),
    Two(IoBlockRef, IoBlockRef),
    Many(ManyBlockRefs),
}

struct ManyBlockRefs {
    refs: Vec<Option<IoBlockRef>>,
    head: usize,
}

impl ManyBlockRefs {
    fn from_three(first: IoBlockRef, second: IoBlockRef, third: IoBlockRef) -> Self {
        Self { refs: vec![Some(first), Some(second), Some(third)], head: 0 }
    }

    fn len(&self) -> usize {
        self.refs.len() - self.head
    }

    fn push_back(&mut self, reference: IoBlockRef) {
        self.refs.push(Some(reference));
    }

    fn pop_front(&mut self) -> Option<IoBlockRef> {
        if self.head == self.refs.len() {
            return None;
        }
        let reference =
            self.refs[self.head].take().expect("BlockRefs::Many live slot was already taken");
        self.head += 1;
        if self.head > 0 && self.head * 2 >= self.refs.len() {
            self.refs.drain(..self.head);
            self.head = 0;
        }
        Some(reference)
    }

    fn front(&self) -> Option<&IoBlockRef> {
        self.refs.get(self.head).and_then(Option::as_ref)
    }

    fn front_mut(&mut self) -> Option<&mut IoBlockRef> {
        self.refs.get_mut(self.head).and_then(Option::as_mut)
    }

    fn back_mut(&mut self) -> Option<&mut IoBlockRef> {
        self.refs.last_mut().and_then(Option::as_mut)
    }

    fn get(&self, index: usize) -> Option<&IoBlockRef> {
        self.refs.get(self.head + index).and_then(Option::as_ref)
    }

    fn iter(&self) -> impl Iterator<Item = &IoBlockRef> {
        self.refs[self.head..].iter().filter_map(Option::as_ref)
    }
}

impl BlockRefs {
    fn push_back(&mut self, reference: IoBlockRef) {
        let old = std::mem::take(self);
        *self = match old {
            Self::Empty => Self::One(reference),
            Self::One(first) => Self::Two(first, reference),
            Self::Two(first, second) => {
                Self::Many(ManyBlockRefs::from_three(first, second, reference))
            }
            Self::Many(mut refs) => {
                refs.push_back(reference);
                Self::Many(refs)
            }
        };
    }

    fn pop_front(&mut self) -> Option<IoBlockRef> {
        let old = std::mem::take(self);
        let (first, replacement) = match old {
            Self::Empty => return None,
            Self::One(first) => (first, Self::Empty),
            Self::Two(first, second) => (first, Self::One(second)),
            Self::Many(mut refs) => {
                let first = refs.pop_front().expect("BlockRefs::Many had no front reference");
                let replacement = match refs.len() {
                    0 => Self::Empty,
                    1 => Self::One(refs.pop_front().expect("BlockRefs::Many len=1 without front")),
                    2 => Self::Two(
                        refs.pop_front().expect("BlockRefs::Many len=2 without first front"),
                        refs.pop_front().expect("BlockRefs::Many len=2 without second front"),
                    ),
                    _ => Self::Many(refs),
                };
                (first, replacement)
            }
        };
        *self = replacement;
        Some(first)
    }

    fn front(&self) -> Option<&IoBlockRef> {
        match self {
            Self::Empty => None,
            Self::One(first) | Self::Two(first, _) => Some(first),
            Self::Many(refs) => refs.front(),
        }
    }

    fn front_mut(&mut self) -> Option<&mut IoBlockRef> {
        match self {
            Self::Empty => None,
            Self::One(first) | Self::Two(first, _) => Some(first),
            Self::Many(refs) => refs.front_mut(),
        }
    }

    fn back_mut(&mut self) -> Option<&mut IoBlockRef> {
        match self {
            Self::Empty => None,
            Self::One(first) => Some(first),
            Self::Two(_, second) => Some(second),
            Self::Many(refs) => refs.back_mut(),
        }
    }

    fn for_each(&self, mut visitor: impl FnMut(&IoBlockRef)) {
        match self {
            Self::Empty => {}
            Self::One(first) => visitor(first),
            Self::Two(first, second) => {
                visitor(first);
                visitor(second);
            }
            Self::Many(refs) => refs.iter().for_each(visitor),
        }
    }
}

/// Tiny queue of shared block ranges. Two ranges are stored without allocating
/// a BigView; moving whole ranges does not touch the block reference counter.
#[derive(Default)]
pub struct IoBuf {
    len: usize,
    refs: BlockRefs,
}

impl std::fmt::Debug for IoBuf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IoBuf")
            .field("len", &self.len)
            .field("blocks", &self.chunks().count())
            .finish()
    }
}

unsafe impl Send for IoBuf {}
unsafe impl Sync for IoBuf {}

impl Clone for IoBuf {
    fn clone(&self) -> Self {
        let mut output = Self::new();
        self.refs.for_each(|reference| output.push_ref(reference.clone()));
        output
    }
}

impl IoBuf {
    pub const fn new() -> Self {
        Self { len: 0, refs: BlockRefs::Empty }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.refs = BlockRefs::Empty;
        self.len = 0;
    }

    pub fn consume(&mut self, count: usize) {
        <Self as Buf>::advance(self, count);
    }

    pub fn append(&mut self, other: &Self) {
        other.refs.for_each(|reference| self.push_ref(reference.clone()));
    }

    pub fn append_movable(&mut self, other: &mut Self) {
        while let Some(reference) = other.refs.pop_front() {
            other.len -= reference.len();
            self.push_ref(reference);
        }
    }

    pub fn split_to(&mut self, mut count: usize) -> Self {
        count = count.min(self.len);
        let mut output = Self::new();
        while count > 0 {
            let front_len = self.refs.front().expect("IoBuf split_to missing front ref").len();
            if front_len <= count {
                let reference =
                    self.refs.pop_front().expect("IoBuf split_to front ref disappeared");
                let reference_len = reference.len();
                self.len -= reference_len;
                count -= reference_len;
                output.push_ref(reference);
            } else {
                let front = self.refs.front_mut().expect("IoBuf split_to front ref disappeared");
                let mut prefix = front.clone();
                prefix.len = checked_block_range_value(count);
                prefix.offset = front.offset;
                front.offset = checked_block_range_value(front.offset() + count);
                front.len = checked_block_range_value(front.len() - count);
                self.len -= count;
                output.push_ref(prefix);
                count = 0;
            }
        }
        output
    }

    pub fn chunks(&self) -> IoBufChunks<'_> {
        IoBufChunks { refs: &self.refs, index: 0 }
    }

    /// Fills a caller-owned iovec array without allocating. Adjacent chunks
    /// remain separate because they may refer to independently pooled blocks.
    pub fn fill_io_slices<'a>(&'a self, output: &mut [IoSlice<'a>]) -> usize {
        let mut count = 0;
        for chunk in self.chunks().take(output.len()) {
            output[count] = IoSlice::new(chunk);
            count += 1;
        }
        count
    }

    pub fn reserve<const N: usize>(&mut self) -> IoBufMut<'_, N> {
        assert!(N > 0);
        let tail = self.refs.back_mut().and_then(|tail| {
            let spare_len = tail.spare_len();
            (tail.is_unique() && spare_len > 0).then(|| ReservedTail {
                block: tail.block,
                offset: checked_block_range_value(tail.end()),
                len: checked_block_range_value(spare_len),
            })
        });

        let mut blocks = [None; N];
        let block_count = N - usize::from(tail.is_some());
        for block in blocks.iter_mut().take(block_count) {
            *block = Some(IO_BLOCK_POOL.acquire_empty());
        }

        IoBufMut { target: self, tail, blocks, block_count, exposed_capacity: 0 }
    }

    fn push_ref(&mut self, reference: IoBlockRef) {
        self.len += reference.len();
        self.refs.push_back(reference);
    }

    fn extend_tail(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        let tail = self.refs.back_mut().expect("reserved tail disappeared");
        debug_assert!(tail.is_unique());
        debug_assert!(len <= tail.spare_len());
        tail.len = checked_block_range_value(tail.len() + len);
        self.len += len;
    }
}

#[derive(Clone, Copy)]
struct ReservedTail {
    block: NonNull<IoBlock>,
    offset: u16,
    len: u16,
}

/// Short-lived writable reservation at the end of an `IoBuf`.
///
/// Dropping without `commit` releases newly acquired blocks and leaves the
/// original buffer unchanged. This type deliberately does not expose `IoBlock`;
/// callers only see temporary `IoSliceMut` views.
pub struct IoBufMut<'a, const N: usize> {
    target: &'a mut IoBuf,
    tail: Option<ReservedTail>,
    blocks: [Option<NonNull<IoBlock>>; N],
    block_count: usize,
    exposed_capacity: usize,
}

impl<const N: usize> IoBufMut<'_, N> {
    pub fn fill_io_slices<'a>(&'a mut self, output: &mut [IoSliceMut<'a>]) -> usize {
        self.exposed_capacity = 0;
        let mut count = 0;
        if let Some(tail) = self.tail.filter(|tail| tail.len > 0) {
            if count == output.len() {
                return count;
            }
            output[count] = IoSliceMut::new(unsafe {
                IoBlock::bytes_mut_from(tail.block, tail.offset, tail.len)
            });
            self.exposed_capacity += tail.len as usize;
            count += 1;
        }

        for block in self.blocks.iter().take(self.block_count) {
            if count == output.len() {
                break;
            }
            let block = block.expect("IoBufMut reserved read block missing while filling iovec");
            output[count] = IoSliceMut::new(unsafe {
                IoBlock::bytes_mut_from(block, 0, checked_block_range_value(IO_BLOCK_BYTES))
            });
            self.exposed_capacity += IO_BLOCK_BYTES;
            count += 1;
        }
        count
    }

    pub fn capacity(&self) -> usize {
        self.tail.map_or(0, |tail| tail.len as usize) + self.block_count * IO_BLOCK_BYTES
    }

    pub fn commit(mut self, read: usize) {
        assert!(read <= self.exposed_capacity);
        let mut remaining = read;
        if let Some(tail) = self.tail {
            let len = remaining.min(tail.len as usize);
            self.target.extend_tail(len);
            remaining -= len;
        }

        for index in 0..self.block_count {
            if remaining == 0 {
                break;
            }
            let block = self.blocks[index]
                .take()
                .expect("IoBufMut reserved read block missing during commit");
            let len = remaining.min(IO_BLOCK_BYTES);
            self.target.push_ref(IoBlockRef::from_owned(block, 0, len));
            remaining -= len;
        }
        debug_assert_eq!(remaining, 0, "read exceeded exposed reservation capacity");
    }
}

impl<const N: usize> Drop for IoBufMut<'_, N> {
    fn drop(&mut self) {
        for block in self.blocks.iter_mut().take(self.block_count) {
            if let Some(block) = block.take() {
                release_block_reference(block);
            }
        }
    }
}

impl Buf for IoBuf {
    fn remaining(&self) -> usize {
        self.len
    }

    fn chunk(&self) -> &[u8] {
        self.refs.front().map_or(&[], IoBlockRef::bytes)
    }

    fn advance(&mut self, mut count: usize) {
        assert!(count <= self.len);
        while count > 0 {
            let front_len = self.refs.front().expect("IoBuf advance missing front ref").len();
            if front_len <= count {
                let reference = self.refs.pop_front().expect("IoBuf advance front ref disappeared");
                let reference_len = reference.len();
                self.len -= reference_len;
                count -= reference_len;
            } else {
                let front = self.refs.front_mut().expect("IoBuf advance front ref disappeared");
                front.offset = checked_block_range_value(front.offset() + count);
                front.len = checked_block_range_value(front.len() - count);
                self.len -= count;
                count = 0;
            }
        }
    }
}

pub struct IoBufChunks<'a> {
    refs: &'a BlockRefs,
    index: usize,
}

impl<'a> Iterator for IoBufChunks<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let reference = match self.refs {
            BlockRefs::Empty => None,
            BlockRefs::One(first) => (self.index == 0).then_some(first),
            BlockRefs::Two(first, second) => match self.index {
                0 => Some(first),
                1 => Some(second),
                _ => None,
            },
            BlockRefs::Many(refs) => refs.get(self.index),
        }?;
        self.index += 1;
        Some(reference.bytes())
    }
}

/// Single-writer adapter used directly by prost. Builders borrow a thread-local
/// append block and commit byte ranges into immutable `IoBuf` references.
pub struct IoBufBuilder {
    output: IoBuf,
    current: Option<ActiveBlock>,
    used: usize,
}

unsafe impl Send for IoBufBuilder {}

impl Default for IoBufBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IoBufBuilder {
    pub fn new() -> Self {
        Self { output: IoBuf::new(), current: None, used: 0 }
    }

    pub fn finish(mut self) -> IoBuf {
        self.flush_current();
        self.release_current();
        std::mem::take(&mut self.output)
    }

    /// Flushes committed bytes while retaining the builder's append block.
    pub fn take(&mut self) -> IoBuf {
        self.flush_current();
        std::mem::take(&mut self.output)
    }

    fn ensure_current(&mut self, minimum: usize) {
        let remaining =
            self.current.map_or(0, |active| IO_BLOCK_BYTES - active.offset as usize - self.used);
        if remaining >= minimum.max(1) {
            return;
        }
        self.flush_current();
        if self.current.map_or(0, |active| IO_BLOCK_BYTES - active.offset as usize) < minimum.max(1)
        {
            self.release_current();
            self.current = Some(IO_BLOCK_POOL.acquire_append());
        }
    }

    fn flush_current(&mut self) {
        let Some(mut active) = self.current else {
            return;
        };
        if self.used > 0 {
            let block = block_pointer(active.pointer);
            self.output.push_ref(IoBlockRef::from_borrowed(
                block,
                active.offset as usize,
                self.used,
            ));
            active.offset = checked_block_range_value(active.offset as usize + self.used);
            self.used = 0;
            self.current = Some(active);
            if active.offset as usize == IO_BLOCK_BYTES {
                self.release_current();
            }
        }
    }

    fn release_current(&mut self) {
        if let Some(active) = self.current.take() {
            IO_BLOCK_POOL.release_append(active);
        }
    }
}

impl Drop for IoBufBuilder {
    fn drop(&mut self) {
        self.release_current();
    }
}

unsafe impl BufMut for IoBufBuilder {
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.output.len - self.used
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        let active = self.current.expect("advance_mut before chunk_mut");
        assert!(count <= IO_BLOCK_BYTES - active.offset as usize - self.used);
        self.used += count;
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.ensure_current(1);
        let active = self.current.expect("IoBufBuilder chunk_mut did not acquire an active block");
        let block = block_pointer(active.pointer);
        // SAFETY: the builder owns the active mutable tail for this thread.
        let block = unsafe { block.as_ref() };
        let pointer = unsafe { block.data().add(active.offset as usize + self.used).cast::<u8>() };
        let len = IO_BLOCK_BYTES - active.offset as usize - self.used;
        unsafe { UninitSlice::from_raw_parts_mut(pointer, len) }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    #[test]
    fn tail_allocation_places_payload_after_header() {
        let (layout, offset) = block_layout();
        assert!(offset >= std::mem::size_of::<IoBlock>());
        assert_eq!(offset, std::mem::size_of::<IoBlock>());
        assert!(layout.size() >= offset + IO_BLOCK_BYTES);
    }

    #[test]
    fn block_headers_and_refs_stay_compact() {
        assert_eq!(std::mem::size_of::<IoBlock>(), std::mem::size_of::<AtomicU32>());
        assert_eq!(std::mem::size_of::<IoBlockRef>(), 16);
        assert_eq!(std::mem::size_of::<Option<IoBlockRef>>(), std::mem::size_of::<IoBlockRef>());
    }

    #[test]
    fn consume_removes_committed_prefix() {
        let mut builder = IoBufBuilder::new();
        builder.put_slice(b"abcdef");
        let mut buffer = builder.finish();

        buffer.consume(2);

        assert_eq!(buffer.len(), 4);
        assert_eq!(collect(&buffer), b"cdef");
    }

    #[test]
    fn reserve_commits_read_bytes_and_rolls_back_unused_blocks() {
        let mut buffer = IoBuf::new();
        {
            let mut reserved = buffer.reserve::<2>();
            let capacity = reserved.capacity();
            assert!(capacity >= IO_BLOCK_BYTES * 2);
            {
                let mut slices: [IoSliceMut<'_>; 2] =
                    std::array::from_fn(|_| IoSliceMut::new(&mut []));
                let count = reserved.fill_io_slices(&mut slices);
                assert_eq!(count, 2);
                slices[0][..5].copy_from_slice(b"hello");
            }
            reserved.commit(5);
        }

        assert_eq!(buffer.len(), 5);
        assert_eq!(collect(&buffer), b"hello");

        {
            let mut reserved = buffer.reserve::<2>();
            {
                let mut slices: [IoSliceMut<'_>; 2] =
                    std::array::from_fn(|_| IoSliceMut::new(&mut []));
                let count = reserved.fill_io_slices(&mut slices);
                assert!(count > 0);
                slices[0][..4].copy_from_slice(b"drop");
            }
        }

        assert_eq!(buffer.len(), 5);
        assert_eq!(collect(&buffer), b"hello");
    }

    #[test]
    fn reserve_reuses_unique_tail_spare_capacity() {
        let mut buffer = IoBuf::new();
        {
            let mut reserved = buffer.reserve::<1>();
            {
                let mut slices: [IoSliceMut<'_>; 1] =
                    std::array::from_fn(|_| IoSliceMut::new(&mut []));
                reserved.fill_io_slices(&mut slices);
                slices[0][..5].copy_from_slice(b"hello");
            }
            reserved.commit(5);
        }
        assert_eq!(buffer.chunks().count(), 1);

        {
            let mut reserved = buffer.reserve::<1>();
            {
                let mut slices: [IoSliceMut<'_>; 1] =
                    std::array::from_fn(|_| IoSliceMut::new(&mut []));
                reserved.fill_io_slices(&mut slices);
                slices[0][..3].copy_from_slice(b"!!!");
            }
            reserved.commit(3);
        }

        assert_eq!(buffer.chunks().count(), 1);
        assert_eq!(collect(&buffer), b"hello!!!");
    }

    #[test]
    fn builders_reuse_thread_local_append_tail() {
        LOCAL_BLOCK_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(active) = cache.active.take() {
                let pointer = block_pointer(active.pointer);
                release_active_reference(pointer);
            }
        });

        let mut first_builder = IoBufBuilder::new();
        first_builder.put_slice(b"abc");
        let first = first_builder.finish();

        let mut second_builder = IoBufBuilder::new();
        second_builder.put_slice(b"def");
        let second = second_builder.finish();

        let first_ref = first.refs.front().expect("first builder did not produce an IoBlockRef");
        let second_ref = second.refs.front().expect("second builder did not produce an IoBlockRef");
        assert_eq!(first_ref.block, second_ref.block);
        assert_eq!(second_ref.offset(), first_ref.end());
        assert_eq!(collect(&first), b"abc");
        assert_eq!(collect(&second), b"def");
    }

    fn collect(buffer: &IoBuf) -> Vec<u8> {
        let mut output = Vec::with_capacity(buffer.len());
        for chunk in buffer.chunks() {
            output.extend_from_slice(chunk);
        }
        output
    }
}
