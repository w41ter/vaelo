// SPDX-License-Identifier: Apache-2.0

//! Chunk-based lock-free lists of `u32` values.

use std::cell::UnsafeCell;
use std::mem::size_of;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use super::RadixTable;

const PAGE_BYTES: usize = 16 * 1024;
const FREE_CHUNK_BYTES: usize = 1024;
const FREE_CHUNK_ENTRIES: usize =
    (FREE_CHUNK_BYTES - size_of::<AtomicU32>() - size_of::<u16>() * 2) / size_of::<u32>();
const CHUNKS_PER_PAGE: u32 = (PAGE_BYTES / FREE_CHUNK_BYTES) as u32;
const CHUNK_INDEX_BITS: u32 = 25;
const CHUNK_INDEX_MASK: u64 = (1 << CHUNK_INDEX_BITS) - 1;
const NULL_CHUNK: u32 = CHUNK_INDEX_MASK as u32;
const CHUNK_TAG_MASK: u64 = (1 << (64 - CHUNK_INDEX_BITS)) - 1;

#[repr(C, align(64))]
pub(crate) struct FreeChunk {
    next: AtomicU32,
    data: UnsafeCell<FreeChunkData>,
}

#[repr(C)]
struct FreeChunkData {
    len: u16,
    reserved: u16,
    entries: [u32; FREE_CHUNK_ENTRIES],
}

impl FreeChunk {
    fn new() -> Self {
        Self {
            next: AtomicU32::new(NULL_CHUNK),
            data: UnsafeCell::new(FreeChunkData {
                len: 0,
                reserved: 0,
                entries: [0; FREE_CHUNK_ENTRIES],
            }),
        }
    }

    // A FreeChunkTable or FreeList owns the chunk index exclusively. Only the
    // owner may access data; next remains atomic because failed pops can race
    // with rapid reuse of the same chunk.
    pub(crate) fn len(&self) -> usize {
        unsafe { (*self.data.get()).len as usize }
    }

    pub(crate) fn is_full(&self) -> bool {
        self.len() == FREE_CHUNK_ENTRIES
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn push(&self, value: u32) {
        let data = unsafe { &mut *self.data.get() };
        debug_assert!((data.len as usize) < FREE_CHUNK_ENTRIES);
        data.entries[data.len as usize] = value;
        data.len += 1;
    }

    pub(crate) fn pop(&self) -> Option<u32> {
        let data = unsafe { &mut *self.data.get() };
        if data.len == 0 {
            return None;
        }
        data.len -= 1;
        Some(data.entries[data.len as usize])
    }

    fn clear(&self) {
        unsafe { (*self.data.get()).len = 0 };
    }
}

unsafe impl Sync for FreeChunk {}

const _: () = assert!(size_of::<FreeChunk>() == FREE_CHUNK_BYTES);

#[repr(C, align(16384))]
struct FreeChunkPage {
    chunks: [FreeChunk; CHUNKS_PER_PAGE as usize],
}

impl FreeChunkPage {
    fn new() -> Self {
        Self { chunks: std::array::from_fn(|_| FreeChunk::new()) }
    }
}

const _: () = assert!(size_of::<FreeChunkPage>() == PAGE_BYTES);

/// Append-only storage that allocates, resolves, and recycles FreeChunks.
pub(crate) struct FreeChunkTable {
    pages: RadixTable<FreeChunkPage>,
    next_page: AtomicU32,
    empty: AtomicU64,
}

impl FreeChunkTable {
    pub(crate) fn new() -> Self {
        Self {
            pages: RadixTable::new(),
            next_page: AtomicU32::new(0),
            empty: AtomicU64::new(NULL_CHUNK as u64),
        }
    }

    pub(crate) fn acquire(&self) -> u32 {
        if let Some(index) = pop(&self.empty, self) {
            return index;
        }
        let page_index = self.next_page.fetch_add(1, Ordering::Relaxed);
        assert!(page_index < (1 << 21), "process FreeChunk table exhausted");
        assert!(self.pages.insert(page_index as usize, FreeChunkPage::new()).is_ok());
        let first = page_index * CHUNKS_PER_PAGE;
        for index in (first + 1)..(first + CHUNKS_PER_PAGE) {
            if index != NULL_CHUNK {
                push(&self.empty, self, index);
            }
        }
        first
    }

    pub(crate) fn release(&self, index: u32) {
        debug_assert_eq!(self.get(index).len(), 0);
        push(&self.empty, self, index);
    }

    pub(crate) fn get(&self, index: u32) -> &FreeChunk {
        assert!(index != NULL_CHUNK && index < (1 << CHUNK_INDEX_BITS));
        let page = self
            .pages
            .get((index / CHUNKS_PER_PAGE) as usize)
            .expect("FreeChunk page was not published");
        &page.chunks[(index % CHUNKS_PER_PAGE) as usize]
    }
}

/// A thread-safe intrusive list of FreeChunks from one FreeChunkTable.
pub(crate) struct FreeList {
    table: &'static FreeChunkTable,
    head: AtomicU64,
}

impl FreeList {
    pub(crate) const fn new(table: &'static FreeChunkTable) -> Self {
        Self { table, head: AtomicU64::new(NULL_CHUNK as u64) }
    }

    pub(crate) fn push(&self, index: u32) {
        push(&self.head, self.table, index);
    }

    pub(crate) fn pop(&self) -> Option<u32> {
        pop(&self.head, self.table)
    }
}

impl Drop for FreeList {
    fn drop(&mut self) {
        while let Some(index) = self.pop() {
            self.table.get(index).clear();
            self.table.release(index);
        }
    }
}

fn push(head: &AtomicU64, table: &FreeChunkTable, index: u32) {
    let chunk = table.get(index);
    let mut old = head.load(Ordering::Relaxed);
    loop {
        let (tag, head_index) = unpack_head(old);
        chunk.next.store(head_index, Ordering::Relaxed);
        let new = pack_head(tag.wrapping_add(1), index);
        match head.compare_exchange_weak(old, new, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => old = actual,
        }
    }
}

fn pop(head: &AtomicU64, table: &FreeChunkTable) -> Option<u32> {
    let mut old = head.load(Ordering::Acquire);
    loop {
        let (tag, index) = unpack_head(old);
        if index == NULL_CHUNK {
            return None;
        }
        let next = table.get(index).next.load(Ordering::Relaxed);
        let new = pack_head(tag.wrapping_add(1), next);
        match head.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Some(index),
            Err(actual) => old = actual,
        }
    }
}

const fn pack_head(tag: u64, index: u32) -> u64 {
    ((tag & CHUNK_TAG_MASK) << CHUNK_INDEX_BITS) | index as u64
}

const fn unpack_head(raw: u64) -> (u64, u32) {
    (raw >> CHUNK_INDEX_BITS, (raw & CHUNK_INDEX_MASK) as u32)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use super::*;

    static TABLE: LazyLock<FreeChunkTable> = LazyLock::new(FreeChunkTable::new);

    #[test]
    fn table_expands_and_reuses_unique_indices() {
        let mut indices = (0..1_000).map(|_| TABLE.acquire()).collect::<Vec<_>>();
        indices.sort_unstable();
        assert!(indices.windows(2).all(|pair| pair[0] != pair[1]));
        for index in indices {
            assert_eq!(TABLE.get(index).len(), 0);
            TABLE.release(index);
        }
    }

    #[test]
    fn list_survives_concurrent_index_reuse() {
        let list = Arc::new(FreeList::new(&TABLE));
        let indices = (0..256).map(|_| TABLE.acquire()).collect::<Vec<_>>();
        for &index in &indices {
            list.push(index);
        }
        let mut workers = Vec::new();
        for _ in 0..8 {
            let list = list.clone();
            workers.push(std::thread::spawn(move || {
                for _ in 0..10_000 {
                    loop {
                        if let Some(index) = list.pop() {
                            list.push(index);
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let mut returned = Vec::new();
        while let Some(index) = list.pop() {
            returned.push(index);
        }
        returned.sort_unstable();
        let mut expected = indices;
        expected.sort_unstable();
        assert_eq!(returned, expected);
        for index in returned {
            TABLE.release(index);
        }
    }

    #[test]
    fn fixed_layout_has_requested_size() {
        assert_eq!(size_of::<FreeChunk>(), FREE_CHUNK_BYTES);
    }
}
