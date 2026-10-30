// SPDX-License-Identifier: Apache-2.0

use std::cell::RefCell;
use std::hint::black_box;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use vaelo::__private::{Resource, ResourceLocal, ResourcePool, ResourcePoolConfig, StaticResource};

const MILLION: usize = 1_000_000;

// MaybeUninit keeps this benchmark focused on allocation and recycling rather
// than repeatedly zeroing or constructing the 64-byte payload.
struct BenchResource(MaybeUninit<[u8; 64]>);

impl BenchResource {
    #[inline]
    fn uninit() -> Self {
        Self(MaybeUninit::uninit())
    }

    #[inline]
    fn address(&self) -> *const u8 {
        self.0.as_ptr().cast()
    }
}

impl Default for BenchResource {
    fn default() -> Self {
        Self::uninit()
    }
}

impl StaticResource for BenchResource {
    fn resource_pool() -> &'static ResourcePool<Self> {
        static POOL: std::sync::LazyLock<ResourcePool<BenchResource>> =
            std::sync::LazyLock::new(|| {
                ResourcePool::new(ResourcePoolConfig { max_items: 1_100_000 })
            });
        &POOL
    }

    fn try_with_resource_local<R>(
        operation: impl FnOnce(&mut ResourceLocal<Self>) -> R,
    ) -> Option<R> {
        thread_local! {
            static LOCAL: RefCell<ResourceLocal<BenchResource>> =
                RefCell::new(ResourceLocal::new(BenchResource::resource_pool()));
        }
        LOCAL.try_with(|local| operation(&mut local.borrow_mut())).ok()
    }
}

#[inline]
fn pool_allocate() -> Resource<BenchResource> {
    BenchResource::resource_pool().acquire().unwrap()
}

#[inline]
fn malloc_allocate() -> Box<MaybeUninit<[u8; 64]>> {
    Box::new(MaybeUninit::uninit())
}

fn steady_state(c: &mut Criterion) {
    // Warm both allocators before measuring their thread-local fast paths.
    drop(pool_allocate());
    drop(malloc_allocate());

    let mut group = c.benchmark_group("million_sequential_allocations");
    group.throughput(Throughput::Elements(MILLION as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("resource_pool", |b| {
        b.iter(|| {
            for _ in 0..MILLION {
                let resource = pool_allocate();
                black_box(resource.address());
                drop(resource);
            }
        });
    });

    group.bench_function("box_malloc", |b| {
        b.iter(|| {
            for _ in 0..MILLION {
                let resource = malloc_allocate();
                black_box(resource.as_ptr());
                drop(resource);
            }
        });
    });
    group.finish();
}

fn live_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("live_allocation_burst");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    for count in [1_024usize, 65_536, MILLION] {
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::new("resource_pool", count), &count, |b, &count| {
            b.iter(|| {
                let mut resources = Vec::with_capacity(count);
                for _ in 0..count {
                    let resource = pool_allocate();
                    black_box(resource.address());
                    resources.push(resource);
                }
                black_box(&resources);
                drop(resources);
            });
        });

        group.bench_with_input(BenchmarkId::new("box_malloc", count), &count, |b, &count| {
            b.iter(|| {
                let mut resources = Vec::with_capacity(count);
                for _ in 0..count {
                    let resource = malloc_allocate();
                    black_box(resource.as_ptr());
                    resources.push(resource);
                }
                black_box(&resources);
                drop(resources);
            });
        });
    }
    group.finish();
}

fn hot_path_components(c: &mut Criterion) {
    let anchor = pool_allocate();
    let id = anchor.id();
    let atomic = AtomicU64::new(0);
    let mut group = c.benchmark_group("million_hot_path_components");
    group.throughput(Throughput::Elements(MILLION as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("resource_clone_drop", |b| {
        b.iter(|| {
            for _ in 0..MILLION {
                let resource = anchor.clone();
                black_box(resource.address());
                drop(resource);
            }
        });
    });

    group.bench_function("resource_get_drop", |b| {
        b.iter(|| {
            for _ in 0..MILLION {
                let resource = BenchResource::resource_pool().get(id).unwrap();
                black_box(resource.address());
                drop(resource);
            }
        });
    });

    group.bench_function("epoch_pin", |b| {
        b.iter(|| {
            for _ in 0..MILLION {
                black_box(crossbeam_epoch::pin());
            }
        });
    });

    group.bench_function("resource_type_tls", |b| {
        b.iter(|| {
            for _ in 0..MILLION {
                BenchResource::try_with_resource_local(|local| {
                    black_box(local as *mut ResourceLocal<BenchResource>);
                })
                .unwrap();
            }
        });
    });

    group.bench_function("atomic_compare_exchange", |b| {
        b.iter(|| {
            for _ in 0..MILLION {
                let old = atomic.load(Ordering::Relaxed);
                let _ = black_box(atomic.compare_exchange_weak(
                    old,
                    old.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ));
            }
        });
    });
    group.finish();
    drop(anchor);
}

#[derive(Clone, Copy)]
struct FastRandom(u64);

impl FastRandom {
    fn next(&mut self, upper_bound: usize) -> usize {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        (value as usize) % upper_bound
    }
}

fn randomized_million_live_churn(c: &mut Criterion) {
    const BATCH: usize = 10_000;
    const ROUNDS: usize = MILLION / BATCH;

    let mut group = c.benchmark_group("million_live_randomized_churn");
    group.throughput(Throughput::Elements(MILLION as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("resource_pool", |b| {
        b.iter_custom(|iterations| {
            let mut total = Duration::ZERO;
            for iteration in 0..iterations {
                let mut live = (0..MILLION).map(|_| pool_allocate()).collect::<Vec<_>>();
                let mut random = FastRandom(0x9e37_79b9_7f4a_7c15 ^ iteration);
                let start = Instant::now();
                for _ in 0..ROUNDS {
                    for _ in 0..BATCH {
                        live.push(pool_allocate());
                    }
                    for _ in 0..BATCH {
                        let index = random.next(live.len());
                        drop(live.swap_remove(index));
                    }
                }
                total += start.elapsed();
                black_box(&live);
                drop(live);
            }
            total
        });
    });

    group.bench_function("box_malloc", |b| {
        b.iter_custom(|iterations| {
            let mut total = Duration::ZERO;
            for iteration in 0..iterations {
                let mut live = (0..MILLION).map(|_| malloc_allocate()).collect::<Vec<_>>();
                let mut random = FastRandom(0x9e37_79b9_7f4a_7c15 ^ iteration);
                let start = Instant::now();
                for _ in 0..ROUNDS {
                    for _ in 0..BATCH {
                        live.push(malloc_allocate());
                    }
                    for _ in 0..BATCH {
                        let index = random.next(live.len());
                        drop(live.swap_remove(index));
                    }
                }
                total += start.elapsed();
                black_box(&live);
                drop(live);
            }
            total
        });
    });
    group.finish();
}

fn cross_thread_remote_free(c: &mut Criterion) {
    const BATCH: usize = 10_000;
    const BATCHES: usize = MILLION / BATCH;
    let mut group = c.benchmark_group("million_cross_thread_remote_free");
    group.throughput(Throughput::Elements(MILLION as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("resource_pool", |b| {
        b.iter(|| {
            let (sender, receiver) = sync_channel::<Vec<Resource<BenchResource>>>(8);
            let consumer = std::thread::spawn(move || {
                for resources in receiver {
                    black_box(&resources);
                    drop(resources);
                }
            });
            for _ in 0..BATCHES {
                let resources = (0..BATCH).map(|_| pool_allocate()).collect();
                sender.send(resources).unwrap();
            }
            drop(sender);
            consumer.join().unwrap();
        });
    });

    group.bench_function("box_malloc", |b| {
        b.iter(|| {
            let (sender, receiver) = sync_channel::<Vec<Box<MaybeUninit<[u8; 64]>>>>(8);
            let consumer = std::thread::spawn(move || {
                for resources in receiver {
                    black_box(&resources);
                    drop(resources);
                }
            });
            for _ in 0..BATCHES {
                let resources = (0..BATCH).map(|_| malloc_allocate()).collect();
                sender.send(resources).unwrap();
            }
            drop(sender);
            consumer.join().unwrap();
        });
    });
    group.finish();
}

fn rpc_concurrency_window(c: &mut Criterion) {
    const IN_FLIGHT: usize = 10_000;
    let mut group = c.benchmark_group("million_rpc_call_lifecycles_10k_in_flight");
    group.throughput(Throughput::Elements(MILLION as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("resource_pool", |b| {
        b.iter(|| {
            let mut calls = (0..IN_FLIGHT).map(|_| pool_allocate()).collect::<Vec<_>>();
            let mut random = FastRandom(0xd1b5_4a32_d192_ed03);
            for _ in 0..MILLION {
                let completed = random.next(calls.len());
                drop(calls.swap_remove(completed));
                calls.push(pool_allocate());
            }
            black_box(calls);
        });
    });

    group.bench_function("box_malloc", |b| {
        b.iter(|| {
            let mut calls = (0..IN_FLIGHT).map(|_| malloc_allocate()).collect::<Vec<_>>();
            let mut random = FastRandom(0xd1b5_4a32_d192_ed03);
            for _ in 0..MILLION {
                let completed = random.next(calls.len());
                drop(calls.swap_remove(completed));
                calls.push(malloc_allocate());
            }
            black_box(calls);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    steady_state,
    live_burst,
    hot_path_components,
    randomized_million_live_churn,
    cross_thread_remote_free,
    rpc_concurrency_window,
);
criterion_main!(benches);
