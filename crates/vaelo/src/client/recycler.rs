// SPDX-License-Identifier: Apache-2.0

//! Coarse-grained maintenance for cached RPC client connections.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};

use crate::{Resource, StaticResource};

const RETIRING: u64 = 1 << 63;
const ACTIVE_MASK: u64 = !RETIRING;
/// Fixed client policy: retire a persistent RPC connection after five idle
/// minutes.
const CONNECTION_IDLE_TIMEOUT_MS: u64 = 5 * 60 * 1_000;
const BUCKETS_PER_TICK: usize = 4;
const SCAN_TICK: Duration = Duration::from_millis(64);

static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
static COARSE_NOW_MS: AtomicU64 = AtomicU64::new(1);
static START_RECYCLER: OnceLock<Result<(), String>> = OnceLock::new();

/// Shared activity state for one cached physical connection.
///
/// The common call path only touches `state`. Time is read from a coarse clock
/// maintained by the recycler, avoiding a clock read for every completed RPC.
#[derive(vaelo_derive::StaticResource)]
pub(crate) struct ClientConnectionState {
    state: AtomicU64,
    idle_since_ms: AtomicU64,
}

pub(crate) struct ClientActivityGuard {
    connection: Resource<ClientConnectionState>,
}

impl ClientConnectionState {
    pub(crate) fn create() -> crate::Result<Resource<Self>> {
        ensure_recycler()?;
        Self::resource_pool()
            .acquire_with_init(|| Self {
                state: AtomicU64::new(0),
                idle_since_ms: AtomicU64::new(coarse_now_ms()),
            })
            .map_err(|_| crate::Error::ResourceExhausted)
    }

    pub(crate) fn try_acquire(connection: &Resource<Self>) -> Option<ClientActivityGuard> {
        let mut current = connection.state.load(Ordering::Acquire);
        loop {
            if current & RETIRING != 0 || current & ACTIVE_MASK == ACTIVE_MASK {
                return None;
            }
            match connection.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(ClientActivityGuard { connection: connection.clone() }),
                Err(actual) => current = actual,
            }
        }
    }

    /// Atomically prevents new calls when the connection has remained idle.
    pub(crate) fn try_retire(&self, now_ms: u64) -> bool {
        if self.state.load(Ordering::Acquire) != 0 {
            return false;
        }
        let idle_since = self.idle_since_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(idle_since) < CONNECTION_IDLE_TIMEOUT_MS {
            return false;
        }
        self.state.compare_exchange(0, RETIRING, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }

    fn release(&self) {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            let active = current & ACTIVE_MASK;
            debug_assert!(current & RETIRING == 0, "active connection cannot already be retiring");
            debug_assert!(active > 0, "client connection activity underflow");
            if active == 1 {
                // Publish the idle timestamp before making zero activity visible
                // to the recycler's acquire load.
                self.idle_since_ms.store(coarse_now_ms(), Ordering::Relaxed);
            }
            match self.state.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for ClientActivityGuard {
    fn drop(&mut self) {
        self.connection.release();
    }
}

pub(crate) fn coarse_now_ms() -> u64 {
    COARSE_NOW_MS.load(Ordering::Relaxed)
}

fn refresh_coarse_clock() -> u64 {
    let elapsed = ORIGIN.elapsed().as_millis().min(u64::MAX as u128 - 1) as u64 + 1;
    COARSE_NOW_MS.store(elapsed, Ordering::Relaxed);
    elapsed
}

fn ensure_recycler() -> crate::Result<()> {
    match START_RECYCLER.get_or_init(|| {
        refresh_coarse_clock();
        std::thread::Builder::new()
            .name("client-connection-recycler".into())
            .spawn(run_recycler)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }) {
        Ok(()) => Ok(()),
        Err(error) => {
            Err(crate::Error::Channel(format!("start client connection recycler: {error}").into()))
        }
    }
}

fn run_recycler() {
    let mut next_bucket = 0;
    loop {
        std::thread::sleep(SCAN_TICK);
        let now_ms = refresh_coarse_clock();
        for _ in 0..BUCKETS_PER_TICK {
            super::brpc::recycle_idle_bucket(next_bucket, now_ms);
            crate::protocol::grpc::client::recycle_idle_bucket(next_bucket, now_ms);
            next_bucket = (next_bucket + 1) % crate::foundation::cow_hash_map::DEFAULT_BUCKETS;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_connection_cannot_retire_and_idle_connection_can() {
        let state = ClientConnectionState::create().unwrap();
        let guard = ClientConnectionState::try_acquire(&state).unwrap();
        assert!(!state.try_retire(u64::MAX));

        drop(guard);
        let idle_since = state.idle_since_ms.load(Ordering::Relaxed);
        assert!(!state.try_retire(idle_since + CONNECTION_IDLE_TIMEOUT_MS - 1));
        assert!(state.try_retire(idle_since + CONNECTION_IDLE_TIMEOUT_MS));
        assert!(ClientConnectionState::try_acquire(&state).is_none());
    }
}
