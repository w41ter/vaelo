// SPDX-License-Identifier: Apache-2.0

//! Low-overhead variable primitives and exposed variables.
//!
//! Primitive variables use arena-backed slots and fixed thread-local shards.
//! Exposed variables are registered by name and described on demand, following
//! bvar's split between hot-path storage and exposed variables.

mod arena;
mod primitive;
mod registry;

pub use primitive::{Balance, Counter, Gauge, LatencyRecorder, LatencySnapshot, MaxRecorder};
pub(crate) use registry::{
    DEFAULT_WINDOW, expose_counter_per_second, expose_counter_window, expose_latency_window,
};
pub use registry::{
    ExposeError, VariableSnapshot, VariableValue, dump, expose_balance, expose_counter,
    expose_gauge, expose_latency, expose_max, hide,
};
