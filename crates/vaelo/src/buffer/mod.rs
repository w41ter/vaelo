// SPDX-License-Identifier: Apache-2.0

//! Network-oriented buffers and their allocation policy.

mod iobuf;

pub use iobuf::{IoBuf, IoBufBuilder, IoBufChunks, IoBufMut};
