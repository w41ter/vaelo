// SPDX-License-Identifier: Apache-2.0

//! Byte transports are deliberately unaware of RPC framing and correlation.

pub(crate) mod connection;
pub(crate) mod inflight;
pub(crate) mod tcp;

use std::io;
use std::io::{IoSlice, IoSliceMut};

pub use tcp::TcpTransport;

pub trait Transport: Send + Sync + 'static {
    fn try_read_vectored(&self, destination: &mut [IoSliceMut<'_>]) -> io::Result<usize>;
    fn try_write_vectored(&self, source: &[IoSlice<'_>]) -> io::Result<usize>;
}
