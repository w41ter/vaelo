// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::time::Duration;

/// Internal lookup key for a physical connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ConnectionKey {
    pub(crate) remote: SocketAddr,
    pub(crate) endpoint_id: u64,
    pub(crate) tag: u64,
    pub(crate) slot: u32,
    pub(crate) connect_timeout: Option<Duration>,
}
