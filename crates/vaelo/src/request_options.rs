// SPDX-License-Identifier: Apache-2.0

use std::time::Instant;

use crate::MetadataMap;

#[derive(Debug, Default)]
pub(crate) struct RequestOptions {
    pub(crate) deadline: Option<Instant>,
    pub(crate) log_id: Option<u64>,
    pub(crate) request_id: Option<String>,
    pub(crate) metadata: MetadataMap,
    pub(crate) profile_sampled: bool,
}
