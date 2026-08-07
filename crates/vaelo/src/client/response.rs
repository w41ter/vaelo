// SPDX-License-Identifier: Apache-2.0

use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::{IoBuf, MetadataMap};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CallStats {
    /// End-to-end latency when per-call timing was enabled.
    ///
    /// Keeping this optional allows runtimes to disable or sample clock reads
    /// on latency-sensitive paths. Applications must not depend on timing
    /// being present unless they explicitly enable an observation policy.
    latency: Option<Duration>,
}

impl CallStats {
    pub fn latency(self) -> Option<Duration> {
        self.latency
    }

    pub(crate) const fn observed_latency(latency: Duration) -> Self {
        Self { latency: Some(latency) }
    }
}

/// A decoded RPC response.
///
/// Initial metadata and trailing metadata are retained independently. Empty
/// metadata must remain allocation-free in the implementation. Protocol-only
/// response data, such as a bRPC attachment, is exposed by the corresponding
/// protocol extension API.
pub struct Response<T> {
    message: T,
    attachment: IoBuf,
    metadata: MetadataMap,
    trailers: MetadataMap,
    stats: CallStats,
}

impl<T> Response<T> {
    pub fn new(message: T) -> Self {
        Self {
            message,
            attachment: IoBuf::new(),
            metadata: MetadataMap::new(),
            trailers: MetadataMap::new(),
            stats: CallStats::default(),
        }
    }

    pub(crate) fn from_parts(message: T, attachment: IoBuf, stats: CallStats) -> Self {
        Self {
            message,
            attachment,
            metadata: MetadataMap::new(),
            trailers: MetadataMap::new(),
            stats,
        }
    }

    pub fn get_ref(&self) -> &T {
        &self.message
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.message
    }

    pub fn into_inner(self) -> T {
        self.message
    }

    pub(crate) fn attachment(&self) -> &IoBuf {
        &self.attachment
    }

    pub(crate) fn take_attachment(&mut self) -> IoBuf {
        std::mem::take(&mut self.attachment)
    }

    pub fn stats(&self) -> CallStats {
        self.stats
    }

    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.metadata
    }

    /// Returns completed trailing metadata for unary responses.
    ///
    /// For a `Response<Streaming<T>>`, final trailers belong to the Streaming
    /// value because they do not exist when the initial response arrives.
    pub fn trailers(&self) -> &MetadataMap {
        &self.trailers
    }

    pub fn trailers_mut(&mut self) -> &mut MetadataMap {
        &mut self.trailers
    }

    pub(crate) fn into_parts(self) -> (T, IoBuf, MetadataMap, MetadataMap, CallStats) {
        (self.message, self.attachment, self.metadata, self.trailers, self.stats)
    }

    pub(crate) fn set_attachment(&mut self, attachment: IoBuf) {
        self.attachment = attachment;
    }
}

impl<T> Deref for Response<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl<T> DerefMut for Response<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
    }
}
