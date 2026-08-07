// SPDX-License-Identifier: Apache-2.0

//! gRPC application protocol carried by an HTTP/2 stream.

pub(crate) mod client;
pub(crate) mod framing;
mod metadata;
pub(crate) mod server;

#[derive(Clone, Copy, Debug)]
pub(crate) struct GrpcConfig {
    pub max_decoding_message_size: usize,
    pub max_encoding_message_size: usize,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            max_decoding_message_size: 4 * 1024 * 1024,
            max_encoding_message_size: 4 * 1024 * 1024,
        }
    }
}
