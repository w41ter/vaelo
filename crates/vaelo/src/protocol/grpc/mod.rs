// SPDX-License-Identifier: Apache-2.0

//! gRPC application protocol carried by an HTTP/2 stream.

pub(crate) mod client;
pub(crate) mod framing;
mod metadata;
pub(crate) mod server;

use crate::protocol::http2::ErrorCode;
use crate::{Status, StatusCode};

/// Per-message limits for the gRPC protocol.
///
/// These limits apply to each length-prefixed gRPC message, not to the total
/// byte count of a streaming RPC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrpcOptions {
    pub(crate) max_decoding_message_size: usize,
    pub(crate) max_encoding_message_size: usize,
}

impl GrpcOptions {
    pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

    /// Sets the largest message accepted from the peer.
    pub const fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.max_decoding_message_size = limit;
        self
    }

    /// Sets the largest message sent to the peer.
    pub const fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.max_encoding_message_size = limit;
        self
    }

    pub const fn decoding_message_size_limit(&self) -> usize {
        self.max_decoding_message_size
    }

    pub const fn encoding_message_size_limit(&self) -> usize {
        self.max_encoding_message_size
    }
}

impl Default for GrpcOptions {
    fn default() -> Self {
        Self {
            max_decoding_message_size: Self::DEFAULT_MAX_MESSAGE_SIZE,
            max_encoding_message_size: Self::DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

pub(crate) fn status_from_h2_reset(code: u32) -> Status {
    let Some(error) = ErrorCode::from_u32(code) else {
        return Status::new(StatusCode::Unknown, format!("gRPC stream reset: UNKNOWN ({code})"));
    };
    let status = match error {
        ErrorCode::NoError
        | ErrorCode::ProtocolError
        | ErrorCode::InternalError
        | ErrorCode::FlowControlError
        | ErrorCode::SettingsTimeout
        | ErrorCode::StreamClosed
        | ErrorCode::FrameSizeError
        | ErrorCode::CompressionError
        | ErrorCode::ConnectError => StatusCode::Internal,
        ErrorCode::RefusedStream => StatusCode::Unavailable,
        ErrorCode::Cancel => StatusCode::Canceled,
        ErrorCode::EnhanceYourCalm => StatusCode::ResourceExhausted,
        ErrorCode::InadequateSecurity => StatusCode::PermissionDenied,
        ErrorCode::Http11Required => StatusCode::Unknown,
    };
    Status::new(status, format!("gRPC stream reset: {} ({code})", error.wire_name()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http2_reset_codes_follow_grpc_status_mapping() {
        let cases = [
            (ErrorCode::NoError, StatusCode::Internal),
            (ErrorCode::ProtocolError, StatusCode::Internal),
            (ErrorCode::StreamClosed, StatusCode::Internal),
            (ErrorCode::FrameSizeError, StatusCode::Internal),
            (ErrorCode::RefusedStream, StatusCode::Unavailable),
            (ErrorCode::Cancel, StatusCode::Canceled),
            (ErrorCode::EnhanceYourCalm, StatusCode::ResourceExhausted),
            (ErrorCode::InadequateSecurity, StatusCode::PermissionDenied),
            (ErrorCode::Http11Required, StatusCode::Unknown),
        ];
        for (error, expected) in cases {
            let status = status_from_h2_reset(error as u32);
            assert_eq!(status.code(), expected, "unexpected mapping for {error:?}");
            assert!(status.message().contains(error.wire_name()));
        }
        assert_eq!(status_from_h2_reset(u32::MAX).code(), StatusCode::Unknown);
    }
}
