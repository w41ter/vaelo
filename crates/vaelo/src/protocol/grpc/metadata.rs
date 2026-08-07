// SPDX-License-Identifier: Apache-2.0

use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::{Engine as _, alphabet};
use bytes::Bytes;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode, percent_encode};

use crate::protocol::http2::hpack::{HeaderBlock, HeaderField, HeaderIndexing};
use crate::{MetadataMap, MetadataValue, Status, StatusCode};

const GRPC_MESSAGE_ENCODING_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'?')
    .add(b'{')
    .add(b'}');

const BASE64_STANDARD: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

const BASE64_STANDARD_NO_PAD: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

pub(super) fn append_metadata(
    headers: &mut HeaderBlock,
    metadata: &MetadataMap,
) -> Result<(), Status> {
    validate_metadata(metadata)?;
    append_metadata_unchecked(headers, metadata);
    Ok(())
}

pub(super) fn validate_metadata(metadata: &MetadataMap) -> Result<(), Status> {
    for entry in metadata.iter() {
        let key = entry.key();
        if is_reserved_header(key.as_bytes()) {
            return Err(Status::new(
                StatusCode::InvalidArgument,
                format!("reserved gRPC metadata key: {key}"),
            ));
        }
        if !key.ends_with("-bin") && !entry.value().iter().all(|byte| matches!(byte, 0x20..=0x7e)) {
            return Err(Status::new(
                StatusCode::InvalidArgument,
                "non-binary gRPC metadata must contain visible ASCII",
            ));
        }
    }
    Ok(())
}

pub(super) fn append_metadata_unchecked(headers: &mut HeaderBlock, metadata: &MetadataMap) {
    for entry in metadata.iter() {
        let value = if entry.key().ends_with("-bin") {
            Bytes::from(BASE64_STANDARD_NO_PAD.encode(entry.value()))
        } else {
            Bytes::copy_from_slice(entry.value())
        };
        headers.push(HeaderField {
            name: Bytes::copy_from_slice(entry.key().as_bytes()),
            value,
            indexing: HeaderIndexing::WithoutIndexing,
        });
    }
}

pub(super) fn metadata_from_headers(headers: &HeaderBlock) -> Result<MetadataMap, Status> {
    let mut metadata = MetadataMap::new();
    for field in headers.fields() {
        if is_reserved_header(field.name.as_ref()) {
            continue;
        }
        let key = std::str::from_utf8(field.name.as_ref())
            .map_err(|_| Status::new(StatusCode::Internal, "gRPC metadata key is not ASCII"))?;
        let value = if key.ends_with("-bin") {
            BASE64_STANDARD.decode(field.value.as_ref()).map_err(|_| {
                Status::new(StatusCode::Internal, "invalid base64 gRPC binary metadata")
            })?
        } else {
            field.value.to_vec()
        };
        metadata
            .append(key.to_owned(), MetadataValue::from_vec(value))
            .map_err(|error| Status::new(StatusCode::Internal, error.to_string()))?;
    }
    Ok(metadata)
}

pub(super) fn encode_grpc_message(message: &str) -> Bytes {
    Bytes::from(percent_encode(message.as_bytes(), GRPC_MESSAGE_ENCODING_SET).to_string())
}

pub(super) fn decode_grpc_message(value: &[u8]) -> Result<String, Status> {
    percent_decode(value)
        .decode_utf8()
        .map(|message| message.into_owned())
        .map_err(|_| Status::new(StatusCode::Unknown, "grpc-message is not valid UTF-8"))
}

pub(super) fn encode_status_details(details: &[u8]) -> Bytes {
    Bytes::from(BASE64_STANDARD_NO_PAD.encode(details))
}

pub(super) fn decode_status_details(value: &[u8]) -> Result<Vec<u8>, Status> {
    BASE64_STANDARD
        .decode(value)
        .map_err(|_| Status::new(StatusCode::Internal, "invalid grpc-status-details-bin"))
}

fn is_reserved_header(name: &[u8]) -> bool {
    name.starts_with(b":")
        || matches!(
            name,
            b"content-type"
                | b"te"
                | b"grpc-timeout"
                | b"grpc-status"
                | b"grpc-message"
                | b"grpc-status-details-bin"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_and_binary_values_round_trip() {
        let message = "leader changed: 50% / 节点";
        assert_eq!(decode_grpc_message(&encode_grpc_message(message)).unwrap(), message);

        let details = b"\0\xfftest-structured-error";
        assert_eq!(decode_status_details(&encode_status_details(details)).unwrap(), details);
    }

    #[test]
    fn metadata_rejects_reserved_keys_and_untyped_binary_values() {
        let mut metadata = MetadataMap::new();
        metadata.insert("grpc-status", "0".to_owned()).unwrap();
        assert_eq!(validate_metadata(&metadata).unwrap_err().code(), StatusCode::InvalidArgument);

        let mut metadata = MetadataMap::new();
        metadata.insert("trace-context", vec![0xff]).unwrap();
        assert_eq!(validate_metadata(&metadata).unwrap_err().code(), StatusCode::InvalidArgument);

        let mut metadata = MetadataMap::new();
        metadata.insert("trace-context-bin", vec![0xff]).unwrap();
        validate_metadata(&metadata).unwrap();
    }
}
