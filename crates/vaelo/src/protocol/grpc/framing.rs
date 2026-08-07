// SPDX-License-Identifier: Apache-2.0

//! Incremental gRPC length-prefixed message framing.

use bytes::{Buf, BufMut};

use crate::{IoBuf, IoBufBuilder, Status, StatusCode};

pub(crate) const MESSAGE_PREFIX_LEN: usize = 5;

pub(crate) struct MessageStreamDecoder {
    max_message_size: usize,
    prefix: [u8; MESSAGE_PREFIX_LEN],
    prefix_len: usize,
    expected_len: Option<usize>,
    payload: IoBuf,
}

impl MessageStreamDecoder {
    pub fn new(max_message_size: usize) -> Self {
        Self {
            max_message_size,
            prefix: [0; MESSAGE_PREFIX_LEN],
            prefix_len: 0,
            expected_len: None,
            payload: IoBuf::new(),
        }
    }

    #[cfg(test)]
    fn push(&mut self, data: IoBuf, end_stream: bool) -> Result<StreamDecodeProgress, Status> {
        let mut messages = Vec::new();
        self.push_each(data, end_stream, |message| {
            messages.push(message);
            Ok(())
        })?;
        Ok(StreamDecodeProgress { messages })
    }

    pub(crate) fn push_each(
        &mut self,
        mut data: IoBuf,
        end_stream: bool,
        mut emit: impl FnMut(IoBuf) -> Result<(), Status>,
    ) -> Result<(), Status> {
        while !data.is_empty() {
            while self.prefix_len < MESSAGE_PREFIX_LEN && !data.is_empty() {
                self.prefix[self.prefix_len] = first_byte(&data);
                data.consume(1);
                self.prefix_len += 1;
            }

            if self.prefix_len == MESSAGE_PREFIX_LEN && self.expected_len.is_none() {
                if self.prefix[0] != 0 {
                    return Err(Status::new(
                        StatusCode::Unimplemented,
                        "compressed gRPC messages are not supported",
                    ));
                }
                let len = u32::from_be_bytes([
                    self.prefix[1],
                    self.prefix[2],
                    self.prefix[3],
                    self.prefix[4],
                ]) as usize;
                if len > self.max_message_size {
                    return Err(Status::new(
                        StatusCode::ResourceExhausted,
                        "gRPC message exceeds limit",
                    ));
                }
                self.expected_len = Some(len);
            }

            if let Some(expected_len) = self.expected_len {
                let missing = expected_len.saturating_sub(self.payload.len());
                let take = missing.min(data.len());
                if take > 0 {
                    let mut chunk = data.split_to(take);
                    self.payload.append_movable(&mut chunk);
                }
                if self.payload.len() == expected_len {
                    emit(std::mem::take(&mut self.payload))?;
                    self.prefix = [0; MESSAGE_PREFIX_LEN];
                    self.prefix_len = 0;
                    self.expected_len = None;
                }
            }
        }

        if end_stream && (self.prefix_len != 0 || self.expected_len.is_some()) {
            return Err(Status::new(
                StatusCode::Internal,
                "gRPC request ended before a complete message arrived",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
struct StreamDecodeProgress {
    messages: Vec<IoBuf>,
}

pub(crate) fn encode_message(mut payload: IoBuf, max_size: usize) -> Result<IoBuf, Status> {
    if payload.len() > max_size || payload.len() > u32::MAX as usize {
        return Err(Status::new(StatusCode::ResourceExhausted, "gRPC message exceeds limit"));
    }
    let mut builder = IoBufBuilder::new();
    builder.put_u8(0);
    builder.put_u32(payload.len() as u32);
    let mut output = builder.finish();
    output.append_movable(&mut payload);
    Ok(output)
}

fn first_byte(input: &IoBuf) -> u8 {
    input.chunk().first().copied().expect("non-empty IoBuf has a first byte")
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    fn iobuf(bytes: &[u8]) -> IoBuf {
        let mut builder = IoBufBuilder::new();
        builder.put_slice(bytes);
        builder.finish()
    }

    fn flatten(input: IoBuf) -> Vec<u8> {
        input.chunks().flatten().copied().collect()
    }

    #[test]
    fn stream_decoder_accepts_multiple_fragmented_messages() {
        let mut decoder = MessageStreamDecoder::new(16);
        let first = encode_message(iobuf(&[1, 2]), 16).unwrap();
        let second = encode_message(iobuf(&[3, 4, 5]), 16).unwrap();
        let mut bytes = flatten(first);
        bytes.extend(flatten(second));

        assert!(decoder.push(iobuf(&bytes[..3]), false).unwrap().messages.is_empty());
        let progress = decoder.push(iobuf(&bytes[3..8]), false).unwrap();
        assert_eq!(progress.messages.len(), 1);
        assert_eq!(flatten(progress.messages.into_iter().next().unwrap()), [1, 2]);

        let progress = decoder.push(iobuf(&bytes[8..]), true).unwrap();
        assert_eq!(progress.messages.len(), 1);
        assert_eq!(flatten(progress.messages.into_iter().next().unwrap()), [3, 4, 5]);
    }
}
