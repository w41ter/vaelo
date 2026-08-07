// SPDX-License-Identifier: Apache-2.0

use crate::{IoBuf, IoBufBuilder, Result};

/// Request codecs report their size before encoding so the controller can
/// reserve one contiguous wire buffer instead of growing it incrementally.
pub trait Encode {
    fn encoded_len(&self) -> usize;
    fn encode_to(&self, destination: &mut IoBufBuilder) -> Result<()>;
}

/// Responses decode into user-owned objects. This permits generated messages to
/// retain their internal `String` and `Vec` capacities across calls.
pub trait Decode {
    fn decode_from(&mut self, source: IoBuf) -> Result<()>;
}

impl<T: prost::Message> Encode for T {
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(self)
    }

    fn encode_to(&self, destination: &mut IoBufBuilder) -> Result<()> {
        prost::Message::encode(self, destination)
            .map_err(|error| crate::Error::Encode(error.to_string().into()))
    }
}

impl<T: prost::Message + Default> Decode for T {
    fn decode_from(&mut self, source: IoBuf) -> Result<()> {
        self.clear();
        prost::Message::merge(self, source)
            .map_err(|error| crate::Error::Decode(error.to_string().into()))
    }
}
