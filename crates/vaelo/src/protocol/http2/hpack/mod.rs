// SPDX-License-Identifier: Apache-2.0

//! HPACK state and header-block representation.
//!
//! The decoder owns the peer's dynamic table and the encoder owns the local
//! dynamic table. Both are connection-scoped and therefore stay inside the
//! single-owner connection driver.

mod huffman;

use std::ops::{Deref, DerefMut};

use bytes::{Buf, BufMut, Bytes};

use super::{ConnectionError, ErrorCode, H2Error};
use crate::{IoBuf, IoBufBuilder, Resource, StaticResource};

const INLINE_HEADER_FIELDS: usize = 8;

#[derive(Clone, Debug, Default)]
pub(crate) struct PseudoHeaders {
    pub method: Option<Bytes>,
    pub scheme: Option<Bytes>,
    pub authority: Option<Bytes>,
    pub path: Option<Bytes>,
    pub status: Option<u16>,
    pub protocol: Option<Bytes>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeaderIndexing {
    Indexed,
    WithoutIndexing,
    NeverIndexed,
}

#[derive(Clone, Debug)]
pub(crate) struct HeaderField {
    pub name: Bytes,
    pub value: Bytes,
    pub indexing: HeaderIndexing,
}

pub(crate) struct HeaderBlock {
    storage: Resource<HeaderBlockStorage>,
}

#[derive(Clone, Debug, vaelo_derive::StaticResource)]
pub(crate) struct HeaderBlockStorage {
    pub pseudo: PseudoHeaders,
    fields: HeaderFields,
}

impl HeaderBlock {
    pub fn new() -> Self {
        Self {
            storage: HeaderBlockStorage::resource_pool()
                .acquire_with_init(|| HeaderBlockStorage {
                    pseudo: PseudoHeaders::default(),
                    fields: HeaderFields::new(),
                })
                .expect("HTTP/2 header block resource pool exhausted"),
        }
    }

    pub fn push(&mut self, field: HeaderField) {
        self.fields.push(field);
    }

    pub fn fields(&self) -> impl Iterator<Item = &HeaderField> {
        self.fields.iter()
    }
}

impl Clone for HeaderBlock {
    fn clone(&self) -> Self {
        Self {
            storage: HeaderBlockStorage::resource_pool()
                .acquire_with_init(|| (**self).clone())
                .expect("HTTP/2 header block resource pool exhausted"),
        }
    }
}

impl std::fmt::Debug for HeaderBlock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.storage.fmt(formatter)
    }
}

impl Deref for HeaderBlock {
    type Target = HeaderBlockStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl DerefMut for HeaderBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: HeaderBlock never exposes its ResourceId, and Clone performs
        // a deep copy instead of sharing the backing resource.
        unsafe { self.storage.get_mut_unchecked() }
            .expect("HTTP/2 header block mutated after being shared")
    }
}

#[derive(Clone, Debug)]
struct HeaderFields {
    inline: [Option<HeaderField>; INLINE_HEADER_FIELDS],
    overflow: Vec<HeaderField>,
}

impl HeaderFields {
    fn new() -> Self {
        Self { inline: std::array::from_fn(|_| None), overflow: Vec::new() }
    }

    fn push(&mut self, field: HeaderField) {
        if let Some(slot) = self.inline.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(field);
        } else {
            self.overflow.push(field);
        }
    }

    fn iter(&self) -> impl Iterator<Item = &HeaderField> {
        self.inline.iter().filter_map(Option::as_ref).chain(self.overflow.iter())
    }
}

impl Default for HeaderBlock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HpackConfig {
    pub max_dynamic_table_size: usize,
    pub max_header_block_size: usize,
    pub max_header_list_size: usize,
    pub max_header_fields: usize,
}

impl Default for HpackConfig {
    fn default() -> Self {
        Self {
            max_dynamic_table_size: 4096,
            max_header_block_size: 32 * 1024,
            max_header_list_size: 32 * 1024,
            max_header_fields: 128,
        }
    }
}

pub(crate) struct HpackDecoder {
    config: HpackConfig,
    dynamic_table: DynamicTable,
}

impl HpackDecoder {
    pub fn new(config: HpackConfig) -> Self {
        Self { dynamic_table: DynamicTable::new(config.max_dynamic_table_size), config }
    }

    pub fn max_header_block_size(&self) -> usize {
        self.config.max_header_block_size
    }

    pub fn decode(&mut self, mut block: IoBuf) -> Result<HeaderBlock, H2Error> {
        let mut headers = HeaderBlock::new();
        let mut fields = 0;
        let mut list_size = 0;
        let mut regular_seen = false;
        let mut can_resize = true;

        while block.has_remaining() {
            let first = block.chunk()[0];
            let (name, value, indexing) = if first & 0x80 != 0 {
                can_resize = false;
                let index = decode_integer(&mut block, 7)?;
                let field = self.dynamic_table.get(index)?;
                (field.name, field.value, HeaderIndexing::Indexed)
            } else if first & 0x40 != 0 {
                can_resize = false;
                let field = self.decode_literal(&mut block, 6, HeaderIndexing::Indexed)?;
                self.dynamic_table.insert(field.clone());
                (field.name, field.value, field.indexing)
            } else if first & 0x20 != 0 {
                if !can_resize {
                    return Err(compression_error(
                        "HPACK table-size update appears after a header field",
                    ));
                }
                let size = decode_integer(&mut block, 5)?;
                if size > self.config.max_dynamic_table_size {
                    return Err(compression_error("HPACK dynamic table size exceeds local limit"));
                }
                self.dynamic_table.set_max_size(size);
                continue;
            } else {
                can_resize = false;
                let indexing = if first & 0x10 != 0 {
                    HeaderIndexing::NeverIndexed
                } else {
                    HeaderIndexing::WithoutIndexing
                };
                let field = self.decode_literal(&mut block, 4, indexing)?;
                (field.name, field.value, field.indexing)
            };

            fields += 1;
            list_size += name.len() + value.len() + 32;
            if fields > self.config.max_header_fields
                || list_size > self.config.max_header_list_size
            {
                return Err(compression_error("decoded header list exceeds configured limit"));
            }
            insert_decoded_header(&mut headers, name, value, indexing, &mut regular_seen)?;
        }
        Ok(headers)
    }

    fn decode_literal(
        &self,
        block: &mut IoBuf,
        prefix_bits: u8,
        indexing: HeaderIndexing,
    ) -> Result<HeaderField, H2Error> {
        let name_index = decode_integer(block, prefix_bits)?;
        let name = if name_index == 0 {
            decode_string(block)?
        } else {
            self.dynamic_table.get(name_index)?.name
        };
        let value = decode_string(block)?;
        Ok(HeaderField { name, value, indexing })
    }
}

pub(crate) struct HpackEncoder {
    config: HpackConfig,
    dynamic_table: DynamicTable,
    pending_table_size_update: Option<usize>,
}

impl HpackEncoder {
    pub fn new(config: HpackConfig) -> Self {
        Self {
            dynamic_table: DynamicTable::new(config.max_dynamic_table_size),
            config,
            pending_table_size_update: None,
        }
    }

    pub fn set_max_dynamic_table_size(&mut self, size: usize) {
        self.dynamic_table.set_max_size(size);
        self.pending_table_size_update = Some(size);
    }

    pub fn encode(&mut self, headers: &HeaderBlock) -> Result<IoBuf, H2Error> {
        let mut builder = IoBufBuilder::new();
        if let Some(size) = self.pending_table_size_update.take() {
            encode_integer(&mut builder, size, 5, 0x20);
        }
        let mut fields = 0;
        let mut list_size = 0;

        macro_rules! pseudo {
            ($name:literal, $value:expr, $exact:expr, $name_index:expr) => {
                if let Some(value) = $value {
                    self.encode_field(
                        &mut builder,
                        $name.as_bytes(),
                        value,
                        HeaderIndexing::WithoutIndexing,
                        $exact,
                        $name_index,
                    );
                    fields += 1;
                    list_size += $name.len() + value.len() + 32;
                }
            };
        }

        pseudo!(":method", headers.pseudo.method.as_deref(), method_index, Some(2));
        pseudo!(":scheme", headers.pseudo.scheme.as_deref(), scheme_index, Some(6));
        pseudo!(":authority", headers.pseudo.authority.as_deref(), no_exact_index, Some(1));
        pseudo!(":path", headers.pseudo.path.as_deref(), path_index, Some(4));
        if let Some(status) = headers.pseudo.status {
            let status = status.to_string();
            self.encode_field(
                &mut builder,
                b":status",
                status.as_bytes(),
                HeaderIndexing::WithoutIndexing,
                status_index,
                Some(8),
            );
            fields += 1;
            list_size += 7 + status.len() + 32;
        }
        pseudo!(":protocol", headers.pseudo.protocol.as_deref(), no_exact_index, None);

        for field in headers.fields() {
            if field.name.is_empty() || field.name.iter().any(|byte| byte.is_ascii_uppercase()) {
                return Err(compression_error("HTTP/2 header name is empty or contains uppercase"));
            }
            self.encode_field(
                &mut builder,
                &field.name,
                &field.value,
                field.indexing,
                no_exact_index,
                static_name_index(&field.name),
            );
            fields += 1;
            list_size += field.name.len() + field.value.len() + 32;
        }

        if fields > self.config.max_header_fields || list_size > self.config.max_header_list_size {
            return Err(compression_error("outbound header list exceeds configured limit"));
        }
        let output = builder.finish();
        if output.len() > self.config.max_header_block_size {
            return Err(compression_error("outbound HPACK block exceeds configured limit"));
        }
        Ok(output)
    }

    fn encode_field(
        &mut self,
        builder: &mut IoBufBuilder,
        name: &[u8],
        value: &[u8],
        indexing: HeaderIndexing,
        exact_index: fn(&[u8]) -> Option<usize>,
        name_index: Option<usize>,
    ) {
        if let Some(index) = exact_index(value) {
            encode_integer(builder, index, 7, 0x80);
            return;
        }
        let (prefix, bits) = match indexing {
            HeaderIndexing::Indexed => (0x40, 6),
            HeaderIndexing::WithoutIndexing => (0, 4),
            HeaderIndexing::NeverIndexed => (0x10, 4),
        };
        encode_integer(builder, name_index.unwrap_or(0), bits, prefix);
        if name_index.is_none() {
            encode_string(builder, name);
        }
        encode_string(builder, value);
        if indexing == HeaderIndexing::Indexed {
            self.dynamic_table.insert(HeaderField {
                name: Bytes::copy_from_slice(name),
                value: Bytes::copy_from_slice(value),
                indexing,
            });
        }
    }
}

struct DynamicTable {
    entries: Vec<HeaderField>,
    size: usize,
    max_size: usize,
}

impl DynamicTable {
    fn new(max_size: usize) -> Self {
        Self { entries: Vec::new(), size: 0, max_size }
    }

    fn insert(&mut self, field: HeaderField) {
        let field_size = field.name.len() + field.value.len() + 32;
        if field_size > self.max_size {
            self.entries.clear();
            self.size = 0;
            return;
        }
        while self.size + field_size > self.max_size {
            let Some(evicted) = self.entries.pop() else { break };
            self.size -= evicted.name.len() + evicted.value.len() + 32;
        }
        self.entries.insert(0, field);
        self.size += field_size;
    }

    fn get(&self, index: usize) -> Result<HeaderField, H2Error> {
        if index == 0 {
            return Err(compression_error("HPACK index zero is invalid"));
        }
        if index <= STATIC_TABLE.len() {
            let (name, value) = STATIC_TABLE[index - 1];
            return Ok(HeaderField {
                name: Bytes::from_static(name),
                value: Bytes::from_static(value),
                indexing: HeaderIndexing::Indexed,
            });
        }
        self.entries
            .get(index - STATIC_TABLE.len() - 1)
            .cloned()
            .ok_or_else(|| compression_error("HPACK dynamic table index is invalid"))
    }

    fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        while self.size > self.max_size {
            let Some(evicted) = self.entries.pop() else { break };
            self.size -= evicted.name.len() + evicted.value.len() + 32;
        }
    }
}

fn decode_integer(block: &mut IoBuf, prefix_bits: u8) -> Result<usize, H2Error> {
    if !block.has_remaining() {
        return Err(compression_error("truncated HPACK integer"));
    }
    let mask = (1_u8 << prefix_bits) - 1;
    let first = block.get_u8();
    let mut value = usize::from(first & mask);
    if value < usize::from(mask) {
        return Ok(value);
    }

    let mut shift = 0;
    loop {
        if !block.has_remaining() || shift >= usize::BITS as usize {
            return Err(compression_error("invalid HPACK integer"));
        }
        let byte = block.get_u8();
        let part = usize::from(byte & 0x7f)
            .checked_shl(shift as u32)
            .ok_or_else(|| compression_error("HPACK integer overflow"))?;
        value =
            value.checked_add(part).ok_or_else(|| compression_error("HPACK integer overflow"))?;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

fn decode_string(block: &mut IoBuf) -> Result<Bytes, H2Error> {
    if !block.has_remaining() {
        return Err(compression_error("truncated HPACK string"));
    }
    let huffman = block.chunk()[0] & 0x80 != 0;
    let length = decode_integer(block, 7)?;
    if length > block.remaining() {
        return Err(compression_error("HPACK string exceeds header block"));
    }
    let mut encoded = vec![0_u8; length];
    block.copy_to_slice(&mut encoded);
    if huffman {
        let decoded = huffman::decode(&encoded)
            .ok_or_else(|| compression_error("invalid HPACK Huffman string"))?;
        Ok(Bytes::from(decoded))
    } else {
        Ok(Bytes::from(encoded))
    }
}

fn insert_decoded_header(
    headers: &mut HeaderBlock,
    name: Bytes,
    value: Bytes,
    indexing: HeaderIndexing,
    regular_seen: &mut bool,
) -> Result<(), H2Error> {
    if name.is_empty() || name.iter().any(|byte| byte.is_ascii_uppercase()) {
        return Err(compression_error("decoded HTTP/2 header name is invalid"));
    }
    if name[0] == b':' {
        if *regular_seen {
            return Err(compression_error("pseudo-header appears after a regular header"));
        }
        match name.as_ref() {
            b":method" => set_once(&mut headers.pseudo.method, value)?,
            b":scheme" => set_once(&mut headers.pseudo.scheme, value)?,
            b":authority" => set_once(&mut headers.pseudo.authority, value)?,
            b":path" => set_once(&mut headers.pseudo.path, value)?,
            b":protocol" => set_once(&mut headers.pseudo.protocol, value)?,
            b":status" => {
                if headers.pseudo.status.is_some()
                    || value.len() != 3
                    || value.iter().any(|byte| !byte.is_ascii_digit())
                {
                    return Err(compression_error("invalid or duplicate :status"));
                }
                headers.pseudo.status = Some(
                    u16::from(value[0] - b'0') * 100
                        + u16::from(value[1] - b'0') * 10
                        + u16::from(value[2] - b'0'),
                );
            }
            _ => return Err(compression_error("unknown HTTP/2 pseudo-header")),
        }
        return Ok(());
    }

    *regular_seen = true;
    match name.as_ref() {
        b"connection" | b"proxy-connection" | b"keep-alive" | b"upgrade" | b"transfer-encoding" => {
            return Err(compression_error("connection-specific HTTP/2 header is forbidden"));
        }
        b"te" if value.as_ref() != b"trailers" => {
            return Err(compression_error("HTTP/2 TE header must equal trailers"));
        }
        _ => {}
    }
    headers.push(HeaderField { name, value, indexing });
    Ok(())
}

fn set_once(slot: &mut Option<Bytes>, value: Bytes) -> Result<(), H2Error> {
    if slot.replace(value).is_some() {
        return Err(compression_error("duplicate HTTP/2 pseudo-header"));
    }
    Ok(())
}

const STATIC_TABLE: [(&[u8], &[u8]); 61] = [
    (b":authority", b""),
    (b":method", b"GET"),
    (b":method", b"POST"),
    (b":path", b"/"),
    (b":path", b"/index.html"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"200"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"304"),
    (b":status", b"400"),
    (b":status", b"404"),
    (b":status", b"500"),
    (b"accept-charset", b""),
    (b"accept-encoding", b"gzip, deflate"),
    (b"accept-language", b""),
    (b"accept-ranges", b""),
    (b"accept", b""),
    (b"access-control-allow-origin", b""),
    (b"age", b""),
    (b"allow", b""),
    (b"authorization", b""),
    (b"cache-control", b""),
    (b"content-disposition", b""),
    (b"content-encoding", b""),
    (b"content-language", b""),
    (b"content-length", b""),
    (b"content-location", b""),
    (b"content-range", b""),
    (b"content-type", b""),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"expect", b""),
    (b"expires", b""),
    (b"from", b""),
    (b"host", b""),
    (b"if-match", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"if-range", b""),
    (b"if-unmodified-since", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"max-forwards", b""),
    (b"proxy-authenticate", b""),
    (b"proxy-authorization", b""),
    (b"range", b""),
    (b"referer", b""),
    (b"refresh", b""),
    (b"retry-after", b""),
    (b"server", b""),
    (b"set-cookie", b""),
    (b"strict-transport-security", b""),
    (b"transfer-encoding", b""),
    (b"user-agent", b""),
    (b"vary", b""),
    (b"via", b""),
    (b"www-authenticate", b""),
];

fn encode_integer(builder: &mut IoBufBuilder, mut value: usize, prefix_bits: u8, prefix: u8) {
    let mask = (1_usize << prefix_bits) - 1;
    if value < mask {
        builder.put_u8(prefix | value as u8);
        return;
    }
    builder.put_u8(prefix | mask as u8);
    value -= mask;
    while value >= 128 {
        builder.put_u8((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    builder.put_u8(value as u8);
}

fn encode_string(builder: &mut IoBufBuilder, value: &[u8]) {
    let encoded_len = huffman::encoded_len(value);
    if encoded_len < value.len() {
        let encoded = huffman::encode(value);
        encode_integer(builder, encoded.len(), 7, 0x80);
        builder.put_slice(&encoded);
    } else {
        encode_integer(builder, value.len(), 7, 0);
        builder.put_slice(value);
    }
}

fn no_exact_index(_: &[u8]) -> Option<usize> {
    None
}

fn method_index(value: &[u8]) -> Option<usize> {
    match value {
        b"GET" => Some(2),
        b"POST" => Some(3),
        _ => None,
    }
}

fn scheme_index(value: &[u8]) -> Option<usize> {
    match value {
        b"http" => Some(6),
        b"https" => Some(7),
        _ => None,
    }
}

fn path_index(value: &[u8]) -> Option<usize> {
    match value {
        b"/" => Some(4),
        b"/index.html" => Some(5),
        _ => None,
    }
}

fn status_index(value: &[u8]) -> Option<usize> {
    match value {
        b"200" => Some(8),
        b"204" => Some(9),
        b"206" => Some(10),
        b"304" => Some(11),
        b"400" => Some(12),
        b"404" => Some(13),
        b"500" => Some(14),
        _ => None,
    }
}

fn static_name_index(name: &[u8]) -> Option<usize> {
    match name {
        b"content-length" => Some(28),
        b"content-type" => Some(31),
        b"grpc-encoding" => None,
        b"grpc-status" => None,
        b"te" => None,
        b"user-agent" => Some(58),
        _ => None,
    }
}

fn compression_error(detail: &'static str) -> H2Error {
    H2Error::Connection(ConnectionError::new(ErrorCode::CompressionError, detail))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_block_is_a_small_pool_handle() {
        assert_eq!(
            std::mem::size_of::<HeaderBlock>(),
            std::mem::size_of::<Resource<HeaderBlockStorage>>()
        );
        assert!(std::mem::size_of::<HeaderBlock>() <= 2 * std::mem::size_of::<usize>());
    }

    fn flatten(buffer: &IoBuf) -> Vec<u8> {
        buffer.chunks().flatten().copied().collect()
    }

    fn input(bytes: &[u8]) -> IoBuf {
        let mut builder = IoBufBuilder::new();
        builder.put_slice(bytes);
        builder.finish()
    }

    #[test]
    fn encoder_uses_static_indices_for_common_request_pseudo_headers() {
        let mut headers = HeaderBlock::new();
        headers.pseudo.method = Some(Bytes::from_static(b"POST"));
        headers.pseudo.scheme = Some(Bytes::from_static(b"https"));
        headers.pseudo.path = Some(Bytes::from_static(b"/"));

        let encoded = HpackEncoder::new(HpackConfig::default()).encode(&headers).unwrap();
        assert_eq!(flatten(&encoded), [0x83, 0x87, 0x84]);
    }

    #[test]
    fn encoder_uses_huffman_when_it_shortens_a_literal() {
        let mut headers = HeaderBlock::new();
        headers.push(HeaderField {
            name: Bytes::from_static(b"grpc-status"),
            value: Bytes::from_static(b"0"),
            indexing: HeaderIndexing::NeverIndexed,
        });

        let encoded = HpackEncoder::new(HpackConfig::default()).encode(&headers).unwrap();
        let bytes = flatten(&encoded);
        assert_eq!(bytes[0], 0x10);
        assert_ne!(bytes[1] & 0x80, 0);

        let decoded = HpackDecoder::new(HpackConfig::default()).decode(input(&bytes)).unwrap();
        let field = decoded.fields().next().unwrap();
        assert_eq!(field.name, "grpc-status");
        assert_eq!(field.value, "0");
    }

    #[test]
    fn encoder_rejects_uppercase_header_names() {
        let mut headers = HeaderBlock::new();
        headers.push(HeaderField {
            name: Bytes::from_static(b"Content-Type"),
            value: Bytes::from_static(b"application/grpc"),
            indexing: HeaderIndexing::WithoutIndexing,
        });
        assert!(HpackEncoder::new(HpackConfig::default()).encode(&headers).is_err());
    }

    #[test]
    fn decoder_round_trips_pseudo_and_regular_headers() {
        let mut expected = HeaderBlock::new();
        expected.pseudo.status = Some(200);
        expected.push(HeaderField {
            name: Bytes::from_static(b"content-type"),
            value: Bytes::from_static(b"application/grpc"),
            indexing: HeaderIndexing::WithoutIndexing,
        });
        let encoded = HpackEncoder::new(HpackConfig::default()).encode(&expected).unwrap();
        let actual = HpackDecoder::new(HpackConfig::default()).decode(encoded).unwrap();
        assert_eq!(actual.pseudo.status, Some(200));
        let field = actual.fields().next().unwrap();
        assert_eq!(field.name, "content-type");
        assert_eq!(field.value, "application/grpc");
    }

    #[test]
    fn decoder_resolves_dynamic_indices_across_header_blocks() {
        let mut decoder = HpackDecoder::new(HpackConfig::default());
        let literal =
            input(&[0x40, 6, b'c', b'u', b's', b't', b'o', b'm', 5, b'v', b'a', b'l', b'u', b'e']);
        decoder.decode(literal).unwrap();
        let indexed = decoder.decode(input(&[0xbe])).unwrap();
        let field = indexed.fields().next().unwrap();
        assert_eq!(field.name, "custom");
        assert_eq!(field.value, "value");
    }

    #[test]
    fn decoder_accepts_rfc_huffman_literal() {
        let block = input(&[
            0x01, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
        ]);
        let decoded = HpackDecoder::new(HpackConfig::default()).decode(block).unwrap();
        assert_eq!(decoded.pseudo.authority.as_deref(), Some(b"www.example.com".as_slice()));
    }

    #[test]
    fn decoder_rejects_invalid_huffman_eos() {
        let error = HpackDecoder::new(HpackConfig::default())
            .decode(input(&[0x01, 0x84, 0xff, 0xff, 0xff, 0xff]))
            .unwrap_err();
        assert!(
            matches!(error, H2Error::Connection(error) if error.code == ErrorCode::CompressionError)
        );
    }

    #[test]
    fn encoder_publishes_peer_table_size_change_in_the_next_block() {
        let mut encoder = HpackEncoder::new(HpackConfig::default());
        encoder.set_max_dynamic_table_size(0);
        let encoded = encoder.encode(&HeaderBlock::new()).unwrap();
        assert_eq!(flatten(&encoded), [0x20]);
        HpackDecoder::new(HpackConfig::default()).decode(encoded).unwrap();
    }
}
