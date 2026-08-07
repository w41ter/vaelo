use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;

/// A validated, canonical metadata key.
///
/// Keys are normalized to lowercase ASCII when constructed. Protocol codecs
/// may impose stricter rules, but must never silently drop an entry which
/// cannot be represented.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MetadataKey(Cow<'static, str>);

impl MetadataKey {
    /// Creates a key without allocation.
    ///
    /// # Panics
    ///
    /// Panics when `key` is empty or is not lowercase metadata ASCII.
    pub const fn from_static(key: &'static str) -> Self {
        let bytes = key.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            assert!(
                matches!(bytes[index], b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.'),
                "invalid static metadata key"
            );
            index += 1;
        }
        assert!(!bytes.is_empty(), "metadata key must not be empty");
        Self(Cow::Borrowed(key))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for MetadataKey {
    type Error = InvalidMetadataKey;

    fn try_from(key: &str) -> Result<Self, Self::Error> {
        validate_key(key)?;
        Ok(Self(Cow::Owned(key.to_ascii_lowercase())))
    }
}

impl TryFrom<String> for MetadataKey {
    type Error = InvalidMetadataKey;

    fn try_from(key: String) -> Result<Self, Self::Error> {
        validate_key(&key)?;
        if key.bytes().all(|byte| !byte.is_ascii_uppercase()) {
            Ok(Self(Cow::Owned(key)))
        } else {
            Ok(Self(Cow::Owned(key.to_ascii_lowercase())))
        }
    }
}

fn validate_key(key: &str) -> Result<(), InvalidMetadataKey> {
    if key.is_empty()
        || !key.bytes().all(
            |byte| matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.'),
        )
    {
        return Err(InvalidMetadataKey);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidMetadataKey;

impl fmt::Display for InvalidMetadataKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("metadata key must be non-empty ASCII [a-zA-Z0-9-_.]")
    }
}

impl std::error::Error for InvalidMetadataKey {}

impl From<Infallible> for InvalidMetadataKey {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

/// Owned metadata bytes.
///
/// The representation is intentionally private. Callers can transfer an
/// owned `Vec<u8>` or use static bytes without copying; protocol decoders may
/// add shared receive-buffer representations later without changing this API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataValue(MetadataValueRepr);

#[derive(Clone, Debug, Eq, PartialEq)]
enum MetadataValueRepr {
    Static(&'static [u8]),
    Owned(Vec<u8>),
}

impl MetadataValue {
    pub const fn from_static(value: &'static [u8]) -> Self {
        Self(MetadataValueRepr::Static(value))
    }

    pub fn from_vec(value: Vec<u8>) -> Self {
        Self(MetadataValueRepr::Owned(value))
    }

    /// Explicitly copies borrowed bytes into owned metadata storage.
    pub fn copy_from_slice(value: &[u8]) -> Self {
        Self::from_vec(value.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            MetadataValueRepr::Static(value) => value,
            MetadataValueRepr::Owned(value) => value,
        }
    }

    pub fn into_vec(self) -> Vec<u8> {
        match self.0 {
            MetadataValueRepr::Static(value) => value.to_vec(),
            MetadataValueRepr::Owned(value) => value,
        }
    }
}

impl From<Vec<u8>> for MetadataValue {
    fn from(value: Vec<u8>) -> Self {
        Self::from_vec(value)
    }
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::from_vec(value.into_bytes())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataEntry {
    key: MetadataKey,
    value: MetadataValue,
}

impl MetadataEntry {
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn value(&self) -> &[u8] {
        self.value.as_bytes()
    }
}

/// Ordered RPC metadata which preserves duplicate keys.
///
/// The empty and small-entry representations are deliberately unspecified so
/// they can be tuned without changing callers. `insert` replaces all values
/// for a key; `append` preserves existing values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataMap {
    entries: Vec<MetadataEntry>,
}

impl MetadataMap {
    pub const fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn insert<K, V>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<Option<MetadataValue>, InvalidMetadataKey>
    where
        K: TryInto<MetadataKey>,
        K::Error: Into<InvalidMetadataKey>,
        V: Into<MetadataValue>,
    {
        let key = key.try_into().map_err(Into::into)?;
        let mut replaced = None;
        self.entries.retain_mut(|entry| {
            if entry.key == key {
                if replaced.is_none() {
                    replaced =
                        Some(std::mem::replace(&mut entry.value, MetadataValue::from_static(b"")));
                }
                false
            } else {
                true
            }
        });
        self.entries.push(MetadataEntry { key, value: value.into() });
        Ok(replaced)
    }

    pub fn append<K, V>(&mut self, key: K, value: V) -> Result<(), InvalidMetadataKey>
    where
        K: TryInto<MetadataKey>,
        K::Error: Into<InvalidMetadataKey>,
        V: Into<MetadataValue>,
    {
        let key = key.try_into().map_err(Into::into)?;
        self.entries.push(MetadataEntry { key, value: value.into() });
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|entry| entry.key().eq_ignore_ascii_case(key))
            .map(MetadataEntry::value)
    }

    pub fn get_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.entries
            .iter()
            .filter(move |entry| entry.key().eq_ignore_ascii_case(key))
            .map(MetadataEntry::value)
    }

    pub fn remove(&mut self, key: &str) -> bool {
        let old_len = self.entries.len();
        self.entries.retain(|entry| !entry.key().eq_ignore_ascii_case(key));
        self.entries.len() != old_len
    }

    pub fn iter(&self) -> impl Iterator<Item = &MetadataEntry> {
        self.entries.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
