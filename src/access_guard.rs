use std::fmt;
use std::marker::PhantomData;

use bincode::error::DecodeError;
use redb::StorageError;
use thiserror::Error;

use crate::{SortKey, SortOrder, BINCODE_CONFIG};
#[derive(Error, Debug)]
pub enum AccessError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Decoding error: {0}")]
    Decode(#[from] DecodeError),
}

pub struct AccessGuard<'a, V, IV = &'static [u8]>
where
    IV: redb::Value + 'static,
{
    inner: redb::AccessGuard<'a, IV>,
    _v: PhantomData<V>,
}

impl<'a, V> From<redb::AccessGuard<'a, &'static [u8]>> for AccessGuard<'a, V> {
    fn from(inner: redb::AccessGuard<'a, &'static [u8]>) -> Self {
        Self {
            inner,
            _v: PhantomData,
        }
    }
}

impl<'a, S, V> From<redb::AccessGuard<'a, SortKey<S>>> for AccessGuard<'a, V, SortKey<S>>
where
    S: SortOrder + fmt::Debug,
{
    fn from(inner: redb::AccessGuard<'a, SortKey<S>>) -> Self {
        Self {
            inner,
            _v: PhantomData,
        }
    }
}

impl<'a, V> AccessGuard<'a, V>
where
    V: bincode::Decode,
{
    pub fn value(&self) -> Result<V, bincode::error::DecodeError> {
        bincode::decode_from_slice(self.inner.value(), BINCODE_CONFIG).map(|v| v.0)
    }
}

impl<'a, V, S> AccessGuard<'a, V, SortKey<S>>
where
    V: bincode::Decode,
    S: SortOrder + fmt::Debug,
{
    pub fn value(&self) -> Result<V, bincode::error::DecodeError> {
        bincode::decode_from_slice(self.inner.value(), BINCODE_CONFIG).map(|v| v.0)
    }
}
