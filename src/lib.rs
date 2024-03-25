use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;

use redb::ReadableTable;
pub use redb::StorageError;

pub const BINCODE_CONFIG: bincode::config::Configuration<bincode::config::BigEndian> =
    bincode::config::standard()
        .with_big_endian()
        .with_variable_int_encoding();

thread_local! {
    pub static ENCODE_KEY: std::cell::UnsafeCell<Vec<u8>> = const { std::cell::UnsafeCell::new(vec![]) };
    pub static ENCODE_VALUE: std::cell::UnsafeCell<Vec<u8>> = const { std::cell::UnsafeCell::new(vec![]) };
}

unsafe fn with_encode_key_buf<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    // https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html#memory-layout
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut<T>(ptr: &UnsafeCell<T>) -> &mut T {
        unsafe { &mut *ptr.get() }
    }

    ENCODE_KEY.with(|buf| {
        let buf = unsafe { get_mut(buf) };
        let res = f(buf);
        buf.clear();
        res
    })
}
unsafe fn with_encode_value_buf<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    // https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html#memory-layout
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut<T>(ptr: &UnsafeCell<T>) -> &mut T {
        unsafe { &mut *ptr.get() }
    }

    ENCODE_VALUE.with(|buf| {
        let buf = unsafe { get_mut(buf) };
        let res = f(buf);
        buf.clear();
        res
    })
}

mod sort;
pub use sort::*;

mod database;
pub use database::*;

mod tx;
pub use tx::*;

pub struct AccessGuard<'a, V> {
    inner: redb::AccessGuard<'a, &'static [u8]>,
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

impl<'a, V> AccessGuard<'a, V>
where
    V: bincode::Decode,
{
    pub fn value(&self) -> Result<V, bincode::error::DecodeError> {
        bincode::decode_from_slice(self.inner.value(), BINCODE_CONFIG).map(|v| v.0)
    }
}

pub struct ReadOnlyTable<K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
{
    inner: redb::ReadOnlyTable<sort::SortKey<S>, &'static [u8]>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V, S> ReadOnlyTable<K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    pub fn as_raw(&self) -> &redb::ReadOnlyTable<sort::SortKey<S>, &'static [u8]> {
        &self.inner
    }

    pub fn get<Q>(&self, key: &Q) -> Result<Option<AccessGuard<'static, V>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        unsafe {
            Ok(with_encode_key_buf(|buf| {
                let size = bincode::encode_into_std_write(key, buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");
                self.inner.get(&buf[..size])
            })?
            .map(AccessGuard::from))
        }
    }
}

pub struct Table<'txn, K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
{
    inner: redb::Table<'txn, sort::SortKey<S>, &'static [u8]>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<'txn, K, V, S> Table<'txn, K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    pub fn as_raw(&self) -> &redb::Table<sort::SortKey<S>, &'static [u8]> {
        &self.inner
    }
    pub fn as_raw_mut(&mut self) -> &'txn mut redb::Table<'_, sort::SortKey<S>, &'static [u8]> {
        &mut self.inner
    }

    pub fn get<Q>(&self, key: &Q) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        unsafe {
            Ok(with_encode_key_buf(|buf| {
                let size = bincode::encode_into_std_write(key, buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");
                self.inner.get(&buf[..size])
            })?
            .map(AccessGuard::from))
        }
    }

    pub fn insert<KQ, VQ>(
        &mut self,
        key: &KQ,
        value: &VQ,
    ) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<KQ>,
        V: Borrow<VQ>,
        KQ: bincode::Encode + ?Sized,
        VQ: bincode::Encode + ?Sized,
    {
        Ok(unsafe {
            with_encode_key_buf(|key_buf| {
                let key_size = bincode::encode_into_std_write(key, key_buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");

                with_encode_value_buf(|value_buf| {
                    let value_size =
                        bincode::encode_into_std_write(value, value_buf, BINCODE_CONFIG)
                            .expect("encoding can't fail");

                    self.inner
                        .insert(&key_buf[..key_size], &value_buf[..value_size])
                })
            })
        }?
        .map(AccessGuard::from))
    }

    pub fn remove<KQ>(&mut self, key: &KQ) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<KQ>,
        KQ: bincode::Encode + ?Sized,
    {
        Ok(unsafe {
            with_encode_key_buf(|key_buf| {
                let key_size = bincode::encode_into_std_write(key, key_buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");
                self.inner.remove(&key_buf[..key_size])
            })
        }?
        .map(AccessGuard::from))
    }
}
