#![doc = include_str!("../README.md")]

mod access_guard;
mod database;
mod range;
mod readable_table;
mod sort;
mod tx;

use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::{fmt, ops};

pub use access_guard::*;
pub use database::*;
pub use range::*;
pub use readable_table::*;
use redb::ReadableTable as _;
pub use redb::StorageError;
pub use sort::*;
pub use tx::*;

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

pub struct ReadOnlyTable<K, V, S = Lexicographical>
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

    #[allow(clippy::type_complexity)]
    pub fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .first()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }

    #[allow(clippy::type_complexity)]
    pub fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .last()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }

    pub fn range<'a, Q>(
        &self,
        range: impl ops::RangeBounds<Q> + 'a,
    ) -> Result<Range<'_, K, V, SortKey<S>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        let redb_range = unsafe {
            with_encode_key_buf(|start_bound_buf| {
                let start_bound_size = range.start_bound().map(|bound| {
                    bincode::encode_into_std_write(bound, start_bound_buf, BINCODE_CONFIG)
                        .expect("encoding can't fail")
                });

                with_encode_value_buf(|end_bound_buf| {
                    let end_bound_size = range.end_bound().map(|bound| {
                        bincode::encode_into_std_write(bound, end_bound_buf, BINCODE_CONFIG)
                            .expect("encoding can't fail")
                    });

                    let start_bound =
                        start_bound_size.map(|size| SortKey(&start_bound_buf[..size]));
                    let end_bound = end_bound_size.map(|size| SortKey(&end_bound_buf[..size]));
                    self.inner.range((start_bound, end_bound))
                })
            })?
        };
        Ok(Range::from(redb_range))
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
}

pub struct Table<'txn, K, V, S = Lexicographical>
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

    #[allow(clippy::type_complexity)]
    pub fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .first()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }

    #[allow(clippy::type_complexity)]
    pub fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .last()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }

    pub fn range<'a, Q>(
        &self,
        range: impl ops::RangeBounds<Q> + 'a,
    ) -> Result<Range<'_, K, V, SortKey<S>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        let redb_range = unsafe {
            with_encode_key_buf(|start_bound_buf| {
                let start_bound_size = range.start_bound().map(|bound| {
                    bincode::encode_into_std_write(bound, start_bound_buf, BINCODE_CONFIG)
                        .expect("encoding can't fail")
                });

                with_encode_value_buf(|end_bound_buf| {
                    let end_bound_size = range.end_bound().map(|bound| {
                        bincode::encode_into_std_write(bound, end_bound_buf, BINCODE_CONFIG)
                            .expect("encoding can't fail")
                    });

                    let start_bound =
                        start_bound_size.map(|size| SortKey(&start_bound_buf[..size]));
                    let end_bound = end_bound_size.map(|size| SortKey(&end_bound_buf[..size]));
                    self.inner.range((start_bound, end_bound))
                })
            })?
        };
        Ok(Range::from(redb_range))
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

    pub fn retain<F>(&mut self, mut predicate: F) -> Result<(), StorageError>
    where
        F: for<'f> FnMut(&'f K, &'f V) -> bool,
    {
        self.inner.retain(|raw_key, raw_val| {
            let k = bincode::decode_from_slice(raw_key, BINCODE_CONFIG)
                .map(|k| k.0)
                .expect("Invalid encoding");
            let v = bincode::decode_from_slice(raw_val, BINCODE_CONFIG)
                .map(|v| v.0)
                .expect("Invalid encoding");
            predicate(&k, &v)
        })
    }

    pub fn retain_in<'a, Q, F>(
        &mut self,
        range: impl ops::RangeBounds<Q> + 'a,
        mut predicate: F,
    ) -> Result<(), StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
        F: for<'f> FnMut(&'f K, &'f V) -> bool,
    {
        unsafe {
            with_encode_key_buf(|start_bound_buf| {
                let start_bound_size = range.start_bound().map(|bound| {
                    bincode::encode_into_std_write(bound, start_bound_buf, BINCODE_CONFIG)
                        .expect("encoding can't fail")
                });

                with_encode_value_buf(|end_bound_buf| {
                    let end_bound_size = range.end_bound().map(|bound| {
                        bincode::encode_into_std_write(bound, end_bound_buf, BINCODE_CONFIG)
                            .expect("encoding can't fail")
                    });

                    let start_bound =
                        start_bound_size.map(|size| SortKey(&start_bound_buf[..size]));
                    let end_bound = end_bound_size.map(|size| SortKey(&end_bound_buf[..size]));
                    self.inner
                        .retain_in((start_bound, end_bound), |raw_key, raw_val| {
                            let k = bincode::decode_from_slice(raw_key, BINCODE_CONFIG)
                                .map(|k| k.0)
                                .expect("Invalid encoding");
                            let v = bincode::decode_from_slice(raw_val, BINCODE_CONFIG)
                                .map(|v| v.0)
                                .expect("Invalid encoding");
                            predicate(&k, &v)
                        })?;

                    Ok(())
                })
            })
        }
    }
}
