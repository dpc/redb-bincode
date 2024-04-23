#![doc = include_str!("../README.md")]

use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::{fmt, ops};

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

mod access_guard;
pub use access_guard::*;

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

    #[allow(clippy::type_complexity)]
    pub fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .first()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }
    pub fn first_eager(&self) -> Result<Option<(K, V)>, AccessError> {
        Ok(if let Some((k, v)) = self.first()? {
            let k = k.value()?;
            let v = v.value()?;
            Some((k, v))
        } else {
            None
        })
    }

    pub fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .last()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }

    pub fn last_eager(&self) -> Result<Option<(K, V)>, AccessError> {
        Ok(if let Some((k, v)) = self.last()? {
            let k = k.value()?;
            let v = v.value()?;
            Some((k, v))
        } else {
            None
        })
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
    pub fn get_eager<Q>(&self, key: &Q) -> Result<Option<V>, AccessError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        let guard = self.get(key)?;
        let v = guard.map(|v| v.value()).transpose()?;

        Ok(v)
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

    pub fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .first()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }

    pub fn first_eager(&self) -> Result<Option<(K, V)>, AccessError> {
        Ok(if let Some((k, v)) = self.first()? {
            let k = k.value()?;
            let v = v.value()?;
            Some((k, v))
        } else {
            None
        })
    }

    pub fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        Ok(self
            .inner
            .last()?
            .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))))
    }

    pub fn last_eager(&self) -> Result<Option<(K, V)>, AccessError> {
        Ok(if let Some((k, v)) = self.last()? {
            let k = k.value()?;
            let v = v.value()?;
            Some((k, v))
        } else {
            None
        })
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

    pub fn get_eager<Q>(&self, key: &Q) -> Result<Option<V>, AccessError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        let guard = self.get(key)?;
        let v = guard.map(|v| v.value()).transpose()?;

        Ok(v)
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

pub struct Range<'a, K, V, IK = &'static [u8]>
where
    IK: redb::Value + 'static + redb::Key,
{
    inner: redb::Range<'a, IK, &'static [u8]>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

// impl<'a, K, V> From<redb::Range<'a, &'static [u8], &'static [u8]>>
//     for Range<'a, K, V, &'static [u8]>
// {
//     fn from(inner: redb::Range<'a, &'static [u8], &'static [u8]>) -> Self {
//         Self {
//             inner,
//             _k: PhantomData,
//             _v: PhantomData,
//         }
//     }
// }

impl<'a, S, K, V> From<redb::Range<'a, SortKey<S>, &'static [u8]>> for Range<'a, K, V, SortKey<S>>
where
    S: SortOrder + fmt::Debug,
{
    fn from(inner: redb::Range<'a, SortKey<S>, &'static [u8]>) -> Self {
        Self {
            inner,
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

impl<'a, K, V> Iterator for Range<'a, K, V, &'static [u8]> {
    type Item = Result<(AccessGuard<'a, K>, AccessGuard<'a, V>), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(
            self.inner
                .next()?
                .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))),
        )
    }
}

impl<'a, S, K, V> Iterator for Range<'a, K, V, SortKey<S>>
where
    S: SortOrder + fmt::Debug,
{
    type Item = Result<(AccessGuard<'a, K, SortKey<S>>, AccessGuard<'a, V>), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(
            self.inner
                .next()?
                .map(|(k, v)| (AccessGuard::from(k), AccessGuard::from(v))),
        )
    }
}
