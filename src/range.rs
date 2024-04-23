use std::fmt;
use std::marker::PhantomData;

use redb::StorageError;

use crate::{AccessGuard, SortKey, SortOrder};

pub struct Range<'a, K, V, IK = &'static [u8]>
where
    IK: redb::Value + 'static + redb::Key,
{
    inner: redb::Range<'a, IK, &'static [u8]>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

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
