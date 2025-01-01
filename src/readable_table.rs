use std::borrow::Borrow;
use std::{fmt, ops};

use redb::StorageError;

type Result<T = (), E = StorageError> = std::result::Result<T, E>;

use crate::{AccessGuard, Lexicographical, Range, ReadOnlyTable, SortKey, SortOrder, Table};

pub trait ReadableTable<K, V, S = Lexicographical>
where
    S: SortOrder + fmt::Debug + 'static,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    #[allow(clippy::type_complexity)]
    fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError>;

    #[allow(clippy::type_complexity)]
    fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError>;

    fn range<'a, Q>(
        &self,
        range: impl ops::RangeBounds<Q> + 'a,
    ) -> Result<Range<'_, K, V, SortKey<S>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized;

    fn get<Q>(&self, key: &Q) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized;
}

impl<K, V, S> ReadableTable<K, V, S> for ReadOnlyTable<K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    #[allow(clippy::type_complexity)]
    fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        self.first()
    }

    #[allow(clippy::type_complexity)]
    fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        self.last()
    }

    fn range<'a, Q>(
        &self,
        range: impl ops::RangeBounds<Q> + 'a,
    ) -> Result<Range<'_, K, V, SortKey<S>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        self.range(range)
    }

    fn get<Q>(&self, key: &Q) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        self.get(key)
    }
}

impl<'txn, K, V, S> ReadableTable<K, V, S> for Table<'txn, K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    #[allow(clippy::type_complexity)]
    fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        self.first()
    }

    #[allow(clippy::type_complexity)]
    fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K, SortKey<S>>, AccessGuard<'_, V>)>, StorageError> {
        self.last()
    }

    fn range<'a, Q>(
        &self,
        range: impl ops::RangeBounds<Q> + 'a,
    ) -> Result<Range<'_, K, V, SortKey<S>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        self.range(range)
    }

    fn get<Q>(&self, key: &Q) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        self.get(key)
    }
}
