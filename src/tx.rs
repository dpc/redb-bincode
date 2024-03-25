use std::marker::PhantomData;

use redb::TableError;

use super::{ReadOnlyTable, Table};
use crate::sort;

pub struct ReadTransaction(redb::ReadTransaction);

impl From<redb::ReadTransaction> for ReadTransaction {
    fn from(value: redb::ReadTransaction) -> Self {
        Self(value)
    }
}

impl ReadTransaction {
    pub fn as_raw(&self) -> &redb::ReadTransaction {
        &self.0
    }
    pub fn open_table<K, V>(
        &self,
        name: &str,
    ) -> Result<ReadOnlyTable<K, V, sort::Lexicographical>, TableError>
    where
        K: bincode::Encode + bincode::Decode,
        V: bincode::Encode + bincode::Decode,
    {
        Ok(ReadOnlyTable {
            inner: self.0.open_table(redb::TableDefinition::new(name))?,
            _k: PhantomData,
            _v: PhantomData,
        })
    }
}

pub struct WriteTransaction(redb::WriteTransaction);

impl From<redb::WriteTransaction> for WriteTransaction {
    fn from(value: redb::WriteTransaction) -> Self {
        Self(value)
    }
}

impl WriteTransaction {
    pub fn as_raw(&self) -> &redb::WriteTransaction {
        &self.0
    }
    pub fn open_table<K, V>(
        &self,
        name: &str,
    ) -> Result<Table<K, V, sort::Lexicographical>, TableError>
    where
        K: bincode::Encode + bincode::Decode,
        V: bincode::Encode + bincode::Decode,
    {
        Ok(Table {
            inner: self.0.open_table(redb::TableDefinition::new(name))?,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    pub fn commit(self) -> Result<(), redb::CommitError> {
        self.0.commit()
    }
}
