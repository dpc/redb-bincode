use std::path::Path;

use redb::{DatabaseError, TransactionError};

use super::tx::{ReadTransaction, WriteTransaction};
use crate::tx;

#[derive(Debug)]
pub struct Database(redb::Database);

impl Database {
    pub fn create(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Ok(Self(redb::Database::create(path)?))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Ok(Self(redb::Database::open(path)?))
    }

    pub fn begin_read(&self) -> Result<tx::ReadTransaction, TransactionError> {
        Ok(ReadTransaction::from(self.0.begin_read()?))
    }

    pub fn begin_write(&self) -> Result<tx::WriteTransaction, TransactionError> {
        Ok(WriteTransaction::from(self.0.begin_write()?))
    }
}

impl From<redb::Database> for Database {
    fn from(value: redb::Database) -> Self {
        Self(value)
    }
}
