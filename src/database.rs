use redb::TransactionError;

use super::tx::{ReadTransaction, WriteTransaction};
use crate::tx;

pub struct Database(redb::Database);

impl Database {
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
