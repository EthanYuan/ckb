use super::{Batch, Error, IteratorDirection, IteratorItem, Store};

use rocksdb::{prelude::*, Direction, IteratorMode, WriteBatch, DB};

use std::sync::Arc;

#[derive(Clone)]
pub struct RocksdbStore {
    db: Arc<DB>,
}

impl Store for RocksdbStore {
    type Batch = RocksdbBatch;

    fn new(path: &str) -> Self {
        let db = Arc::new(DB::open_default(path).expect("Failed to open rocksdb"));
        Self { db }
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        self.db
            .get(key.as_ref())
            .map(|v| v.map(|vi| vi.to_vec()))
            .map_err(Into::into)
    }

    fn exists<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, Error> {
        self.db
            .get(key.as_ref())
            .map(|v| v.is_some())
            .map_err(Into::into)
    }

    fn iter<K: AsRef<[u8]>>(
        &self,
        from_key: K,
        mode: IteratorDirection,
    ) -> Result<Box<dyn Iterator<Item = IteratorItem> + '_>, Error> {
        let mode = IteratorMode::From(
            from_key.as_ref(),
            match mode {
                IteratorDirection::Forward => Direction::Forward,
                IteratorDirection::Reverse => Direction::Reverse,
            },
        );
        Ok(Box::new(self.db.iterator(mode)) as Box<_>)
    }

    fn batch(&self) -> Result<Self::Batch, Error> {
        Ok(Self::Batch {
            db: Arc::clone(&self.db),
            wb: WriteBatch::default(),
        })
    }
}

pub struct RocksdbBatch {
    db: Arc<DB>,
    wb: WriteBatch,
}

impl Batch for RocksdbBatch {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<(), Error> {
        self.wb.put(key, value)?;
        Ok(())
    }

    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), Error> {
        self.wb.delete(key.as_ref())?;
        Ok(())
    }

    fn commit(self) -> Result<(), Error> {
        self.db.write(&self.wb)?;
        Ok(())
    }
}

impl From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Error {
        Error::DbError(e.to_string())
    }
}

impl RocksdbStore {
    pub fn inner(&self) -> &DB {
        &self.db
    }
}
