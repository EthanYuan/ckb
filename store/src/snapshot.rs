use crate::store::ChainStore;
use ckb_db::{
    iter::{DBIterator, DBIteratorItem, Direction},
    Col, DBPinnableSlice, RocksDBSnapshot,
};
use ckb_merkle_mountain_range::{Error as MMRError, MMRStore, Result as MMRResult};
use ckb_types::{packed, prelude::*};

pub struct StoreSnapshot {
    pub inner: RocksDBSnapshot,
}

impl<'a> ChainStore<'a> for StoreSnapshot {
    type Vector = DBPinnableSlice<'a>;

    fn get(&'a self, col: Col, key: &[u8]) -> Option<Self::Vector> {
        self.inner
            .get_pinned(col, key)
            .expect("db operation should be ok")
    }

    fn get_iter<'i>(
        &'i self,
        col: Col,
        from_key: &'i [u8],
        direction: Direction,
    ) -> Box<Iterator<Item = DBIteratorItem> + 'i> {
        self.inner
            .iter(col, from_key, direction)
            .expect("db operation should be ok")
    }
}

impl MMRStore<packed::HeaderDigest> for StoreSnapshot {
    fn get_elem(&self, pos: u64) -> MMRResult<Option<packed::HeaderDigest>> {
        use crate::COLUMN_CHAIN_ROOT_MMR;
        Ok(self
            .get(COLUMN_CHAIN_ROOT_MMR, &pos.to_le_bytes()[..])
            .map(|slice| {
                let reader = packed::HeaderDigestReader::from_slice(&slice.as_ref()).should_be_ok();
                reader.to_entity()
            }))
    }

    /// snapshot MMR is readonly
    fn append(&mut self, _pos: u64, _elems: Vec<packed::HeaderDigest>) -> MMRResult<()> {
        Err(MMRError::StoreError(
            "Failed to append to MMR, snapshot MMR is readonly".into(),
        ))
    }
}
