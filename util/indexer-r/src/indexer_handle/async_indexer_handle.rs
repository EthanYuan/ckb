use crate::store::SQLXPool;

use ckb_indexer_sync::{Error, Pool};
use ckb_jsonrpc_types::IndexerTip;
use ckb_types::H256;
use sqlx::Row;

use std::sync::{Arc, RwLock};

/// Async handle to the indexer-r.
pub struct AsyncIndexerRHandle {
    store: SQLXPool,
    _pool: Option<Arc<RwLock<Pool>>>,
}

impl AsyncIndexerRHandle {
    /// Construct new AsyncIndexerRHandle instance
    pub fn new(store: SQLXPool, pool: Option<Arc<RwLock<Pool>>>) -> Self {
        Self { store, _pool: pool }
    }
}

impl AsyncIndexerRHandle {
    /// Get indexer current tip
    pub async fn query_indexer_tip(&self) -> Result<Option<IndexerTip>, Error> {
        let query = SQLXPool::new_query(
            r#"
            SELECT block_hash, block_number FROM block
            ORDER BY block_number
            DESC LIMIT 1
            "#,
        );
        self.store
            .fetch_optional(query)
            .await
            .map(|res| {
                res.map(|row| IndexerTip {
                    block_number: (row.get::<i64, _>("block_number") as u64).into(),
                    block_hash: bytes_to_h256(row.get("block_hash")),
                })
            })
            .map_err(|err| Error::DB(err.to_string()))
    }
}

pub(crate) fn bytes_to_h256(input: &[u8]) -> H256 {
    H256::from_slice(&input[0..32]).expect("bytes to h256")
}

pub(crate) fn sqlx_param_placeholders(range: std::ops::Range<usize>) -> Result<Vec<String>, Error> {
    if range.start == 0 {
        return Err(Error::Params("no valid parameter".to_owned()));
    }
    Ok((1..=range.end)
        .map(|i| format!("${}", i))
        .collect::<Vec<String>>())
}
