use crate::error::RPCError;
use async_trait::async_trait;
use ckb_jsonrpc_types::{
    IndexerCell, IndexerCellsCapacity, IndexerOrder, IndexerPagination, IndexerSearchKey,
    IndexerTip, IndexerTx, JsonBytes, Uint32,
};
use ckb_rich_indexer::AsyncRichIndexerHandle;
use jsonrpc_core::Result;
use jsonrpc_utils::rpc;

/// RPC Module Indexer.
#[rpc]
#[async_trait]
pub trait RichIndexerRpc {
    /// Returns the indexed tip
    ///
    /// ## Returns
    ///   * block_hash - indexed tip block hash
    ///   * block_number - indexed tip block number
    ///
    /// ## Examples
    ///
    /// Request
    ///
    /// ```json
    /// {
    ///     "id": 2,
    ///     "jsonrpc": "2.0",
    ///     "method": "get_rich_indexer_tip"
    /// }
    /// ```
    ///
    /// Response
    ///
    /// ```json
    /// {
    ///   "jsonrpc": "2.0",
    ///   "result": {
    ///     "block_hash": "0x4959d6e764a2edc6038dbf03d61ebcc99371115627b186fdcccb2161fbd26edc",
    ///     "block_number": "0x5b513e"
    ///   },
    ///   "id": 2
    /// }
    /// ```
    #[rpc(name = "get_rich_indexer_tip")]
    async fn get_rich_indexer_tip(&self) -> Result<Option<IndexerTip>>;

    #[rpc(name = "get_rich_indexer_cells")]
    async fn get_rich_indexer_cells(
        &self,
        search_key: IndexerSearchKey,
        order: IndexerOrder,
        limit: Uint32,
        after: Option<JsonBytes>,
    ) -> Result<IndexerPagination<IndexerCell>>;

    #[rpc(name = "get_rich_indexer_transactions")]
    async fn get_rich_indexer_transactions(
        &self,
        search_key: IndexerSearchKey,
        order: IndexerOrder,
        limit: Uint32,
        after: Option<JsonBytes>,
    ) -> Result<IndexerPagination<IndexerTx>>;

    #[rpc(name = "get_rich_indexer_cells_capacity")]
    async fn get_rich_indexer_cells_capacity(
        &self,
        search_key: IndexerSearchKey,
    ) -> Result<Option<IndexerCellsCapacity>>;
}

#[derive(Clone)]
pub(crate) struct RichIndexerRpcImpl {
    pub(crate) handle: AsyncRichIndexerHandle,
}

impl RichIndexerRpcImpl {
    pub fn new(handle: AsyncRichIndexerHandle) -> Self {
        RichIndexerRpcImpl { handle }
    }
}

#[async_trait]
impl RichIndexerRpc for RichIndexerRpcImpl {
    async fn get_rich_indexer_tip(&self) -> Result<Option<IndexerTip>> {
        self.handle
            .get_indexer_tip()
            .await
            .map_err(|e| RPCError::custom(RPCError::Indexer, e))
    }

    async fn get_rich_indexer_cells(
        &self,
        search_key: IndexerSearchKey,
        order: IndexerOrder,
        limit: Uint32,
        after: Option<JsonBytes>,
    ) -> Result<IndexerPagination<IndexerCell>> {
        self.handle
            .get_cells(search_key, order, limit, after)
            .await
            .map_err(|e| RPCError::custom(RPCError::Indexer, e))
    }

    async fn get_rich_indexer_transactions(
        &self,
        search_key: IndexerSearchKey,
        order: IndexerOrder,
        limit: Uint32,
        after: Option<JsonBytes>,
    ) -> Result<IndexerPagination<IndexerTx>> {
        self.handle
            .get_transactions(search_key, order, limit, after)
            .await
            .map_err(|e| RPCError::custom(RPCError::Indexer, e))
    }

    async fn get_rich_indexer_cells_capacity(
        &self,
        search_key: IndexerSearchKey,
    ) -> Result<Option<IndexerCellsCapacity>> {
        self.handle
            .get_cells_capacity(search_key)
            .await
            .map_err(|e| RPCError::custom(RPCError::Indexer, e))
    }
}
