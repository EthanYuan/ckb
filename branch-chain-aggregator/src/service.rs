use crate::indexer::{CustomFilters, Indexer};
use crate::pool::Pool;
use crate::store::{RocksdbStore, Store};

use ckb_jsonrpc_types::{
    BlockNumber, Capacity, CellOutput, HeaderView, JsonBytes, LocalNode, OutPoint, Uint32,
};
use ckb_stop_handler::has_received_stop_signal;
use ckb_types::{core, prelude::*, H256};
use jsonrpc_core_client::RpcError;
use jsonrpc_derive::rpc;
use log::{error, info, trace};
use serde::{Deserialize, Serialize};
use version_compare::Version;

use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

/// Have to use RocksdbStore instead of generic `Store` type here,
/// because some rpc need rocksdb snapshot funtion which has lifetime mark and is hard to wrap in a trait
pub struct Service {
    store: RocksdbStore,
    pool: Option<Arc<RwLock<Pool>>>,
    poll_interval: Duration,
    _listen_address: String,
}

impl Service {
    pub fn new(
        store_path: &str,
        pool: Option<Arc<RwLock<Pool>>>,
        listen_address: &str,
        poll_interval: Duration,
    ) -> Self {
        let store = RocksdbStore::new(store_path);
        Self {
            store,
            pool,
            _listen_address: listen_address.to_string(),
            poll_interval,
        }
    }

    pub async fn poll(
        &self,
        rpc_client: gen_client::Client,
        block_filter_str: Option<&str>,
        cell_filter_str: Option<&str>,
    ) {
        let incompatible_version = Version::from("0.99.99").expect("checked version str");
        // assume that long fork will not happen >= 100 blocks.
        let keep_num = 100;
        let indexer = Indexer::new(
            self.store.clone(),
            keep_num,
            1000,
            self.pool.clone(),
            CustomFilters::new(block_filter_str, cell_filter_str),
        );

        loop {
            if has_received_stop_signal() {
                info!("try_loop_sync received exit signal, exit now");
                break;
            }
            
            match rpc_client.local_node_info().await {
                Ok(local_node_info) => {
                    let ckb_version =
                        Version::from(&local_node_info.version).expect("checked version str");
                    if ckb_version > incompatible_version {
                        break;
                    } else {
                        error!("only ckb version 0.100.0 and above are supported");
                    }
                }
                Err(err) => {
                    // < 0.32.0 compatibility, no `version` field in `local_node_info` rpc.
                    if format!("#{}", err).contains("missing field") {
                        error!("only ckb version 0.100.0 and above are supported");
                    } else {
                        error!("cannot get local_node_info from ckb node: {}", err);
                    }
                }
            }
            thread::sleep(self.poll_interval);
        }

        let mut last_updated_time = Instant::now();
        let mut last_warning_time = Instant::now();
        loop {
            if has_received_stop_signal() {
                info!("try_loop_sync received exit signal, exit now");
                break;
            }

            if let Some((tip_number, tip_hash)) = indexer.tip().expect("get tip should be OK") {
                match get_block_by_number(&rpc_client, tip_number + 1).await {
                    Ok(Some(block)) => {
                        last_updated_time = Instant::now();
                        if block.parent_hash() == tip_hash {
                            info!("append {}, {}", block.number(), block.hash());
                            indexer.append(&block).expect("append block should be OK");
                        } else {
                            // Long fork detection
                            let longest_fork_number = tip_number.saturating_sub(keep_num);
                            match get_block_by_number(&rpc_client, longest_fork_number).await {
                                Ok(Some(block)) => {
                                    if let Some(stored_block_hash) = indexer
                                        .get_block_hash(longest_fork_number)
                                        .expect("get block hash should be OK")
                                    {
                                        if block.hash() != stored_block_hash {
                                            error!("long fork detected, ckb-indexer stored block {} => {:#x}, ckb node returns block {} => {:#x}, please check if ckb-indexer is connected to the same network ckb node.", longest_fork_number, stored_block_hash, longest_fork_number, block.hash());
                                            thread::sleep(self.poll_interval);
                                            continue;
                                        }
                                    }
                                    info!("rollback {}, {}", tip_number, tip_hash);
                                    indexer.rollback().expect("rollback block should be OK");
                                }
                                Ok(None) => {
                                    error!("long fork detected, ckb-indexer stored block {}, ckb node returns none, please check if ckb-indexer is connected to the same network ckb node.", longest_fork_number);
                                    thread::sleep(self.poll_interval);
                                }
                                Err(err) => {
                                    error!("cannot get block from ckb node, error: {}", err);
                                    thread::sleep(self.poll_interval);
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        if last_updated_time.elapsed() > Duration::from_secs(60)
                            && last_warning_time.elapsed() > Duration::from_secs(60)
                        {
                            if let Ok(ckb_tip_header) = get_tip_header(&rpc_client).await {
                                error!(
                                    "it has been {}s since the last update, ckb.tip_number = {}, ckb.tip_hash = {:#x}, indexer.tip_number = {}, indexer.tip_hash = {:#x}",
                                    last_updated_time.elapsed().as_secs(),
                                    ckb_tip_header.number(),
                                    ckb_tip_header.hash(),
                                    tip_number,
                                    tip_hash,
                                );
                                last_warning_time = Instant::now();
                            }
                        }
                        trace!("no new block");
                        thread::sleep(self.poll_interval);
                    }
                    Err(err) => {
                        error!("cannot get block from ckb node, error: {}", err);
                        thread::sleep(self.poll_interval);
                    }
                }
            } else {
                match get_block_by_number(&rpc_client, 0).await {
                    Ok(Some(block)) => indexer.append(&block).expect("append block should be OK"),
                    Ok(None) => {
                        error!("ckb node returns an empty genesis block");
                        thread::sleep(self.poll_interval);
                    }
                    Err(err) => {
                        error!("cannot get genesis block from ckb node, error: {}", err);
                        thread::sleep(self.poll_interval);
                    }
                }
            }
        }
    }
}

pub async fn get_block_by_number(
    rpc_client: &gen_client::Client,
    block_number: u64,
) -> std::result::Result<Option<core::BlockView>, RpcError> {
    rpc_client
        .get_block_by_number_with_verbosity(block_number.into(), 0.into())
        .await
        .map(|opt| {
            opt.map(|json_bytes| {
                ckb_types::packed::Block::new_unchecked(json_bytes.into_bytes()).into_view()
            })
        })
}

pub async fn get_tip_header(
    rpc_client: &gen_client::Client,
) -> std::result::Result<core::HeaderView, RpcError> {
    rpc_client
        .get_tip_header()
        .await
        .map(|json_header: HeaderView| json_header.into())
}

#[rpc(client)]
pub trait CkbRpc {
    #[rpc(name = "get_block_by_number")]
    fn get_block_by_number_with_verbosity(
        &self,
        number: BlockNumber,
        verbosity: Uint32,
    ) -> Result<Option<JsonBytes>>;

    #[rpc(name = "local_node_info")]
    fn local_node_info(&self) -> Result<LocalNode>;

    #[rpc(name = "get_tip_header")]
    fn get_tip_header(&self) -> Result<HeaderView>;
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptType {
    Lock,
    Type,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Order {
    Desc,
    Asc,
}

#[derive(Serialize)]
pub struct Tip {
    block_hash: H256,
    block_number: BlockNumber,
}

#[derive(Serialize)]
pub struct CellsCapacity {
    capacity: Capacity,
    block_hash: H256,
    block_number: BlockNumber,
}

#[derive(Serialize)]
pub struct IndexerInfo {
    version: String,
}

#[derive(Serialize)]
pub struct Cell {
    output: CellOutput,
    output_data: Option<JsonBytes>,
    out_point: OutPoint,
    block_number: BlockNumber,
    tx_index: Uint32,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum Tx {
    Ungrouped(TxWithCell),
    Grouped(TxWithCells),
}

impl Tx {
    pub fn tx_hash(&self) -> H256 {
        match self {
            Tx::Ungrouped(tx) => tx.tx_hash.clone(),
            Tx::Grouped(tx) => tx.tx_hash.clone(),
        }
    }
}

#[derive(Serialize)]
pub struct TxWithCell {
    tx_hash: H256,
    block_number: BlockNumber,
    tx_index: Uint32,
    io_index: Uint32,
    io_type: CellType,
}

#[derive(Serialize)]
pub struct TxWithCells {
    tx_hash: H256,
    block_number: BlockNumber,
    tx_index: Uint32,
    cells: Vec<(CellType, Uint32)>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum CellType {
    Input,
    Output,
}
