//! Branch Chain Aggregator

pub(crate) mod error;
pub(crate) mod schemas;
pub(crate) mod transaction;
pub(crate) mod utils;

use crate::error::Error;
use crate::schemas::leap::{MessageUnion, RequestLockArgs, Transfer};
use crate::utils::QUEUE_TYPE;
use crate::utils::{
    decode_udt_amount, encode_udt_amount, get_sighash_script_from_privkey, REQUEST_LOCK, SECP256K1,
    XUDT,
};

use ckb_app_config::{AggregatorConfig, AssetConfig, LockConfig, ScriptConfig};
use ckb_logger::{info, warn};
use ckb_sdk::traits::LiveCell;
use ckb_sdk::{
    rpc::ckb_indexer::{Cell, Order},
    rpc::CkbRpcClient as RpcClient,
    traits::{CellQueryOptions, MaturityOption, PrimaryScriptType, QueryOrder},
};
use ckb_stop_handler::{
    new_crossbeam_exit_rx, new_tokio_exit_rx, register_thread, CancellationToken,
};
use ckb_types::H256;
use ckb_types::{
    bytes::Bytes,
    core::FeeRate,
    packed::{CellDep, Script},
    prelude::*,
};
use molecule::prelude::Byte;

use std::collections::HashMap;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

const THREAD_NAME: &str = "Aggregator";
const CKB_FEE_RATE_LIMIT: u64 = 5000;

///
#[derive(Clone)]
pub struct Aggregator {
    chain_id: String,
    config: AggregatorConfig,
    poll_interval: Duration,
    rpc_client: RpcClient,
    rgbpp_scripts: HashMap<String, ScriptInfo>,
    branch_scripts: HashMap<String, ScriptInfo>,
    rgbpp_assets: HashMap<H256, AssetInfo>,
    rgbpp_locks: HashMap<H256, Script>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScriptInfo {
    pub script: Script,
    pub cell_dep: CellDep,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AssetInfo {
    pub script: Script,
    pub is_capacity: bool,
    pub script_name: String,
}

#[allow(dead_code)]
pub(crate) enum RequestType {
    CkbToBranch = 1,
    BranchToCkb = 2,
    BranchToBranch = 3,
}

impl Aggregator {
    /// Create an Aggregator
    pub fn new(config: AggregatorConfig, poll_interval: Duration, chain_id: String) -> Self {
        let rpc_client = RpcClient::new(&config.rgbpp_uri);
        Aggregator {
            chain_id,
            config: config.clone(),
            poll_interval,
            rpc_client,
            rgbpp_scripts: get_script_map(config.rgbpp_scripts),
            branch_scripts: get_script_map(config.branch_scripts),
            rgbpp_assets: get_asset_map(config.rgbpp_asset_configs),
            rgbpp_locks: get_rgbpp_locks(config.rgbpp_lock_configs),
        }
    }

    /// Run the Aggregator
    pub fn run(&self) {
        info!("chain id: {}", self.chain_id);

        // Setup cancellation token
        let stop_rx = new_crossbeam_exit_rx();
        let poll_interval = self.poll_interval;
        let poll_service: Aggregator = self.clone();

        let aggregator_jh = thread::Builder::new()
            .name(THREAD_NAME.into())
            .spawn(move || {
                loop {
                    match stop_rx.try_recv() {
                        Ok(_) => {
                            info!("Aggregator received exit signal, stopped");
                            break;
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => {
                            // No exit signal, continue execution
                        }
                        Err(_) => {
                            info!("Error receiving exit signal");
                            break;
                        }
                    }

                    if let Err(e) = poll_service.scan_rgbpp_request() {
                        info!("Aggregator: {:?}", e);
                    }

                    thread::sleep(poll_interval);
                }
            })
            .expect("Start aggregator failed!");
        register_thread(THREAD_NAME, aggregator_jh);
    }

    fn scan_rgbpp_request(&self) -> Result<(), Error> {
        info!("Scan RGB++ Request ...");

        let stop: CancellationToken = new_tokio_exit_rx();

        let request_search_option = self.build_request_cell_search_option()?;
        let mut cursor = None;
        let limit = 10;

        loop {
            if stop.is_cancelled() {
                info!("Aggregator scan_rgbpp_request received exit signal, exiting now");
                return Ok(());
            }

            let request_cells = self
                .rpc_client
                .get_cells(
                    request_search_option.clone().into(),
                    Order::Asc,
                    limit.into(),
                    cursor,
                )
                .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;

            if request_cells.objects.is_empty() {
                info!("No more request cells found");
                break;
            }
            cursor = Some(request_cells.last_cursor);

            info!("Found {} request cells", request_cells.objects.len());
            let cells_with_messge = self.check_request(request_cells.objects.clone());
            info!("Found {} valid request cells", cells_with_messge.len());
            if cells_with_messge.is_empty() {
                break;
            }

            let custodian_tx = self.create_custodian_tx(cells_with_messge)?;
            match wait_for_tx_confirmation(
                self.rpc_client.clone(),
                custodian_tx,
                Duration::from_secs(15),
            ) {
                Ok(()) => info!("Transaction confirmed"),
                Err(e) => info!("{}", e.to_string()),
            }

            let leap_tx = self.create_leap_tx()?;
            match wait_for_tx_confirmation(
                self.rpc_client.clone(),
                leap_tx,
                Duration::from_secs(15),
            ) {
                Ok(()) => info!("Transaction confirmed"),
                Err(e) => info!("{}", e.to_string()),
            }

            let update_queue_tx = self.create_update_rgbpp_queue_tx()?;
            match wait_for_tx_confirmation(
                self.rpc_client.clone(),
                update_queue_tx,
                Duration::from_secs(15),
            ) {
                Ok(()) => info!("Transaction confirmed"),
                Err(e) => info!("{}", e.to_string()),
            }
        }

        Ok(())
    }

    pub(crate) fn build_request_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let request_script = self.get_rgbpp_script(REQUEST_LOCK)?;
        Ok(CellQueryOptions::new_lock(request_script))
    }

    pub(crate) fn build_message_queue_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let message_queue_type = self.get_rgbpp_script(QUEUE_TYPE)?;
        let (message_queue_lock, _) =
            get_sighash_script_from_privkey(self.config.rgbpp_queue_lock_key_path.clone())?;

        let cell_query_option = CellQueryOptions {
            primary_script: message_queue_type,
            primary_type: PrimaryScriptType::Type,
            with_data: Some(true),
            secondary_script: Some(message_queue_lock),
            secondary_script_len_range: None,
            data_len_range: None,
            capacity_range: None,
            block_range: None,
            order: QueryOrder::Asc,
            limit: Some(1),
            maturity: MaturityOption::Mature,
            min_total_capacity: 1,
            script_search_mode: None,
        };
        Ok(cell_query_option)
    }

    fn check_request(&self, cells: Vec<Cell>) -> Vec<(Cell, Transfer)> {
        cells
            .into_iter()
            .filter_map(|cell| {
                let live_cell: LiveCell = cell.clone().into();
                RequestLockArgs::from_slice(&live_cell.output.lock().args().raw_data())
                    .ok()
                    .and_then(|args| {
                        let target_request_type_hash = args.request_type_hash();
                        info!("target_request_type_hash: {:?}", target_request_type_hash);
                        let content = args.content();
                        let target_chain_id: Bytes = content.target_chain_id().as_bytes();
                        info!("target_chain_id: {:?}", target_chain_id);
                        let request_type = content.request_type();
                        let message = content.message();
                        let (check_message, transfer) = {
                            let message_union = message.to_enum();
                            match message_union {
                                MessageUnion::Transfer(transfer) => {
                                    let transfer_amount: u128 = transfer.amount().unpack();
                                    let check_message = cell
                                        .clone()
                                        .output_data
                                        .and_then(|data| decode_udt_amount(data.as_bytes()))
                                        .map_or(false, |amount| {
                                            info!(
                                                "original amount: {:?}, transfer amount: {:?}",
                                                amount, transfer_amount
                                            );
                                            transfer_amount <= amount
                                        });
                                    (check_message, transfer)
                                }
                            }
                        };
                        let request_type_hash = self
                            .rgbpp_scripts
                            .get(QUEUE_TYPE)
                            .map(|script_info| script_info.script.calc_script_hash());
                        if Some(target_request_type_hash) == request_type_hash
                            // && self.chain_id.clone() == target_chain_id
                            && request_type == Byte::new(RequestType::CkbToBranch as u8)
                            && check_message
                        {
                            Some((cell, transfer))
                        } else {
                            None
                        }
                    })
            })
            .collect()
    }

    fn create_update_rgbpp_queue_tx(&self) -> Result<H256, Error> {
        todo!()
    }

    fn get_rgbpp_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self.rgbpp_scripts
            .get(script_name)
            .map(|script_info| script_info.cell_dep.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    fn _get_branch_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self.branch_scripts
            .get(script_name)
            .map(|script_info| script_info.cell_dep.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    fn get_rgbpp_script(&self, script_name: &str) -> Result<Script, Error> {
        self.rgbpp_scripts
            .get(script_name)
            .map(|script_info| script_info.script.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    fn _get_branch_script(&self, script_name: &str) -> Result<Script, Error> {
        self.branch_scripts
            .get(script_name)
            .map(|script_info| script_info.script.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    fn fee_rate(&self) -> Result<u64, Error> {
        let value = {
            let dynamic = self
                .rpc_client
                .get_fee_rate_statistics(None)
                .map_err(|e| Error::RpcError(format!("get dynamic fee rate error: {}", e)))?
                .ok_or_else(|| Error::RpcError("get dynamic fee rate error: None".to_string()))
                .map(|resp| resp.median)
                .map(Into::into)
                .map_err(|e| Error::RpcError(format!("get dynamic fee rate error: {}", e)))?;
            info!("CKB fee rate: {} (dynamic)", FeeRate(dynamic));
            if dynamic > CKB_FEE_RATE_LIMIT {
                warn!(
                    "dynamic CKB fee rate {} is too large, it seems unreasonable;\
                so the upper limit {} will be used",
                    FeeRate(dynamic),
                    FeeRate(CKB_FEE_RATE_LIMIT)
                );
                CKB_FEE_RATE_LIMIT
            } else {
                dynamic
            }
        };
        Ok(value)
    }
}

fn get_script_map(scripts: Vec<ScriptConfig>) -> HashMap<String, ScriptInfo> {
    scripts
        .iter()
        .map(|s| {
            (
                s.script_name.clone(),
                ScriptInfo {
                    script: serde_json::from_str::<ckb_jsonrpc_types::Script>(&s.script)
                        .expect("config string to script")
                        .into(),
                    cell_dep: serde_json::from_str::<ckb_jsonrpc_types::CellDep>(&s.cell_dep)
                        .expect("config string to cell dep")
                        .into(),
                },
            )
        })
        .collect()
}

fn get_asset_map(asset_configs: Vec<AssetConfig>) -> HashMap<H256, AssetInfo> {
    let mut is_capacity_found = false;

    asset_configs
        .into_iter()
        .map(|asset_config| {
            let script = serde_json::from_str::<ckb_jsonrpc_types::Script>(&asset_config.script)
                .expect("config string to script")
                .into();
            let script_name = asset_config.script_name.clone();
            let is_capacity = asset_config.is_capacity && !is_capacity_found;
            if is_capacity {
                is_capacity_found = true;
            }
            let asset_id = asset_config.asset_id.clone();
            (
                H256::from_str(&asset_id).expect("asset id to h256"),
                AssetInfo {
                    script,
                    is_capacity,
                    script_name,
                },
            )
        })
        .collect()
}

fn get_rgbpp_locks(lock_configs: Vec<LockConfig>) -> HashMap<H256, Script> {
    lock_configs
        .iter()
        .map(|lock_config| {
            let lock_hash = H256::from_str(&lock_config.lock_hash).expect("lock hash to h256");
            let script = serde_json::from_str::<ckb_jsonrpc_types::Script>(&lock_config.script)
                .expect("config string to script")
                .into();
            (lock_hash, script)
        })
        .collect()
}

fn wait_for_tx_confirmation(
    _client: RpcClient,
    _tx_hash: H256,
    timeout: Duration,
) -> Result<(), Error> {
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            return Err(Error::TimedOut(
                "Transaction confirmation timed out".to_string(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ckb_types::{bytes::Bytes, core::ScriptHashType};

    use std::str::FromStr;

    #[test]
    fn calc_script() {
        let code_hash = "00000000000000000000000000000000000000000000000000545950455f4944";
        let args = "57fdfd0617dcb74d1287bb78a7368a3a4bf9a790cfdcf5c1a105fd7cb406de0d";
        let script_hash = "6283a479a3cf5d4276cd93594de9f1827ab9b55c7b05b3d28e4c2e0a696cfefd";

        let code_hash = H256::from_str(code_hash).unwrap();
        let args = Bytes::from(hex::decode(args).unwrap());

        let script = Script::new_builder()
            .code_hash(code_hash.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(args.pack())
            .build();

        println!("{:?}", script.calc_script_hash());

        assert_eq!(
            script.calc_script_hash().as_bytes(),
            Bytes::from(hex::decode(script_hash).unwrap())
        );
    }
}
