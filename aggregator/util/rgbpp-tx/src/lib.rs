#![allow(missing_docs)]

mod branch_to_rgbpp;
mod rgbpp_to_branch;
mod schemas;

pub use crate::schemas::leap::{self, CrossChainQueue, Request, Requests};

use aggregator_common::{
    error::Error,
    utils::{privkey::get_sighash_lock_args_from_privkey, QUEUE_TYPE},
};
use ckb_app_config::{AssetConfig, LockConfig, ScriptConfig};
use ckb_logger::{info, warn};
use ckb_sdk::{
    rpc::CkbRpcClient as RpcClient,
    rpc::ResponseFormatGetter,
    traits::{CellQueryOptions, MaturityOption, PrimaryScriptType, QueryOrder},
};
use ckb_types::{
    core::{FeeRate, ScriptHashType},
    h256,
    packed::{Bytes as PackedBytes, CellDep, Script, Transaction, WitnessArgs},
    prelude::*,
    H256,
};

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptInfo {
    pub script: Script,
    pub cell_dep: CellDep,
}

/// Sighash type hash
pub const SIGHASH_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");
/// Confirmation threshold
const CONFIRMATION_THRESHOLD: u64 = 24;
/// CKB fee rate limit
const CKB_FEE_RATE_LIMIT: u64 = 5000;

#[derive(Clone, Debug, PartialEq, Eq)]
struct AssetInfo {
    pub script: Script,
    pub is_capacity: bool,
    pub script_name: String,
}

#[derive(Clone)]
pub struct RgbppTxBuilder {
    chain_id: String,
    rgbpp_uri: String,
    rgbpp_rpc_client: RpcClient,
    rgbpp_scripts: HashMap<String, ScriptInfo>,
    rgbpp_custodian_lock_key_path: PathBuf,
    rgbpp_queue_lock_key_path: PathBuf,
    rgbpp_ckb_provider_key_path: PathBuf,
    asset_types: HashMap<H256, AssetInfo>,
    asset_locks: HashMap<H256, Script>,
}

impl RgbppTxBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: String,
        rgbpp_uri: String,
        rgbpp_script_config: Vec<ScriptConfig>,
        rgbpp_custodian_lock_key_path: PathBuf,
        rgbpp_queue_lock_key_path: PathBuf,
        rgbpp_ckb_provider_key_path: PathBuf,
        asset_types: Vec<AssetConfig>,
        asset_locks: Vec<LockConfig>,
    ) -> Self {
        let rgbpp_rpc_client = RpcClient::new(&rgbpp_uri);
        Self {
            chain_id,
            rgbpp_uri,
            rgbpp_rpc_client,
            rgbpp_scripts: get_script_map(rgbpp_script_config),
            rgbpp_custodian_lock_key_path,
            rgbpp_queue_lock_key_path,
            rgbpp_ckb_provider_key_path,
            asset_types: get_asset_types(asset_types),
            asset_locks: get_asset_locks(asset_locks),
        }
    }

    fn fee_rate(&self) -> Result<u64, Error> {
        let value = {
            let dynamic = self
                .rgbpp_rpc_client
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

    fn get_tx_witness_input_type(
        &self,
        out_point: ckb_jsonrpc_types::OutPoint,
        rpc_client: RpcClient,
    ) -> Result<(H256, PackedBytes), Error> {
        let tx_hash = out_point.tx_hash;
        let index: u32 = out_point.index.into();
        let tx = rpc_client
            .get_transaction(tx_hash.clone())
            .map_err(|e| Error::RpcError(format!("get transaction error: {}", e)))?
            .ok_or(Error::RpcError("get transaction error: None".to_string()))?
            .transaction
            .ok_or(Error::RpcError("get transaction error: None".to_string()))?
            .get_value()
            .map_err(|e| Error::RpcError(format!("get transaction error: {}", e)))?
            .inner;
        let tx: Transaction = tx.into();
        let witness = tx
            .witnesses()
            .get(index as usize)
            .ok_or(Error::TransactionParseError(
                "get witness error: None".to_string(),
            ))?;
        let witness_input_type = WitnessArgs::from_slice(&witness.raw_data())
            .map_err(|e| Error::TransactionParseError(format!("get witness error: {}", e)))?
            .input_type()
            .to_opt()
            .ok_or(Error::TransactionParseError(
                "get witness input type error: None".to_string(),
            ))?;
        Ok((tx.calc_tx_hash().unpack(), witness_input_type))
    }

    pub(crate) fn get_rgbpp_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self.rgbpp_scripts
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

    fn build_message_queue_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let message_queue_type = self.get_rgbpp_script(QUEUE_TYPE)?;
        let (message_queue_lock_args, _) =
            get_sighash_lock_args_from_privkey(self.rgbpp_queue_lock_key_path.clone())?;
        let message_queue_lock = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(message_queue_lock_args.pack())
            .build();

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

fn get_asset_types(asset_configs: Vec<AssetConfig>) -> HashMap<H256, AssetInfo> {
    let mut is_capacity_found = false;

    asset_configs
        .into_iter()
        .map(|asset_config| {
            let script = serde_json::from_str::<ckb_jsonrpc_types::Script>(&asset_config.script)
                .expect("config string to script")
                .into();
            let script_name = asset_config.asset_name.clone();
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

fn get_asset_locks(lock_configs: Vec<LockConfig>) -> HashMap<H256, Script> {
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
