//! Branch Chain Aggregator

pub(crate) mod branch_to_rgbpp;
pub(crate) mod rgbpp_to_branch;
pub mod schemas;

use aggregator_common::{error::Error, utils::encode_udt_amount};
use aggregator_rgbpp_tx::RgbppTxBuilder;
use aggregator_storage::Storage;
use ckb_app_config::{AggregatorConfig, AssetConfig, LockConfig, ScriptConfig};
use ckb_logger::{info, warn};
use ckb_sdk::rpc::CkbRpcClient as RpcClient;
use ckb_types::{
    core::FeeRate,
    h256,
    packed::{CellDep, Script},
    H256,
};

use std::collections::HashMap;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

pub const SIGHASH_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");
const CKB_FEE_RATE_LIMIT: u64 = 5000;
const CONFIRMATION_THRESHOLD: u64 = 24;

/// Aggregator
#[derive(Clone)]
pub struct Aggregator {
    config: AggregatorConfig,
    poll_interval: Duration,
    rgbpp_rpc_client: RpcClient,
    branch_rpc_client: RpcClient,

    branch_scripts: HashMap<String, ScriptInfo>,
    asset_types: HashMap<H256, AssetInfo>,
    asset_locks: HashMap<H256, Script>,

    rgbpp_tx_builder: RgbppTxBuilder,

    store: Storage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptInfo {
    pub script: Script,
    pub cell_dep: CellDep,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AssetInfo {
    pub script: Script,
    pub is_capacity: bool,
    pub script_name: String,
}

impl Aggregator {
    /// Create an Aggregator
    pub fn new(config: AggregatorConfig, poll_interval: Duration, branch_chain_id: String) -> Self {
        let rgbpp_rpc_client = RpcClient::new(&config.rgbpp_uri);
        let branch_rpc_client = RpcClient::new(&config.branch_uri);
        let rgbpp_tx_builder = RgbppTxBuilder::new(
            branch_chain_id.clone(),
            config.rgbpp_uri.clone(),
            config.rgbpp_scripts.clone(),
            config.rgbpp_custodian_lock_key_path.clone(),
            config.rgbpp_queue_lock_key_path.clone(),
            config.rgbpp_queue_lock_inbox_key_path.clone(),
            config.rgbpp_ckb_provider_key_path.clone(),
            config.asset_types.clone(),
            config.asset_locks.clone(),
        );
        info!("aggregator store:{:?}", config.store);
        let store = Storage::new(config.store.clone()).expect("storage init");
        Aggregator {
            config: config.clone(),
            poll_interval,
            rgbpp_rpc_client,
            branch_rpc_client,
            branch_scripts: get_script_map(config.branch_scripts),
            asset_types: get_asset_types(config.asset_types),
            asset_locks: get_asset_locks(config.asset_locks),
            rgbpp_tx_builder,
            store,
        }
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
                .branch_rpc_client
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

fn wait_for_tx_confirmation(
    client: RpcClient,
    tx_hash: H256,
    timeout: Duration,
) -> Result<u64, Error> {
    let start = std::time::Instant::now();

    loop {
        let tx = client
            .get_transaction(tx_hash.clone())
            .map_err(|e| Error::RpcError(format!("Failed to get transaction: {}", e)))?;
        match tx {
            Some(tx) => {
                if let Some(height) = tx.tx_status.block_number {
                    return Ok(height.into());
                }
            }
            None => {
                return Err(Error::TransactionNotFound(format!(
                    "Transaction not found: {:?}",
                    tx_hash
                )));
            }
        }
        if start.elapsed() > timeout {
            return Err(Error::TimedOut(
                "Transaction confirmation timed out".to_string(),
            ));
        }
        sleep(Duration::from_secs(2));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ckb_types::{bytes::Bytes, core::ScriptHashType, prelude::*};

    use std::str::FromStr;

    #[test]
    fn calc_script() {
        let code_hash = "00000000000000000000000000000000000000000000000000545950455f4944";
        let args = "314f67c0ffd0c6fbffe886f03c6b00b42e4e66e3e71d32a66b8a38d69e6a4250";
        let target_script_hash = "9c6933d977360f115a3e9cd5a2e0e475853681b80d775d93ad0f8969da343e56";

        let code_hash = H256::from_str(code_hash).unwrap();
        let args = Bytes::from(hex::decode(args).unwrap());

        let script = Script::new_builder()
            .code_hash(code_hash.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(args.pack())
            .build();

        let script_hash: H256 = script.calc_script_hash().unpack();
        println!("script_hash: {:?}", script_hash.to_string());

        assert_eq!(
            script.calc_script_hash().as_bytes(),
            Bytes::from(hex::decode(target_script_hash).unwrap())
        );
    }
}
