//! Branch Chain Aggregator

pub(crate) mod branch_to_rgbpp;
pub(crate) mod rgbpp_to_branch;
pub(crate) mod schemas;

use aggregator_common::{error::Error, utils::encode_udt_amount};
use aggregator_rgbpp_tx::RgbppTxBuilder;
use ckb_app_config::{AggregatorConfig, AssetConfig, LockConfig, ScriptConfig};
use ckb_sdk::rpc::CkbRpcClient as RpcClient;
use ckb_types::{
    packed::{CellDep, Script},
    H256,
};

use std::collections::HashMap;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

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
            config.rgbpp_ckb_provider_key_path.clone(),
            config.asset_types.clone(),
            config.asset_locks.clone(),
        );
        Aggregator {
            config: config.clone(),
            poll_interval,
            rgbpp_rpc_client,
            branch_rpc_client,
            branch_scripts: get_script_map(config.branch_scripts),
            asset_types: get_asset_types(config.asset_types),
            asset_locks: get_asset_locks(config.asset_locks),
            rgbpp_tx_builder,
        }
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
    _client: RpcClient,
    _tx_hash: H256,
    timeout: Duration,
) -> Result<(), Error> {
    let start = std::time::Instant::now();

    loop {
        if true {
            sleep(Duration::from_secs(8));
            return Ok(());
        }

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

    use ckb_types::{bytes::Bytes, core::ScriptHashType, prelude::*};

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
