#![allow(missing_docs)]

mod clear;
mod custodian;
mod schemas;

use ckb_app_config::ScriptConfig;
use ckb_sdk::rpc::CkbRpcClient as RpcClient;
use ckb_types::packed::{CellDep, Script};

use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScriptInfo {
    pub script: Script,
    pub cell_dep: CellDep,
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
}

impl RgbppTxBuilder {
    pub fn new(
        chain_id: String,
        rgbpp_uri: String,
        rgbpp_script_config: Vec<ScriptConfig>,
        rgbpp_custodian_lock_key_path: PathBuf,
        rgbpp_queue_lock_key_path: PathBuf,
        rgbpp_ckb_provider_key_path: PathBuf,
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
