use serde::{Deserialize, Serialize};

use std::path::{Path, PathBuf};

/// Aggregator config options.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregatorConfig {
    /// RGB++ URI
    #[serde(default)]
    pub rgbpp_uri: String,
    /// The Aggregator store path, default `data_dir / branch_chain / store`,
    /// which will be realized through AggregatorConfig::adjust.
    #[serde(default)]
    pub store: PathBuf,
    /// RGB++ CKB provider key path
    #[serde(default)]
    pub rgbpp_ckb_provider_key_path: PathBuf,
    /// RGB++ queue cell lock key path
    #[serde(default)]
    pub rgbpp_queue_lock_key_path: PathBuf,
    /// RGB++ custodian lock key path
    #[serde(default)]
    pub rgbpp_custodian_lock_key_path: PathBuf,
    /// Branch Chain URI
    #[serde(default)]
    pub branch_uri: String,
    /// Branch Chain capacity provider key path
    #[serde(default)]
    pub branch_chain_capacity_provider_key_path: PathBuf,
    /// Branch Chain token manager lock key path
    #[serde(default)]
    pub branch_chain_token_manager_lock_key_path: PathBuf,
    /// Branch Chain token manager with outbox key path
    #[serde(default)]
    pub branch_chain_token_manager_outbox_lock_key_path: PathBuf,
    /// RGB++ scripts
    #[serde(default)]
    pub rgbpp_scripts: Vec<ScriptConfig>,
    /// Branch Chain scripts
    #[serde(default)]
    pub branch_scripts: Vec<ScriptConfig>,
    /// Asset configs
    #[serde(default)]
    pub asset_types: Vec<AssetConfig>,
    /// Lock configs
    #[serde(default)]
    pub asset_locks: Vec<LockConfig>,
}

/// Script config options.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct ScriptConfig {
    /// Script name
    pub script_name: String,
    /// Script type
    pub script: String,
    /// Cell dep
    pub cell_dep: String,
}

/// Asset config options.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct AssetConfig {
    /// Script name
    pub asset_name: String,
    /// Is capacity
    pub is_capacity: bool,
    /// Asset ID
    pub asset_id: String,
    /// Script type
    pub script: String,
}

/// Lock config options.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct LockConfig {
    /// Lock hash
    pub lock_hash: String,
    /// Script type
    pub script: String,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        AggregatorConfig {
            store: PathBuf::new(),
            rgbpp_uri: "https://testnet.ckb.dev".to_string(),
            rgbpp_ckb_provider_key_path: PathBuf::new(),
            rgbpp_queue_lock_key_path: PathBuf::new(),
            rgbpp_custodian_lock_key_path: PathBuf::new(),
            branch_uri: "http://localhost:8114".to_string(),
            branch_chain_capacity_provider_key_path: PathBuf::new(),
            branch_chain_token_manager_lock_key_path: PathBuf::new(),
            branch_chain_token_manager_outbox_lock_key_path: PathBuf::new(),
            rgbpp_scripts: Vec::new(),
            branch_scripts: Vec::new(),
            asset_types: Vec::new(),
            asset_locks: Vec::new(),
        }
    }
}

impl AggregatorConfig {
    /// Canonicalizes paths in the config options.
    ///
    /// If `self.store` is not set, set it to `data_dir / branch_chain / aggregator`.
    ///
    /// If any of the above paths is relative, convert them to absolute path using
    /// `root_dir` as current working directory.
    pub fn adjust<P: AsRef<Path>>(&mut self, root_dir: &Path, branch_chain_dir: P) {
        _adjust(
            root_dir,
            branch_chain_dir.as_ref(),
            &mut self.store,
            "aggregator",
        );
    }
}

fn _adjust(root_dir: &Path, branch_chain_dir: &Path, target: &mut PathBuf, sub: &str) {
    if target.to_str().is_none() || target.to_str() == Some("") {
        *target = branch_chain_dir.to_path_buf().join(sub);
    } else if target.is_relative() {
        *target = root_dir.to_path_buf().join(&target)
    }
}
