use ckb_types::{bytes::Bytes, H256};
use serde::{Deserialize, Serialize};

use std::path::{Path, PathBuf};

/// Branch Chain config options.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct BranchChainConfig {
    /// Aggregator config options
    #[serde(default)]
    pub aggregator: AggregatorConfig,
}

impl BranchChainConfig {
    /// Canonicalizes paths in the config options.
    ///
    /// If `self.aggregator.store` is not set, set it to `data_dir / branch_chain / aggregator`.
    ///
    /// If any of the above paths is relative, convert them to absolute path using
    /// `root_dir` as current working directory.
    pub fn adjust<P: AsRef<Path>>(&mut self, root_dir: &Path, branch_chain_dir: P) {
        _adjust(
            root_dir,
            branch_chain_dir.as_ref(),
            &mut self.aggregator.store,
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
    /// RGB++ request lock code hash
    #[serde(default)]
    pub rgbpp_request_lock_code_hash: H256,
    /// RGB++ message queue type code hash
    #[serde(default)]
    pub rgbpp_message_queue_type_code_hash: H256,
    /// RGB++ message queue type args
    #[serde(default)]
    pub rgbpp_message_queue_type_args: Bytes,
    /// RGB++ sighash key path
    #[serde(default)]
    pub rgbpp_sighash_key_path: PathBuf,
    /// RGB++ branch chain sighash key path
    #[serde(default)]
    pub branch_chain_sighash_key_path: PathBuf,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        AggregatorConfig {
            store: PathBuf::new(),
            rgbpp_uri: "http://127.0.0.1:8114".to_string(),
            rgbpp_request_lock_code_hash: H256::default(),
            rgbpp_message_queue_type_code_hash: H256::default(),
            rgbpp_message_queue_type_args: Bytes::default(),
            rgbpp_sighash_key_path: PathBuf::new(),
            branch_chain_sighash_key_path: PathBuf::new(),
        }
    }
}
