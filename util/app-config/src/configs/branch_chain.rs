use ckb_types::H256;
use serde::{Deserialize, Serialize};

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

/// Branch Chain config options.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchChainConfig {
    /// Aggregator config options
    #[serde(default)]
    pub aggregator: AggregatorConfig,
}

impl Default for BranchChainConfig {
    fn default() -> Self {
        BranchChainConfig {
            aggregator: AggregatorConfig::default(),
        }
    }
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
    /// The poll interval by secs
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// Customize block filter
    #[serde(default)]
    pub block_filter: Option<String>,
    /// Customize cell filter
    #[serde(default)]
    pub cell_filter: Option<String>,
    /// Maximum number of concurrent db background jobs (compactions and flushes)
    #[serde(default)]
    pub db_background_jobs: Option<NonZeroUsize>,
    /// Maximal db info log files to be kept.
    #[serde(default)]
    pub db_keep_log_file_num: Option<NonZeroUsize>,
    /// The init tip block hash
    #[serde(default)]
    pub init_tip_hash: Option<H256>,
}

const fn default_poll_interval() -> u64 {
    2
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        AggregatorConfig {
            rgbpp_uri: "http://127.0.0.1:8114".to_string(),
            poll_interval: 2,
            store: PathBuf::new(),
            block_filter: None,
            cell_filter: None,
            db_background_jobs: None,
            db_keep_log_file_num: None,
            init_tip_hash: None,
        }
    }
}
