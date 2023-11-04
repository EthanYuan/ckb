use serde::{Deserialize, Serialize};
use std::{default::Default, path::PathBuf};

/// IndexerR database type.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IndexerRDbType {
    /// Sqlite config options.
    #[default]
    Sqlite,
    /// Postgres config options.
    Postgres,
}

/// IndexerR config options.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexerRConfig {
    /// IndexerR database type.
    #[serde(default)]
    pub db_type: IndexerRDbType,
    /// The index-r store path, default `data_dir / indexer / indexer_r`,
    /// which will be realized through IndexerConfig::adjust.
    #[serde(default)]
    pub store: PathBuf,
    /// The database name, default `indexer_r`.
    #[serde(default = "default_db_name")]
    pub db_name: String,
    /// The database host.
    #[serde(default = "default_db_host")]
    pub db_host: String,
    /// The database port.
    #[serde(default = "default_db_port")]
    pub db_port: u16,
    /// The database user.
    #[serde(default = "default_db_user")]
    pub db_user: String,
    /// The database password.
    #[serde(default = "default_db_password")]
    pub password: String,
}

impl Default for IndexerRConfig {
    fn default() -> Self {
        Self {
            db_type: IndexerRDbType::default(),
            store: PathBuf::default(),
            db_name: default_db_name(),
            db_host: default_db_host(),
            db_port: default_db_port(),
            db_user: default_db_user(),
            password: default_db_password(),
        }
    }
}

fn default_db_name() -> String {
    "indexer_r".to_string()
}

fn default_db_host() -> String {
    "127.0.0.1".to_string()
}

fn default_db_port() -> u16 {
    8532
}

fn default_db_user() -> String {
    "postgres".to_string()
}

fn default_db_password() -> String {
    "123456".to_string()
}
