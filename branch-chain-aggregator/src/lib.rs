//! Branch Chain Aggregator

#![allow(missing_docs)]

pub mod indexer;
pub mod pool;
pub mod service;
pub mod store;

use crate::service::Service;

use ckb_app_config::AggregatorConfig;
use ckb_async_runtime::Handle;
use jsonrpc_core_client::transports::http;

#[derive(Clone)]
pub struct Aggregator {
    config: AggregatorConfig,
    async_handle: Handle,
}

impl Aggregator {
    pub fn new(config: AggregatorConfig, async_handle: Handle) -> Self {
        Aggregator {
            config,
            async_handle,
        }
    }

    pub fn run(&self) {
        let store_path = self.config.store.to_str().expect("store path").to_owned();
        let rgbpp_uri = self.config.rgbpp_uri.clone();
        let block_filter = self.config.block_filter.clone();
        let cell_filter = self.config.cell_filter.clone();
        let async_handle = self.async_handle.clone();

        async_handle.spawn(async move {
            let service = Service::new(
                store_path.clone().as_str(),
                None,
                "127.0.0.1:8216",
                std::time::Duration::from_secs(2),
            );
            let client = http::connect(rgbpp_uri.clone().as_str())
                .await
                .unwrap_or_else(|_| {
                    panic!("Failed to connect to {:?}", rgbpp_uri.clone().as_str())
                });
            service
                .poll(client, block_filter.as_deref(), cell_filter.as_deref())
                .await;
        });
    }
}
