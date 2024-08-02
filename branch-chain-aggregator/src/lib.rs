//! Branch Chain Aggregator

pub(crate) mod client;
pub(crate) mod error;
pub(crate) mod schemas;

use ckb_app_config::AggregatorConfig;
use ckb_async_runtime::{
    tokio::{
        self,
        time::{self, interval, Duration},
    },
    Handle,
};
use ckb_logger::{error, info};
use ckb_sdk::{rpc::ckb_indexer::Cell, traits::CellQueryOptions};
use ckb_stop_handler::{new_tokio_exit_rx, CancellationToken};
use ckb_types::{
    core::ScriptHashType,
    packed::Script,
    prelude::{Builder, Entity, Pack},
};
use molecule::prelude::Byte;
use schemas::leap::{Message, RequestLockArgs};

use crate::client::RpcClient;
use crate::error::Error;
///
#[derive(Clone)]
pub struct Aggregator {
    chain_id: String,
    config: AggregatorConfig,
    async_handle: Handle,
    poll_interval: Duration,
    rpc_client: RpcClient,
}

#[allow(dead_code)]
enum RequestType {
    CkbToBranch = 1,
    BranchToCkb = 2,
    BranchToBranch = 3,
}

impl Aggregator {
    /// Create an Aggregator
    pub fn new(
        config: AggregatorConfig,
        async_handle: Handle,
        poll_interval: Duration,
        chain_id: String,
    ) -> Self {
        let rpc_client = RpcClient::new(&config.rgbpp_uri);
        Aggregator {
            chain_id,
            config,
            async_handle,
            poll_interval,
            rpc_client,
        }
    }

    /// Run the Aggregator
    pub fn run(&self) {
        info!("chain id: {}", self.chain_id);

        // Setup cancellation token
        let stop: CancellationToken = new_tokio_exit_rx();
        let poll_interval = self.poll_interval;
        let poll_service: Aggregator = self.clone();

        self.async_handle.spawn(async move {
            if stop.is_cancelled() {
                info!("Aggregator received exit signal, exit now");
                return;
            }

            let mut interval = interval(poll_interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let service = poll_service.clone();
                        if let Err(e) = service.scan_rgbpp_request().await {
                            error!("Error generating block: {:?}", e);
                        }
                    }
                    _ = stop.cancelled() => {
                        info!("Aggregator received exit signal, exiting now");
                        break;
                    },
                }
            }
        });
    }

    async fn scan_rgbpp_request(&self) -> Result<(), Error> {
        info!("Scan RGB++ Request ...");

        let stop: CancellationToken = new_tokio_exit_rx();

        let request_search_option = self.build_request_search_option();
        let mut cursor = None;
        let limit = 10;

        loop {
            if stop.is_cancelled() {
                info!("Aggregator scan_rgbpp_request received exit signal, exiting now");
                return Ok(());
            }

            let request_cells = self
                .rpc_client
                .get_cells(request_search_option.clone().into(), limit, cursor)
                .await
                .map_err(|_| Error::FetchLiveCellsError)?;
            if request_cells.objects.is_empty() {
                break;
            }
            cursor = Some(request_cells.last_cursor);

            let cells_with_messge = self.check_request(request_cells.objects);
            self.create_custodian_tx();
            self.create_leap_tx();
            self.create_update_rgbpp_queue_tx();
        }

        Ok(())
    }

    fn build_request_search_option(&self) -> CellQueryOptions {
        let request_script = Script::new_builder()
            .code_hash(self.config.rgbpp_request_lock_code_hash.clone().0.pack())
            .hash_type(ScriptHashType::Type.into())
            .build();
        CellQueryOptions::new_lock(request_script)
    }

    fn check_request(&self, cells: Vec<Cell>) -> Vec<(Cell, Message)> {
        cells
            .into_iter()
            .filter_map(|cell| {
                RequestLockArgs::from_slice(cell.output.lock.args.as_bytes())
                    .ok()
                    .and_then(|args| {
                        let content = args.content();
                        let target_chain_id = content.target_chain_id().as_bytes();
                        let request_type = content.request_type();
                        let message = content.message();
                        if self.chain_id.clone() == target_chain_id
                            && request_type == Byte::new(RequestType::CkbToBranch as u8)
                        {
                            Some((cell, message))
                        } else {
                            None
                        }
                    })
            })
            .collect()
    }

    fn create_custodian_tx(&self) {
        // TODO
    }

    fn create_leap_tx(&self) {
        // TODO
    }

    fn create_update_rgbpp_queue_tx(&self) {
        // TODO
    }
}
