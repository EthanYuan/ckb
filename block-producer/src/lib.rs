//! Block Producer

use ckb_async_runtime::{
    tokio::{
        self,
        time::{self, interval, Duration},
    },
    Handle,
};
use ckb_chain::chain::ChainController;
use ckb_error::{Error, OtherError};
use ckb_logger::{error, info};
use ckb_network::{NetworkController, SupportProtocols};
use ckb_shared::shared::Shared;
use ckb_stop_handler::{new_tokio_exit_rx, CancellationToken};
use ckb_types::{packed, prelude::*, H256};

use std::collections::HashSet;
use std::sync::Arc;

/// Block Producer
#[derive(Clone)]
pub struct BlockProducer {
    network_controller: NetworkController,
    shared: Shared,
    chain: ChainController,
    async_handle: Handle,
    poll_interval: Duration,
}

impl BlockProducer {
    /// Create a new BlockProducer
    pub fn new(
        network_controller: NetworkController,
        shared: Shared,
        chain: ChainController,
        poll_interval: Duration,
    ) -> Self {
        let async_handle = shared.async_handle().clone();
        BlockProducer {
            network_controller,
            shared,
            chain,
            async_handle,
            poll_interval,
        }
    }

    fn generate_block(&self) -> Result<H256, Error> {
        let tx_pool = self.shared.tx_pool_controller();
        let block_template = tx_pool
            .get_block_template(None, None, None)
            .map_err(|err| OtherError::new(err.to_string()))?
            .map_err(|err| OtherError::new(err.to_string()))?;

        self.process_and_announce_block(block_template.into())
    }

    fn process_and_announce_block(&self, block: packed::Block) -> Result<H256, Error> {
        let block_view = Arc::new(block.into_view());
        let content = packed::CompactBlock::build_from_block(&block_view, &HashSet::new());
        let message = packed::RelayMessage::new_builder().set(content).build();

        // insert block to chain
        self.chain.process_block(Arc::clone(&block_view))?;

        // announce new block
        if let Err(err) = self
            .network_controller
            .quick_broadcast(SupportProtocols::RelayV2.protocol_id(), message.as_bytes())
        {
            error!("Broadcast new block failed: {:?}", err);
        }

        Ok(block_view.header().hash().unpack())
    }

    /// Start producing blocks on schedule
    pub fn produce_blocks_on_schedule(&self) {
        // Setup cancellation token
        let stop: CancellationToken = new_tokio_exit_rx();
        let async_handle = self.async_handle.clone();
        let poll_interval = self.poll_interval;
        let poll_service = self.clone();

        self.async_handle.spawn(async move {
            if stop.is_cancelled() {
                info!("BlockProducer received exit signal, cancel generate_block task, exit now");
                return;
            }

            let mut interval = interval(poll_interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let service = poll_service.clone();
                        if let Err(e) = async_handle.spawn_blocking(move || {
                            service.generate_block()
                        }).await {
                            error!("Error generating block: {:?}", e);
                        }
                    }
                    _ = stop.cancelled() => {
                        info!("BlockProducer received exit signal, exiting now");
                        break;
                    },
                }
            }
        });
    }
}
