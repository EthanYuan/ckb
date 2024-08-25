use crate::Aggregator;

use aggregator_common::error::Error;
use ckb_logger::info;
use ckb_types::H256;

use std::thread::sleep;
use std::time::Duration;

impl Aggregator {
    pub(crate) fn check_storage(&self) -> Result<(), Error> {
        if let Some(tx_hash) = self.store.get_staged_tx()? {
            let hash = H256::from_slice(tx_hash.as_bytes())
                .expect("Failed to convert staged transaction hash to H256");
            let tx = self
                .branch_rpc_client
                .get_transaction(hash.clone())
                .map_err(|e| Error::RpcError(format!("Failed to get transaction: {}", e)))?;
            match tx {
                Some(tx) => {
                    let height = tx.tx_status.block_number;
                    match height {
                        Some(height) => {
                            if self.store.is_tx_stored(height.into(), tx_hash.clone())? {
                                self.store.clear_staged_tx()?;
                            } else {
                                self.store.insert_branch_request(height.into(), tx_hash)?;
                                self.store.clear_staged_tx()?;
                            }
                        }
                        None => {
                            let height = self.wait_for_transaction_packing(hash)?;
                            self.store.insert_branch_request(height.into(), tx_hash)?;
                            self.store.clear_staged_tx()?;
                        }
                    }
                }
                None => {
                    // Theoretically, this situation does not exist
                    self.store.clear_staged_tx()?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn create_clear_queue_outbox_tx(&self) -> Result<(), Error> {
        Ok(())
    }

    fn wait_for_transaction_packing(&self, tx_hash: H256) -> Result<u64, Error> {
        loop {
            let tx = self
                .branch_rpc_client
                .get_transaction(
                    H256::from_slice(tx_hash.as_bytes())
                        .expect("Failed to convert staged transaction hash to H256"),
                )
                .map_err(|e| Error::RpcError(format!("Failed to get transaction: {}", e)))?;

            if let Some(tx) = tx {
                if let Some(height) = tx.tx_status.block_number {
                    // Transaction has been included in a block, return the height
                    return Ok(height.into());
                } else {
                    // Transaction is pending, log and wait
                    info!("Transaction is pending, waiting for block...");
                }
            }

            // Wait before next retry
            sleep(Duration::from_secs(1));
        }
    }
}
