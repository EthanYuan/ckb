mod burn_tx;
mod clear_queue_outbox;

pub use crate::schemas::leap::{CrossChainQueue, Request, Requests};
use crate::{wait_for_tx_confirmation, Aggregator, SIGHASH_TYPE_HASH};

use aggregator_common::{error::Error, utils::privkey::get_sighash_lock_args_from_privkey};
use ckb_channel::Receiver;
use ckb_logger::{debug, error, info};
use ckb_sdk::{
    rpc::{
        ckb_indexer::{Cell, Order},
        ResponseFormatGetter,
    },
    traits::{CellQueryOptions, LiveCell, MaturityOption, PrimaryScriptType, QueryOrder},
};
use ckb_types::{
    core::ScriptHashType,
    packed::{Byte32, Bytes as PackedBytes, OutPoint, Script, Transaction, WitnessArgs},
    prelude::*,
    H256,
};

use std::collections::HashSet;
use std::thread;
use std::time::Duration;

impl Aggregator {
    /// Collect Branch requests and send them to the RGB++ chain
    pub fn poll_branch_requests(&self, stop_rx: Receiver<()>) {
        info!("Branch Aggregator service started ...");

        let poll_interval = self.poll_interval;

        loop {
            match stop_rx.try_recv() {
                Ok(_) => {
                    info!("Aggregator received exit signal, stopped");
                    break;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // No exit signal, continue execution
                }
                Err(_) => {
                    info!("Error receiving exit signal");
                    break;
                }
            }

            // get Branch queue outbox data
            let rgbpp_requests = self.get_branch_queue_outbox_requests();
            let (rgbpp_requests, _queue_cell) = match rgbpp_requests {
                Ok((rgbpp_requests, queue_cell)) => (rgbpp_requests, queue_cell),
                Err(e) => {
                    error!("get RGB++ queue data error: {}", e.to_string());
                    continue;
                }
            };

            // clear queue
            if rgbpp_requests.is_empty() {
                let _ = self.check_storage();
            } else {
                let clear_queue_tx = self.create_clear_queue_outbox_tx(rgbpp_requests);
                let clear_queue_tx = match clear_queue_tx {
                    Ok(clear_queue_tx) => clear_queue_tx,
                    Err(e) => {
                        error!("{}", e.to_string());
                        continue;
                    }
                };
                match wait_for_tx_confirmation(
                    self.branch_rpc_client.clone(),
                    clear_queue_tx.clone(),
                    Duration::from_secs(30),
                ) {
                    Ok(height) => {
                        self.store
                            .insert_branch_request(height, clear_queue_tx)
                            .expect("Failed to insert clear queue transaction into storage");
                        self.store.clear_staged_tx().expect(
                            "Failed to clear staged transactions after successful clear queue",
                        );
                    }
                    Err(e) => error!("{}", e.to_string()),
                }
            }

            if let Err(e) = self.collect_branch_requests() {
                info!("Aggregator collect Branch requests: {:?}", e);
            }

            thread::sleep(poll_interval);
        }
    }

    pub fn poll_rgbpp_unlock(&self, stop_rx: Receiver<()>) {
        info!("RGB++ Unlock loop started ...");

        let poll_interval = self.poll_interval;

        loop {
            thread::sleep(poll_interval);

            match stop_rx.try_recv() {
                Ok(_) => {
                    info!("Aggregator received exit signal, stopped");
                    break;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // No exit signal, continue execution
                }
                Err(_) => {
                    info!("Error receiving exit signal");
                    break;
                }
            }

            let pending_request = self.store.get_earliest_pending();
            match pending_request {
                Ok(None) => {
                    info!("No pending request found");
                }
                Err(e) => {
                    error!("{}", e.to_string());
                    continue;
                }
                Ok(Some((height, tx_hash))) => {
                    info!("Found pending request at height: {}", height);
                    let tip = self.branch_rpc_client.get_tip_block_number();
                    let tip: u64 = match tip {
                        Ok(tip) => tip.into(),
                        Err(e) => {
                            error!("{}", e.to_string());
                            continue;
                        }
                    };
                    if height + self.config.challenge_period < tip {
                        let requests_in_witness = if let Ok((_, requests_in_witness)) =
                            self.get_burn_tx_requests_in_witness(tx_hash.clone())
                        {
                            requests_in_witness
                        } else {
                            error!("Burn tx requests not found in witness");
                            continue;
                        };

                        let inbox_tx = self.rgbpp_tx_builder.create_add_queue_inbox_tx(
                            height,
                            tx_hash.0.into(),
                            requests_in_witness.raw_data(),
                        );
                        let inbox_tx = match inbox_tx {
                            Ok(inbox_tx) => {
                                H256::from_slice(inbox_tx.as_bytes()).expect("unlock tx to H256")
                            }
                            Err(e) => {
                                error!("{}", e.to_string());
                                continue;
                            }
                        };
                        match wait_for_tx_confirmation(
                            self.rgbpp_rpc_client.clone(),
                            H256(inbox_tx.0),
                            Duration::from_secs(30),
                        ) {
                            Ok(_) => {
                                self.store
                                    .commit_branch_request(height)
                                    .expect("commit branch request");
                            }
                            Err(e) => error!("{}", e.to_string()),
                        }
                    }
                }
            }
        }
    }

    fn get_branch_queue_outbox_requests(&self) -> Result<(Vec<Request>, OutPoint), Error> {
        let (queue_cell, queue_cell_data) = self.get_branch_queue_outbox_cell()?;
        if queue_cell_data.outbox().is_empty() {
            debug!("No requests in queue");
            return Ok((vec![], OutPoint::default()));
        }
        let request_ids: Vec<Byte32> = queue_cell_data.outbox().into_iter().collect();

        let queue_out_point = queue_cell.out_point.clone();
        let (_, witness_input_type) =
            self.get_tx_witness_input_type(queue_cell.out_point, self.branch_rpc_client.clone())?;
        let requests = Requests::from_slice(&witness_input_type.raw_data()).map_err(|e| {
            Error::TransactionParseError(format!("get requests from witness error: {}", e))
        })?;
        info!("Found {} requests in outbox", requests.len());

        // check requests
        let request_set: HashSet<Byte32> = requests
            .clone()
            .into_iter()
            .map(|request| request.as_bytes().pack().calc_raw_data_hash())
            .collect();
        let all_ids_present = request_ids.iter().all(|id| request_set.contains(id));
        if all_ids_present {
            Ok((requests.into_iter().collect(), queue_out_point.into()))
        } else {
            Err(Error::QueueCellDataError(
                "Request IDs in queue cell data do not match witness".to_string(),
            ))
        }
    }

    fn get_branch_queue_outbox_cell(&self) -> Result<(Cell, CrossChainQueue), Error> {
        info!("Scan Branch Message Queue Outbox ...");

        let queue_cell_search_option = self.build_branch_queue_outbox_cell_search_option()?;
        let queue_cell = self
            .branch_rpc_client
            .get_cells(queue_cell_search_option.into(), Order::Asc, 1.into(), None)
            .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;
        if queue_cell.objects.len() != 1 {
            return Err(Error::LiveCellNotFound(format!(
                "Branch queue outbox cell found: {}",
                queue_cell.objects.len()
            )));
        }
        debug!("Found {} queue outbox cell", queue_cell.objects.len());
        let queue_cell = queue_cell.objects[0].clone();

        let queue_live_cell: LiveCell = queue_cell.clone().into();
        let queue_data = queue_live_cell.output_data;
        let queue = CrossChainQueue::from_slice(&queue_data)
            .map_err(|e| Error::QueueCellDataDecodeError(e.to_string()))?;

        Ok((queue_cell, queue))
    }

    fn build_branch_queue_outbox_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let (message_queue_outbox_lock_args, _) = get_sighash_lock_args_from_privkey(
            self.config
                .branch_chain_token_manager_outbox_lock_key_path
                .clone(),
        )?;
        let message_queue_outbox_lock = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(message_queue_outbox_lock_args.pack())
            .build();

        let cell_query_option = CellQueryOptions {
            primary_script: message_queue_outbox_lock,
            primary_type: PrimaryScriptType::Lock,
            with_data: Some(true),
            secondary_script: None,
            secondary_script_len_range: None,
            data_len_range: None,
            capacity_range: None,
            block_range: None,
            order: QueryOrder::Asc,
            limit: Some(1),
            maturity: MaturityOption::Mature,
            min_total_capacity: 1,
            script_search_mode: None,
        };
        Ok(cell_query_option)
    }

    pub(crate) fn get_burn_tx_requests_in_witness(
        &self,
        tx_hash: H256,
    ) -> Result<(H256, PackedBytes), Error> {
        let tx = self
            .branch_rpc_client
            .get_transaction(tx_hash.clone())
            .map_err(|e| Error::RpcError(format!("get transaction error: {}", e)))?
            .ok_or(Error::RpcError("get transaction error: None".to_string()))?
            .transaction
            .ok_or(Error::RpcError("get transaction error: None".to_string()))?
            .get_value()
            .map_err(|e| Error::RpcError(format!("get transaction error: {}", e)))?
            .inner;
        let tx: Transaction = tx.into();
        let witness = tx
            .witnesses()
            .get(0 as usize)
            .ok_or(Error::TransactionParseError(
                "get witness error: None".to_string(),
            ))?;
        let witness_input_type = WitnessArgs::from_slice(&witness.raw_data())
            .map_err(|e| Error::TransactionParseError(format!("get witness error: {}", e)))?
            .input_type()
            .to_opt()
            .ok_or(Error::TransactionParseError(
                "get witness input type error: None".to_string(),
            ))?;

        let requests = Requests::from_slice(&witness_input_type.raw_data()).map_err(|e| {
            Error::TransactionParseError(format!("get requests from witness error: {}", e))
        })?;
        info!("Found {} requests in burn tx witness", requests.len());

        Ok((tx.calc_tx_hash().unpack(), witness_input_type))
    }
}
