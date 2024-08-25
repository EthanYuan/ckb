mod clear_queue_outbox;

pub use crate::schemas::leap::{CrossChainQueue, Request, Requests};
use crate::{Aggregator, SIGHASH_TYPE_HASH};

use aggregator_common::{error::Error, utils::privkey::get_sighash_lock_args_from_privkey};
use ckb_channel::Receiver;
use ckb_logger::{error, info};
use ckb_sdk::{
    rpc::ckb_indexer::{Cell, Order},
    traits::{CellQueryOptions, LiveCell, MaturityOption, PrimaryScriptType, QueryOrder},
};
use ckb_types::{
    core::ScriptHashType,
    packed::{Byte32, OutPoint, Script},
    prelude::*,
};

use std::collections::HashSet;
use std::thread;

impl Aggregator {
    /// Collect Branch requests and send them to the RGB++ chain
    pub fn poll_branch_requests(&self, stop_rx: Receiver<()>) {
        info!("Branch Aggregator service started ...");

        let poll_interval = self.poll_interval;
        let poll_service: Aggregator = self.clone();

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
            let rgbpp_requests = poll_service.get_branch_queue_outbox_requests();
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
                let _ = self.create_clear_queue_outbox_tx();
            }

            thread::sleep(poll_interval);
        }
    }

    fn get_branch_queue_outbox_requests(&self) -> Result<(Vec<Request>, OutPoint), Error> {
        let (queue_cell, queue_cell_data) = self.get_branch_queue_outbox_cell()?;
        if queue_cell_data.outbox().is_empty() {
            info!("No requests in queue");
            return Ok((vec![], OutPoint::default()));
        }
        let request_ids: Vec<Byte32> = queue_cell_data.outbox().into_iter().collect();

        let queue_out_point = queue_cell.out_point.clone();
        let (_, witness_input_type) =
            self.get_tx_witness_input_type(queue_cell.out_point, self.branch_rpc_client.clone())?;
        let requests = Requests::from_slice(&witness_input_type.raw_data()).map_err(|e| {
            Error::TransactionParseError(format!("get requests from witness error: {}", e))
        })?;
        info!("Found {} requests in witness", requests.len());

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
        info!("Found {} queue outbox cell", queue_cell.objects.len());
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
        info!(
            "message_queue_outbox_lock_args: {:?}",
            message_queue_outbox_lock_args.pack().to_string()
        );
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
}
