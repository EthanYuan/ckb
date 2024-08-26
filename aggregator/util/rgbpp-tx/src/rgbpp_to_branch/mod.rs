mod clear;
mod custodian;

pub use crate::schemas::leap::{CrossChainQueue, Request, Requests};
use crate::RgbppTxBuilder;

use aggregator_common::error::Error;
use ckb_logger::info;
use ckb_sdk::{
    rpc::ckb_indexer::{Cell, Order},
    traits::LiveCell,
};
use ckb_types::{
    packed::{Byte32, OutPoint},
    prelude::*,
};

use std::{collections::HashSet, path::PathBuf};

impl RgbppTxBuilder {
    pub fn get_rgbpp_queue_outbox_requests(&self) -> Result<(Vec<Request>, OutPoint), Error> {
        let (queue_cell, queue_cell_data) =
            self.get_rgbpp_queue_cell(self.rgbpp_queue_lock_key_path.clone())?;
        if queue_cell_data.outbox().is_empty() {
            info!("No requests in queue");
            return Ok((vec![], OutPoint::default()));
        }
        let request_ids: Vec<Byte32> = queue_cell_data.outbox().into_iter().collect();

        let queue_out_point = queue_cell.out_point.clone();
        let (_, witness_input_type) =
            self.get_tx_witness_input_type(queue_cell.out_point, self.rgbpp_rpc_client.clone())?;
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

    pub(crate) fn get_rgbpp_queue_cell(
        &self,
        privkey: PathBuf,
    ) -> Result<(Cell, CrossChainQueue), Error> {
        info!("Scan RGB++ Message Queue ...");

        let queue_cell_search_option = self.build_message_queue_cell_search_option(privkey)?;
        let queue_cell = self
            .rgbpp_rpc_client
            .get_cells(queue_cell_search_option.into(), Order::Asc, 1.into(), None)
            .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;
        if queue_cell.objects.len() != 1 {
            return Err(Error::LiveCellNotFound(format!(
                "Queue cell found: {}",
                queue_cell.objects.len()
            )));
        }
        info!("Found {} queue cell", queue_cell.objects.len());
        let queue_cell = queue_cell.objects[0].clone();

        let queue_live_cell: LiveCell = queue_cell.clone().into();
        let queue_data = queue_live_cell.output_data;
        let queue = CrossChainQueue::from_slice(&queue_data)
            .map_err(|e| Error::QueueCellDataDecodeError(e.to_string()))?;

        Ok((queue_cell, queue))
    }
}
