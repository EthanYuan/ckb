use crate::error::Error;
use crate::schemas::leap::Request;
use crate::utils::get_sighash_script_from_privkey;
use crate::Aggregator;

use ckb_logger::info;
use ckb_sdk::{
    rpc::ckb_indexer::{Cell, Order},
    traits::{CellQueryOptions, MaturityOption, PrimaryScriptType, QueryOrder},
};
use ckb_types::{packed::OutPoint, H256};

impl Aggregator {
    pub(crate) fn create_leap_tx(
        &self,
        requests: Vec<Request>,
        queue_cell: OutPoint,
    ) -> Result<H256, Error> {
        // Check if the requests of the last leap tx are duplicated, and if so, return immediately.
        let token_manager_cell = self.get_token_manager_cell()?;
        info!("Token Manager Cell: {:?}", token_manager_cell.out_point);

        Ok(H256::default())
    }

    fn get_token_manager_cell(&self) -> Result<Cell, Error> {
        info!("Search Token Manager Cell ...");

        let token_manager_cell_search_option = self.build_token_manager_cell_search_option()?;
        let token_manager_cell = self
            .branch_rpc_client
            .get_cells(
                token_manager_cell_search_option.into(),
                Order::Asc,
                1.into(),
                None,
            )
            .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;
        if token_manager_cell.objects.len() != 1 {
            return Err(Error::LiveCellNotFound(format!(
                "Token Manager cell found: {}",
                token_manager_cell.objects.len()
            )));
        }
        info!(
            "Found {} Token Manager cell",
            token_manager_cell.objects.len()
        );
        let token_manager_cell = token_manager_cell.objects[0].clone();

        Ok(token_manager_cell)
    }

    fn build_token_manager_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let (token_manager_lock, _) = get_sighash_script_from_privkey(
            self.config.branch_chain_token_manager_lock_key_path.clone(),
        )?;

        let cell_query_option = CellQueryOptions {
            primary_script: token_manager_lock,
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

    // fn get_rgbpp_queue_cell(&self) -> Result<(Cell, CrossChainQueue), Error> {
    //     info!("Search Branch Chain Message Queue ...");

    //     let queue_cell_search_option = self.build_message_queue_cell_search_option()?;
    //     let queue_cell = self
    //         .rgbpp_rpc_client
    //         .get_cells(queue_cell_search_option.into(), Order::Asc, 1.into(), None)
    //         .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;
    //     if queue_cell.objects.len() != 1 {
    //         return Err(Error::LiveCellNotFound(format!(
    //             "Queue cell found: {}",
    //             queue_cell.objects.len()
    //         )));
    //     }
    //     info!("Found {} queue cell", queue_cell.objects.len());
    //     let queue_cell = queue_cell.objects[0].clone();

    //     let queue_live_cell: LiveCell = queue_cell.clone().into();
    //     let queue_data = queue_live_cell.output_data;
    //     let queue = CrossChainQueue::from_slice(&queue_data)
    //         .map_err(|e| Error::QueueCellDataDecodeError(e.to_string()))?;

    //     Ok((queue_cell, queue))
    // }
}
