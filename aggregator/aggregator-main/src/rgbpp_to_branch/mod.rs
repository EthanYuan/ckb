mod leap;

use crate::schemas::leap::Request;
use crate::wait_for_tx_confirmation;
use crate::Aggregator;

use aggregator_common::error::Error;
use ckb_channel::Receiver;
use ckb_logger::{error, info};
use ckb_types::{
    packed::{CellDep, OutPoint},
    prelude::*,
    H256,
};

use std::thread;
use std::time::Duration;

impl Aggregator {
    /// Collect RGB++ requests and send them to the branch chain
    pub fn poll_rgbpp_requests(&self, stop_rx: Receiver<()>) {
        info!("RGB++ Aggregator service started ...");

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

            // get RGB++ queue outbox data
            let rgbpp_requests = poll_service
                .rgbpp_tx_builder
                .get_rgbpp_queue_outbox_requests();
            let (rgbpp_requests, queue_cell) = match rgbpp_requests {
                Ok((rgbpp_requests, queue_cell)) => {
                    let rgbpp_requests: Vec<_> = rgbpp_requests
                        .into_iter()
                        .map(|r| Request::new_unchecked(r.as_bytes()))
                        .collect();
                    let queue_cell = OutPoint::new_unchecked(queue_cell.as_bytes());
                    (rgbpp_requests, queue_cell)
                }
                Err(e) => {
                    error!("get RGB++ queue data error: {}", e.to_string());
                    continue;
                }
            };

            // create leap transaction
            let leap_tx = poll_service.create_leap_tx(rgbpp_requests.clone(), queue_cell);
            let leap_tx = match leap_tx {
                Ok(leap_tx) => leap_tx,
                Err(e) => {
                    error!("create leap transaction error: {}", e.to_string());
                    continue;
                }
            };
            match wait_for_tx_confirmation(
                poll_service.branch_rpc_client.clone(),
                leap_tx,
                Duration::from_secs(600),
            ) {
                Ok(_) => {}
                Err(e) => error!("{}", e.to_string()),
            }

            // clear queue
            if !rgbpp_requests.is_empty() {
                let update_queue_tx = poll_service.rgbpp_tx_builder.create_clear_queue_tx();
                let update_queue_tx = match update_queue_tx {
                    Ok(update_queue_tx) => update_queue_tx,
                    Err(e) => {
                        error!("{}", e.to_string());
                        continue;
                    }
                };
                match wait_for_tx_confirmation(
                    poll_service.rgbpp_rpc_client.clone(),
                    H256(update_queue_tx.0),
                    Duration::from_secs(600),
                ) {
                    Ok(_) => {}
                    Err(e) => error!("{}", e.to_string()),
                }
            }

            if let Err(e) = poll_service.rgbpp_tx_builder.collect_rgbpp_requests() {
                info!("Aggregator collect RGB++ requests: {:?}", e);
            }

            thread::sleep(poll_interval);
        }
    }

    pub(crate) fn get_branch_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self.branch_scripts
            .get(script_name)
            .map(|script_info| script_info.cell_dep.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }
}
