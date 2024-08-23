mod leap;

use crate::schemas::leap::Request;
use crate::wait_for_tx_confirmation;
use crate::Aggregator;

use aggregator_common::error::Error;
use ckb_channel::Receiver;
use ckb_logger::{error, info, warn};
use ckb_types::{
    core::FeeRate,
    h256,
    packed::{CellDep, OutPoint, Script},
    prelude::*,
    H256,
};

use std::thread::{self};
use std::time::Duration;

pub const SIGHASH_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");
const CKB_FEE_RATE_LIMIT: u64 = 5000;

impl Aggregator {
    /// Collect RGB++ requests and send them to the branch chain
    pub fn poll_rgbpp_requests(&self, stop_rx: Receiver<()>) {
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

            // get queue data
            let rgbpp_requests = poll_service.rgbpp_tx_builder.get_rgbpp_queue_requests();
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
                Ok(()) => {}
                Err(e) => error!("{}", e.to_string()),
            }

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
                    Ok(()) => {}
                    Err(e) => error!("{}", e.to_string()),
                }
            }

            if let Err(e) = poll_service.rgbpp_tx_builder.collect_rgbpp_request() {
                info!("Aggregator: {:?}", e);
            }

            thread::sleep(poll_interval);
        }
    }

    fn fee_rate(&self) -> Result<u64, Error> {
        let value = {
            let dynamic = self
                .branch_rpc_client
                .get_fee_rate_statistics(None)
                .map_err(|e| Error::RpcError(format!("get dynamic fee rate error: {}", e)))?
                .ok_or_else(|| Error::RpcError("get dynamic fee rate error: None".to_string()))
                .map(|resp| resp.median)
                .map(Into::into)
                .map_err(|e| Error::RpcError(format!("get dynamic fee rate error: {}", e)))?;
            info!("CKB fee rate: {} (dynamic)", FeeRate(dynamic));
            if dynamic > CKB_FEE_RATE_LIMIT {
                warn!(
                    "dynamic CKB fee rate {} is too large, it seems unreasonable;\
            so the upper limit {} will be used",
                    FeeRate(dynamic),
                    FeeRate(CKB_FEE_RATE_LIMIT)
                );
                CKB_FEE_RATE_LIMIT
            } else {
                dynamic
            }
        };
        Ok(value)
    }

    fn get_branch_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self.branch_scripts
            .get(script_name)
            .map(|script_info| script_info.cell_dep.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    fn _get_branch_script(&self, script_name: &str) -> Result<Script, Error> {
        self.branch_scripts
            .get(script_name)
            .map(|script_info| script_info.script.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }
}
