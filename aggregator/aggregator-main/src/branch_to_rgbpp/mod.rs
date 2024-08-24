use crate::Aggregator;

use ckb_channel::Receiver;
use ckb_logger::info;

use std::thread;

impl Aggregator {
    /// Collect Branch requests and send them to the RGB++ chain
    pub fn poll_branch_requests(&self, stop_rx: Receiver<()>) {
        info!("Branch Aggregator service started ...");

        let poll_interval = self.poll_interval;
        let _poll_service: Aggregator = self.clone();

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

            thread::sleep(poll_interval);
        }
    }
}
