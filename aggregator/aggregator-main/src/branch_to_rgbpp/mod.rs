use crate::Aggregator;

use ckb_channel::Receiver;
use ckb_logger::info;

use std::thread;

impl Aggregator {
    /// Collect Branch requests and send them to the RGB++ chain
    pub fn poll_branch_requests(&self, _stop_rx: Receiver<()>) {
        info!("Branch Aggregator service started ...");

        let poll_interval = self.poll_interval;
        thread::sleep(poll_interval);
    }
}
