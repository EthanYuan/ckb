use branch_chain_aggregator::Aggregator;
use ckb_app_config::{AggregatorArgs, ExitCode};
use ckb_logger::info;
use ckb_stop_handler::{broadcast_exit_signals, wait_all_ckb_services_exit};

use std::time::Duration;

pub fn aggregator(args: AggregatorArgs, chain_id: String) -> Result<(), ExitCode> {
    info!("Starting aggregator...");
    let aggregator = Aggregator::new(args.config, Duration::from_secs(2), chain_id);
    aggregator.run();

    info!("Branch Aggregator service started ...");
    ctrlc::set_handler(|| {
        info!("Trapped exit signal, exiting...");
        broadcast_exit_signals();
    })
    .expect("Error setting Ctrl-C handler");

    wait_all_ckb_services_exit();

    Ok(())
}
