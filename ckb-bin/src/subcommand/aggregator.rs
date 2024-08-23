use aggregator_main::Aggregator;
use ckb_app_config::{AggregatorArgs, ExitCode};
use ckb_logger::info;
use ckb_stop_handler::{
    broadcast_exit_signals, new_crossbeam_exit_rx, register_thread, wait_all_ckb_services_exit,
};

use std::thread;
use std::time::Duration;

pub fn aggregator(args: AggregatorArgs, chain_id: String) -> Result<(), ExitCode> {
    info!("chain id: {}", chain_id);

    let aggregator = Aggregator::new(args.config, Duration::from_secs(2), chain_id);

    let stop_rx_rgbpp = new_crossbeam_exit_rx();
    let stop_rx_branch = new_crossbeam_exit_rx();

    const THREAD_NAME_RGBPP: &str = "Aggregator_RGBPP";
    let aggregator_rgbpp = aggregator.clone();
    let aggregator_rgbpp_jh = thread::Builder::new()
        .name(THREAD_NAME_RGBPP.into())
        .spawn(move || {
            aggregator_rgbpp.poll_rgbpp_requests(stop_rx_rgbpp);
        })
        .expect("Start aggregator failed!");
    register_thread(THREAD_NAME_RGBPP, aggregator_rgbpp_jh);

    const THREAD_NAME_BRANCH: &str = "Aggregator_Branch";
    let aggregator_branch = aggregator.clone();
    let aggregator_branch_jh = thread::Builder::new()
        .name(THREAD_NAME_BRANCH.into())
        .spawn({
            move || {
                aggregator_branch.poll_branch_requests(stop_rx_branch);
            }
        })
        .expect("Start Branch aggregator failed!");
    register_thread(THREAD_NAME_BRANCH, aggregator_branch_jh);

    ctrlc::set_handler(|| {
        info!("Trapped exit signal, exiting...");
        broadcast_exit_signals();
    })
    .expect("Error setting Ctrl-C handler");

    wait_all_ckb_services_exit();

    Ok(())
}
