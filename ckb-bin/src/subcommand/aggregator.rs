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

    let stop_rx = new_crossbeam_exit_rx();
    const THREAD_NAME: &str = "Aggregator";
    let aggregator_jh = thread::Builder::new()
        .name(THREAD_NAME.into())
        .spawn(move || {
            aggregator.run(stop_rx);
        })
        .expect("Start aggregator failed!");
    register_thread(THREAD_NAME, aggregator_jh);

    info!("Branch Aggregator service started ...");
    ctrlc::set_handler(|| {
        info!("Trapped exit signal, exiting...");
        broadcast_exit_signals();
    })
    .expect("Error setting Ctrl-C handler");

    wait_all_ckb_services_exit();

    Ok(())
}
