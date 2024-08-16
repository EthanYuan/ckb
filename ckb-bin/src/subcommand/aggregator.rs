use ckb_app_config::{AggregatorArgs, ExitCode};
use ckb_async_runtime::Handle;
use ckb_logger::info;

pub fn aggregator(_args: AggregatorArgs, _async_handle: Handle) -> Result<(), ExitCode> {
    info!("Starting aggregator...");
    Ok(())
}
