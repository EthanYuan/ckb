//! Branch Chain Aggregator

pub(crate) mod client;
pub(crate) mod error;
pub(crate) mod schemas;
pub(crate) mod utils;

use crate::client::RpcClient;
use crate::error::Error;
use crate::utils::{
    decode_udt_amount, encode_udt_amount, get_sighash_script_from_privkey, REQUEST_LOCK, SECP256K1,
    XUDT,
};

use ckb_app_config::{AggregatorConfig, ScriptConfig};
use ckb_async_runtime::{
    tokio::{
        self,
        time::{self, interval, Duration},
    },
    Handle,
};
use ckb_logger::{error, info};
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::ckb_indexer::Cell,
    traits::{CellQueryOptions, MaturityOption, PrimaryScriptType, QueryOrder},
};
use ckb_stop_handler::{new_tokio_exit_rx, CancellationToken};
use ckb_types::{
    bytes::Bytes,
    packed::{CellDep, CellInput, CellOutput, Script},
    prelude::*,
};
use molecule::prelude::Byte;
use schemas::leap::{
    CrossChainQueue, Message, MessageUnion, Request, RequestContent, RequestLockArgs, Requests,
    Transfer,
};
use utils::QUEUE_TYPE;

use std::collections::HashMap;

///
#[derive(Clone)]
pub struct Aggregator {
    chain_id: String,
    config: AggregatorConfig,
    async_handle: Handle,
    poll_interval: Duration,
    rpc_client: RpcClient,
    rgbpp_scripts: HashMap<String, ScriptInfo>,
    _branch_scripts: HashMap<String, ScriptInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptInfo {
    pub script: Script,
    pub cell_dep: CellDep,
}

#[allow(dead_code)]
enum RequestType {
    CkbToBranch = 1,
    BranchToCkb = 2,
    BranchToBranch = 3,
}

impl Aggregator {
    /// Create an Aggregator
    pub fn new(
        config: AggregatorConfig,
        async_handle: Handle,
        poll_interval: Duration,
        chain_id: String,
    ) -> Self {
        let rpc_client = RpcClient::new(&config.rgbpp_uri);
        Aggregator {
            chain_id,
            config: config.clone(),
            async_handle,
            poll_interval,
            rpc_client,
            rgbpp_scripts: get_script_map(config.rgbpp_scripts.clone()),
            _branch_scripts: get_script_map(config.branch_scripts.clone()),
        }
    }

    /// Run the Aggregator
    pub fn run(&self) {
        info!("chain id: {}", self.chain_id);

        // Setup cancellation token
        let stop: CancellationToken = new_tokio_exit_rx();
        let poll_interval = self.poll_interval;
        let poll_service: Aggregator = self.clone();

        self.async_handle.spawn(async move {
            if stop.is_cancelled() {
                info!("Aggregator received exit signal, exit now");
                return;
            }

            let mut interval = interval(poll_interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let service = poll_service.clone();
                        if let Err(e) = service.scan_rgbpp_request().await {
                            error!("Aggregator Error: {:?}", e);
                        }
                    }
                    _ = stop.cancelled() => {
                        info!("Aggregator received exit signal, exiting now");
                        break;
                    },
                }
            }
        });
    }

    async fn scan_rgbpp_request(&self) -> Result<(), Error> {
        info!("Scan RGB++ Request ...");

        let stop: CancellationToken = new_tokio_exit_rx();

        let request_search_option = self.build_request_cell_search_option()?;
        let mut cursor = None;
        let limit = 10;

        loop {
            if stop.is_cancelled() {
                info!("Aggregator scan_rgbpp_request received exit signal, exiting now");
                return Ok(());
            }

            let request_cells = self
                .rpc_client
                .get_cells(request_search_option.clone().into(), limit, cursor)
                .await
                .map_err(|_| Error::FetchRequestCellsError)?;

            if request_cells.objects.is_empty() {
                info!("No more request cells found");
                break;
            }
            cursor = Some(request_cells.last_cursor);

            info!("Found {} request cells", request_cells.objects.len());
            let cells_with_messge = self.check_request(request_cells.objects.clone());
            info!("Found {} valid request cells", cells_with_messge.len());
            self.create_custodian_tx(cells_with_messge).await?;
            self.create_leap_tx();
            self.create_update_rgbpp_queue_tx();
        }

        Ok(())
    }

    fn build_request_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let request_script = self.get_rgbpp_script(REQUEST_LOCK)?;
        Ok(CellQueryOptions::new_lock(request_script))
    }

    fn build_message_queue_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let message_queue_type = self.get_rgbpp_script(QUEUE_TYPE)?;
        let message_queue_lock =
            get_sighash_script_from_privkey(self.config.rgbpp_queue_lock_key_path.clone())?;

        let cell_query_option = CellQueryOptions {
            primary_script: message_queue_type,
            primary_type: PrimaryScriptType::Type,
            with_data: Some(true),
            secondary_script: Some(message_queue_lock),
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

    fn check_request(&self, cells: Vec<Cell>) -> Vec<(Cell, Transfer)> {
        cells
            .into_iter()
            .filter_map(|cell| {
                RequestLockArgs::from_slice(cell.output.lock.args.as_bytes())
                    .ok()
                    .and_then(|args| {
                        let target_request_type_hash = args.request_type_hash();
                        let content = args.content();
                        let target_chain_id = content.target_chain_id().as_bytes();
                        let request_type = content.request_type();
                        let message = content.message();
                        let (check_message, transfer) = {
                            let message_union = message.to_enum();
                            match message_union {
                                MessageUnion::Transfer(transfer) => {
                                    let transfer_amount: u128 = transfer.amount().unpack();
                                    let check_message = cell
                                        .clone()
                                        .output_data
                                        .and_then(|data| decode_udt_amount(&data.as_bytes()))
                                        .map_or(false, |amount| transfer_amount <= amount);
                                    (check_message, transfer)
                                }
                            }
                        };
                        let request_type_hash = self
                            .rgbpp_scripts
                            .get(QUEUE_TYPE)
                            .map(|script_info| script_info.script.calc_script_hash());
                        if Some(target_request_type_hash) == request_type_hash
                            && self.chain_id.clone() == target_chain_id
                            && request_type == Byte::new(RequestType::CkbToBranch as u8)
                            && check_message
                        {
                            Some((cell, transfer))
                        } else {
                            None
                        }
                    })
            })
            .collect()
    }

    async fn create_custodian_tx(&self, request_cells: Vec<(Cell, Transfer)>) -> Result<(), Error> {
        // get queue cell
        let queue_cell_search_option = self.build_message_queue_cell_search_option()?;
        let queue_cell = self
            .rpc_client
            .get_cells(queue_cell_search_option.into(), 1, None)
            .await
            .map_err(|e| Error::FetchRgbppMessageQueueCellError(e.to_string()))?;
        if queue_cell.objects.len() != 1 {
            return Err(Error::QueueCellNotFound);
        }
        let queue_cell = queue_cell.objects[0].clone();

        // build new queue
        let mut request_ids = vec![];
        let mut requests = vec![];
        for (cell, transfer) in request_cells.clone() {
            let request_content = RequestContent::new_builder()
                .request_type(Byte::new(RequestType::CkbToBranch as u8))
                .target_chain_id(self.chain_id.pack())
                .message(
                    Message::new_builder()
                        .set(MessageUnion::Transfer(transfer))
                        .build(),
                )
                .build();
            let request = Request::new_builder()
                .request_cell(cell.out_point.into())
                .request_content(request_content)
                .build();
            let request_id = request.as_bytes().pack().calc_raw_data_hash();
            request_ids.push(request_id);
            requests.push(request);
        }
        let queue = CrossChainQueue::from_slice(
            &queue_cell
                .output_data
                .ok_or_else(|| Error::QueueCellDataDecodeError)?
                .as_bytes(),
        )
        .map_err(|_| Error::QueueCellDataDecodeError)?;
        let existing_outbox = queue.outbox().as_builder().extend(request_ids).build();
        let queue_data = queue.as_builder().outbox(existing_outbox).build();
        let queue_witness = Requests::new_builder().set(requests).build();

        // build custodian lock
        let custodian_lock =
            get_sighash_script_from_privkey(self.config.rgbpp_custodian_lock_key_path.clone())?;

        // build inputs
        let inputs: Vec<CellInput> = std::iter::once(queue_cell.out_point)
            .chain(request_cells.iter().map(|(cell, _)| cell.out_point.clone()))
            .map(|out_point| {
                CellInput::new_builder()
                    .previous_output(out_point.into())
                    .build()
            })
            .collect();

        // build outputs
        let mut outputs = vec![queue_cell.output.into()];
        let mut outputs_data = vec![queue_data.as_bytes().pack()];
        for (cell, transfer) in &request_cells {
            let output: CellOutput = cell.output.clone().into();
            let output = output.as_builder().lock(custodian_lock.clone()).build();
            outputs.push(output);

            let udt_amount: u128 = transfer.amount().unpack();
            outputs_data.push(Bytes::from(encode_udt_amount(udt_amount)).pack());
        }

        // cell deps
        let secp256k1_cell_dep = self.get_rgbpp_cell_dep(SECP256K1)?;
        let xudt_cell_dep = self.get_rgbpp_cell_dep(XUDT)?;
        let request_cell_dep = self.get_rgbpp_cell_dep(REQUEST_LOCK)?;
        let queue_type_cell_dep = self.get_rgbpp_cell_dep(QUEUE_TYPE)?;

        // build transaction
        let mut tx_builder = TransactionBuilder::default();
        tx_builder
            .inputs(inputs)
            .outputs(outputs)
            .outputs_data(outputs_data)
            .cell_deps(vec![
                secp256k1_cell_dep,
                xudt_cell_dep,
                request_cell_dep,
                queue_type_cell_dep,
            ])
            .witness(queue_witness.as_bytes().pack());

        // sign

        // send tx

        Ok(())
    }

    fn create_leap_tx(&self) {
        // TODO
    }

    fn create_update_rgbpp_queue_tx(&self) {
        // TODO
    }

    pub fn get_rgbpp_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self.rgbpp_scripts
            .get(script_name)
            .map(|script_info| script_info.cell_dep.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    pub fn get_branch_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self._branch_scripts
            .get(script_name)
            .map(|script_info| script_info.cell_dep.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    pub fn get_rgbpp_script(&self, script_name: &str) -> Result<Script, Error> {
        self.rgbpp_scripts
            .get(script_name)
            .map(|script_info| script_info.script.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    pub fn get_branch_script(&self, script_name: &str) -> Result<Script, Error> {
        self._branch_scripts
            .get(script_name)
            .map(|script_info| script_info.script.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }
}

/// Get scripts map.
pub fn get_script_map(scripts: Vec<ScriptConfig>) -> HashMap<String, ScriptInfo> {
    scripts
        .iter()
        .map(|s| {
            (
                s.script_name.clone(),
                ScriptInfo {
                    script: serde_json::from_str::<ckb_jsonrpc_types::Script>(&s.script)
                        .expect("config string to script")
                        .into(),
                    cell_dep: serde_json::from_str::<ckb_jsonrpc_types::CellDep>(&s.cell_dep)
                        .expect("config string to cell dep")
                        .into(),
                },
            )
        })
        .collect()
}
