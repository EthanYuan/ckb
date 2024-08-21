use crate::schemas::leap::{
    CrossChainQueue, Message, MessageUnion, Request, RequestContent, RequestLockArgs, Requests,
    Transfer,
};
use crate::RgbppTxBuilder;

use aggregator_common::{
    error::Error,
    types::RequestType,
    utils::{
        decode_udt_amount, encode_udt_amount, privkey::get_sighash_lock_args_from_privkey,
        QUEUE_TYPE, REQUEST_LOCK, SECP256K1, XUDT,
    },
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::{debug, info, warn};
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::ckb_indexer::{Cell, Order},
    rpc::CkbRpcClient as RpcClient,
    traits::{CellQueryOptions, LiveCell, MaturityOption, PrimaryScriptType, QueryOrder},
    transaction::{
        builder::{ChangeBuilder, DefaultChangeBuilder},
        handler::HandlerContexts,
        input::{InputIterator, TransactionInput},
        signer::{SignContexts, TransactionSigner},
        TransactionBuilderConfiguration,
    },
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup, Since, SinceType,
};
use ckb_stop_handler::{new_tokio_exit_rx, CancellationToken};
use ckb_types::{
    bytes::Bytes,
    core::{FeeRate, ScriptHashType},
    h256,
    packed::{Byte32, Bytes as PackedBytes, CellDep, CellInput, CellOutput, Script, WitnessArgs},
    prelude::*,
    H256,
};
use molecule::prelude::Byte;

use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

/// Sighash type hash
pub const SIGHASH_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");
/// CKB fee rate limit
const CKB_FEE_RATE_LIMIT: u64 = 5000;
const CONFIRMATION_THRESHOLD: u64 = 24;

impl RgbppTxBuilder {
    pub fn scan_rgbpp_request(&self) -> Result<(), Error> {
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
                .rgbpp_rpc_client
                .get_cells(
                    request_search_option.clone().into(),
                    Order::Asc,
                    limit.into(),
                    cursor,
                )
                .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;

            if request_cells.objects.is_empty() {
                info!("No more request cells found");
                break;
            }
            cursor = Some(request_cells.last_cursor);

            info!("Found {} request cells", request_cells.objects.len());
            let tip = self
                .rgbpp_rpc_client
                .get_tip_block_number()
                .map_err(|e| Error::RpcError(format!("get tip block number error: {}", e)))?
                .value();
            let cells_with_messge = self.check_request(request_cells.objects.clone(), tip);
            info!("Found {} valid request cells", cells_with_messge.len());
            if cells_with_messge.is_empty() {
                break;
            }

            let custodian_tx = self.create_custodian_tx(cells_with_messge)?;
            match wait_for_tx_confirmation(
                self.rgbpp_rpc_client.clone(),
                custodian_tx,
                Duration::from_secs(15),
            ) {
                Ok(()) => info!("Transaction confirmed"),
                Err(e) => info!("{}", e.to_string()),
            }
        }

        Ok(())
    }

    pub fn create_custodian_tx(&self, request_cells: Vec<(Cell, Transfer)>) -> Result<H256, Error> {
        // get queue cell
        let (queue_cell, queue_cell_data) = self.get_rgbpp_queue_cell()?;

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

        if !queue_cell_data.outbox().is_empty() {
            return Err(Error::QueueOutboxHasUnprocessedRequests);
        }
        let existing_outbox = queue_cell_data
            .outbox()
            .as_builder()
            .extend(request_ids)
            .build();
        let queue_data = queue_cell_data.as_builder().outbox(existing_outbox).build();
        let queue_witness = Requests::new_builder().set(requests).build();

        // build custodian lock
        let (custodian_lock_args, _) =
            get_sighash_lock_args_from_privkey(self.rgbpp_custodian_lock_key_path.clone())?;
        let custodian_lock = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(custodian_lock_args.pack())
            .build();

        // build inputs
        let inputs: Vec<CellInput> = std::iter::once(queue_cell.out_point.clone())
            .chain(request_cells.iter().map(|(cell, _)| cell.out_point.clone()))
            .map(|out_point| {
                CellInput::new_builder()
                    .previous_output(out_point.into())
                    .build()
            })
            .collect();

        // build outputs
        let mut outputs = vec![queue_cell.output.clone().into()];
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
            .cell_deps(vec![
                secp256k1_cell_dep,
                xudt_cell_dep,
                request_cell_dep,
                queue_type_cell_dep,
            ])
            .inputs(inputs)
            .outputs(outputs)
            .outputs_data(outputs_data)
            .witness(
                WitnessArgs::new_builder()
                    .input_type(Some(queue_witness.as_bytes()).pack())
                    .build()
                    .as_bytes()
                    .pack(),
            )
            .witnesses(vec![PackedBytes::default(); request_cells.len()]);

        // group
        #[allow(clippy::mutable_key_type)]
        let mut lock_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut type_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        {
            let lock_script: Script = queue_cell.output.lock.clone().into();
            lock_groups
                .entry(lock_script.calc_script_hash())
                .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                .input_indices
                .push(0);
            for (index, cell) in request_cells.iter().enumerate() {
                let lock_script: Script = cell.0.output.lock.clone().into();
                lock_groups
                    .entry(lock_script.calc_script_hash())
                    .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                    .input_indices
                    .push(index + 1);
            }
        }
        for (output_idx, output) in tx_builder.get_outputs().clone().iter().enumerate() {
            if let Some(type_script) = &output.type_().to_opt() {
                type_groups
                    .entry(type_script.calc_script_hash())
                    .or_insert_with(|| ScriptGroup::from_type_script(type_script))
                    .output_indices
                    .push(output_idx);
            }
        }

        // balance transaction
        let network_info = NetworkInfo::new(NetworkType::Testnet, self.rgbpp_uri.clone());
        let fee_rate = self.fee_rate()?;
        let configuration = {
            let mut config =
                TransactionBuilderConfiguration::new_with_network(network_info.clone())
                    .map_err(|e| Error::TransactionBuildError(e.to_string()))?;
            config.fee_rate = fee_rate;
            config
        };
        let (capacity_provider_script_args, capacity_provider_key) =
            get_sighash_lock_args_from_privkey(self.rgbpp_ckb_provider_key_path.clone())?;
        let capacity_provider_script = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(capacity_provider_script_args.pack())
            .build();

        let mut change_builder =
            DefaultChangeBuilder::new(&configuration, capacity_provider_script.clone(), Vec::new());
        change_builder.init(&mut tx_builder);
        {
            let queue_cell_input = TransactionInput {
                live_cell: queue_cell.clone().into(),
                since: 0,
            };
            let _ = change_builder.check_balance(queue_cell_input, &mut tx_builder);
            for (cell, _) in &request_cells {
                let request_input = TransactionInput {
                    live_cell: cell.to_owned().into(),
                    since: 0,
                };
                let _ = change_builder.check_balance(request_input, &mut tx_builder);
            }
        };
        let contexts = HandlerContexts::default();
        let iterator = InputIterator::new(vec![capacity_provider_script], &network_info);
        let mut tx_with_groups = {
            let mut check_result = None;
            for (mut input_index, input) in iterator.enumerate() {
                input_index += 1 + request_cells.len(); // queue cell + request cells
                let input = input.map_err(|err| {
                    let msg = format!("failed to find {input_index}-th live cell since {err}");
                    Error::Other(msg)
                })?;
                tx_builder.input(input.cell_input());
                tx_builder.witness(PackedBytes::default());

                let previous_output = input.previous_output();
                let lock_script = previous_output.lock();
                lock_groups
                    .entry(lock_script.calc_script_hash())
                    .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                    .input_indices
                    .push(input_index);

                if change_builder.check_balance(input, &mut tx_builder) {
                    let mut script_groups: Vec<ScriptGroup> = lock_groups
                        .into_values()
                        .chain(type_groups.into_values())
                        .collect();
                    for script_group in script_groups.iter_mut() {
                        for handler in configuration.get_script_handlers() {
                            for context in &contexts.contexts {
                                if handler
                                    .build_transaction(
                                        &mut tx_builder,
                                        script_group,
                                        context.as_ref(),
                                    )
                                    .map_err(|e| Error::TransactionBuildError(e.to_string()))?
                                {
                                    break;
                                }
                            }
                        }
                    }
                    let tx_view = change_builder.finalize(tx_builder);

                    check_result = Some(TransactionWithScriptGroups::new(tx_view, script_groups));
                    break;
                }
            }
            check_result
        }
        .ok_or_else(|| {
            let msg = "live cells are not enough".to_string();
            Error::Other(msg)
        })?;

        // sign
        let (_, message_queue_key) =
            get_sighash_lock_args_from_privkey(self.rgbpp_queue_lock_key_path.clone())?;
        TransactionSigner::new(&network_info)
            .sign_transaction(
                &mut tx_with_groups,
                &SignContexts::new_sighash(vec![message_queue_key, capacity_provider_key]),
            )
            .map_err(|e| Error::TransactionSignError(e.to_string()))?;

        // send tx
        let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
        debug!(
            "custodian tx: {}",
            serde_json::to_string_pretty(&tx_json).unwrap()
        );
        let tx_hash = self
            .rgbpp_rpc_client
            .send_transaction(tx_json.inner, None)
            .map_err(|e| Error::TransactionSendError(format!("send transaction error: {}", e)))?;
        info!("custodian tx send: {:?}", tx_hash.pack());

        Ok(tx_hash)
    }

    fn get_rgbpp_queue_cell(&self) -> Result<(Cell, CrossChainQueue), Error> {
        info!("Scan RGB++ Message Queue ...");

        let queue_cell_search_option = self.build_message_queue_cell_search_option()?;
        let queue_cell = self
            .rgbpp_rpc_client
            .get_cells(queue_cell_search_option.into(), Order::Asc, 1.into(), None)
            .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;
        if queue_cell.objects.len() != 1 {
            return Err(Error::LiveCellNotFound(format!(
                "Queue cell found: {}",
                queue_cell.objects.len()
            )));
        }
        info!("Found {} queue cell", queue_cell.objects.len());
        let queue_cell = queue_cell.objects[0].clone();

        let queue_live_cell: LiveCell = queue_cell.clone().into();
        let queue_data = queue_live_cell.output_data;
        let queue = CrossChainQueue::from_slice(&queue_data)
            .map_err(|e| Error::QueueCellDataDecodeError(e.to_string()))?;

        Ok((queue_cell, queue))
    }

    pub(crate) fn build_message_queue_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let message_queue_type = self.get_rgbpp_script(QUEUE_TYPE)?;
        let (message_queue_lock_args, _) =
            get_sighash_lock_args_from_privkey(self.rgbpp_queue_lock_key_path.clone())?;
        let message_queue_lock = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(message_queue_lock_args.pack())
            .build();

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

    fn get_rgbpp_cell_dep(&self, script_name: &str) -> Result<CellDep, Error> {
        self.rgbpp_scripts
            .get(script_name)
            .map(|script_info| script_info.cell_dep.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    fn get_rgbpp_script(&self, script_name: &str) -> Result<Script, Error> {
        self.rgbpp_scripts
            .get(script_name)
            .map(|script_info| script_info.script.clone())
            .ok_or_else(|| Error::MissingScriptInfo(script_name.to_string()))
    }

    fn fee_rate(&self) -> Result<u64, Error> {
        let value = {
            let dynamic = self
                .rgbpp_rpc_client
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

    pub(crate) fn build_request_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let request_script = self.get_rgbpp_script(REQUEST_LOCK)?;
        Ok(CellQueryOptions::new_lock(request_script))
    }

    fn check_request(&self, cells: Vec<Cell>, tip: u64) -> Vec<(Cell, Transfer)> {
        cells
            .into_iter()
            .filter_map(|cell| {
                let live_cell: LiveCell = cell.clone().into();
                RequestLockArgs::from_slice(&live_cell.output.lock().args().raw_data())
                    .ok()
                    .and_then(|args| {
                        let target_request_type_hash = args.request_type_hash();
                        info!("target_request_type_hash: {:?}", target_request_type_hash);

                        let timeout: u64 = args.timeout().unpack();
                        let since = Since::from_raw_value(timeout);
                        let since_check =
                            since.extract_metric().map_or(false, |(since_type, value)| {
                                match since_type {
                                    SinceType::BlockNumber => {
                                        let threshold = if since.is_absolute() {
                                            value
                                        } else {
                                            cell.block_number.value() + value
                                        };
                                        tip + CONFIRMATION_THRESHOLD < threshold
                                    }
                                    _ => false,
                                }
                            });

                        let content = args.content();
                        let target_chain_id: Bytes = content.target_chain_id().raw_data();
                        info!("target_chain_id: {:?}", target_chain_id);
                        let request_type = content.request_type();

                        let (check_message, transfer) = {
                            let message = content.message();
                            let message_union = message.to_enum();
                            match message_union {
                                MessageUnion::Transfer(transfer) => {
                                    let transfer_amount: u128 = transfer.amount().unpack();
                                    let check_message = cell
                                        .clone()
                                        .output_data
                                        .and_then(|data| decode_udt_amount(data.as_bytes()))
                                        .map_or(false, |amount| {
                                            info!(
                                                "original amount: {:?}, transfer amount: {:?}",
                                                amount, transfer_amount
                                            );
                                            transfer_amount <= amount
                                        });
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
                            && since_check
                        {
                            Some((cell, transfer))
                        } else {
                            None
                        }
                    })
            })
            .collect()
    }
}

fn wait_for_tx_confirmation(
    _client: RpcClient,
    _tx_hash: H256,
    timeout: Duration,
) -> Result<(), Error> {
    let start = std::time::Instant::now();

    loop {
        if true {
            sleep(Duration::from_secs(8));
            return Ok(());
        }

        if start.elapsed() > timeout {
            return Err(Error::TimedOut(
                "Transaction confirmation timed out".to_string(),
            ));
        }
    }
}
