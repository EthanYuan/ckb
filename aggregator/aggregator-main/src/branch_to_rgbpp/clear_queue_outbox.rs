use crate::schemas::leap::{Request, Requests};
use crate::Aggregator;
use crate::SIGHASH_TYPE_HASH;

use aggregator_common::{
    error::Error,
    utils::{privkey::get_sighash_lock_args_from_privkey, SECP256K1},
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::{debug, info};
use ckb_sdk::{
    core::TransactionBuilder,
    transaction::{
        builder::{ChangeBuilder, DefaultChangeBuilder},
        handler::HandlerContexts,
        input::{InputIterator, TransactionInput},
        signer::{SignContexts, TransactionSigner},
        TransactionBuilderConfiguration,
    },
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup,
};
use ckb_types::{
    core::ScriptHashType,
    packed::{Byte32, Bytes as PackedBytes, CellInput, Script, WitnessArgs},
    prelude::*,
    H256,
};

use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

impl Aggregator {
    pub(crate) fn check_storage(&self) -> Result<(), Error> {
        if let Some(tx_hash) = self.store.get_staged_tx()? {
            let hash = H256::from_slice(tx_hash.as_bytes())
                .expect("Failed to convert staged transaction hash to H256");
            let tx = self
                .branch_rpc_client
                .get_transaction(hash.clone())
                .map_err(|e| Error::RpcError(format!("Failed to get transaction: {}", e)))?;
            match tx {
                Some(tx) => {
                    let height = tx.tx_status.block_number;
                    match height {
                        Some(height) => {
                            if self.store.is_tx_stored(height.into(), tx_hash.clone())? {
                                self.store.clear_staged_tx()?;
                            } else {
                                self.store.insert_branch_request(height.into(), tx_hash)?;
                                self.store.clear_staged_tx()?;
                            }
                        }
                        None => {
                            let height = self.wait_for_transaction_packing(hash)?;
                            self.store.insert_branch_request(height.into(), tx_hash)?;
                            self.store.clear_staged_tx()?;
                        }
                    }
                }
                None => {
                    // Theoretically, this situation does not exist
                    self.store.clear_staged_tx()?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn create_clear_queue_outbox_tx(
        &self,
        requests: Vec<Request>,
    ) -> Result<H256, Error> {
        let (queue_cell, queue_cell_data) = self.get_branch_queue_outbox_cell()?;
        info!(
            "The Branch queue outbox contains {} items that need to be cleared.",
            queue_cell_data.outbox().len()
        );
        if queue_cell_data.outbox().is_empty() {
            return Ok(H256::default());
        }

        // build new queue
        let request_ids = vec![];
        let existing_outbox = queue_cell_data
            .outbox()
            .as_builder()
            .set(request_ids)
            .build();
        let queue_data = queue_cell_data.as_builder().outbox(existing_outbox).build();
        let queue_witness = Requests::new_builder().set(requests).build();

        // build inputs
        let inputs: Vec<CellInput> = std::iter::once(queue_cell.out_point.clone())
            .map(|out_point| {
                CellInput::new_builder()
                    .previous_output(out_point.into())
                    .build()
            })
            .collect();

        // build outputs
        let outputs = vec![queue_cell.output.clone().into()];
        let outputs_data = vec![queue_data.as_bytes().pack()];

        // cell deps
        let secp256k1_cell_dep = self.get_branch_cell_dep(SECP256K1)?;

        // build transaction
        let mut tx_builder = TransactionBuilder::default();
        tx_builder
            .cell_deps(vec![secp256k1_cell_dep])
            .inputs(inputs)
            .outputs(outputs)
            .outputs_data(outputs_data)
            .witness(
                WitnessArgs::new_builder()
                    .input_type(Some(queue_witness.as_bytes()).pack())
                    .build()
                    .as_bytes()
                    .pack(),
            );

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
            let network_info =
                NetworkInfo::new(NetworkType::Testnet, self.config.branch_uri.clone());
            let fee_rate = self.fee_rate()?;
            let configuration = {
                let mut config =
                    TransactionBuilderConfiguration::new_with_network(network_info.clone())
                        .map_err(|e| Error::TransactionBuildError(e.to_string()))?;
                config.fee_rate = fee_rate;
                config
            };
            let (capacity_provider_script_args, capacity_provider_key) =
                get_sighash_lock_args_from_privkey(
                    self.config.branch_chain_capacity_provider_key_path.clone(),
                )?;
            let capacity_provider_script = Script::new_builder()
                .code_hash(SIGHASH_TYPE_HASH.pack())
                .hash_type(ScriptHashType::Type.into())
                .args(capacity_provider_script_args.pack())
                .build();
            let mut change_builder = DefaultChangeBuilder::new(
                &configuration,
                capacity_provider_script.clone(),
                Vec::new(),
            );
            change_builder.init(&mut tx_builder);
            {
                let queue_cell_input = TransactionInput {
                    live_cell: queue_cell.clone().into(),
                    since: 0,
                };
                let _ = change_builder.check_balance(queue_cell_input, &mut tx_builder);
            };
            let contexts = HandlerContexts::default();
            let iterator = InputIterator::new(vec![capacity_provider_script], &network_info);
            let mut tx_with_groups = {
                let mut check_result = None;
                for (mut input_index, input) in iterator.enumerate() {
                    input_index += 1;
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

                        check_result =
                            Some(TransactionWithScriptGroups::new(tx_view, script_groups));
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
            let (_, message_queue_key) = get_sighash_lock_args_from_privkey(
                self.config
                    .branch_chain_token_manager_outbox_lock_key_path
                    .clone(),
            )?;
            TransactionSigner::new(&network_info)
                .sign_transaction(
                    &mut tx_with_groups,
                    &SignContexts::new_sighash(vec![message_queue_key, capacity_provider_key]),
                )
                .map_err(|e| Error::TransactionSignError(e.to_string()))?;

            // send tx
            let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
            debug!(
                "clear queue tx: {}",
                serde_json::to_string_pretty(&tx_json).unwrap()
            );
            let tx_hash = self
                .rgbpp_rpc_client
                .send_transaction(tx_json.inner, None)
                .map_err(|e| {
                    Error::TransactionSendError(format!("send transaction error: {}", e))
                })?;
            info!("clear branch queue outbox tx send: {:?}", tx_hash.pack());

            Ok(tx_hash)
        }
    }

    fn wait_for_transaction_packing(&self, tx_hash: H256) -> Result<u64, Error> {
        loop {
            let tx = self
                .branch_rpc_client
                .get_transaction(
                    H256::from_slice(tx_hash.as_bytes())
                        .expect("Failed to convert staged transaction hash to H256"),
                )
                .map_err(|e| Error::RpcError(format!("Failed to get transaction: {}", e)))?;

            if let Some(tx) = tx {
                if let Some(height) = tx.tx_status.block_number {
                    // Transaction has been included in a block, return the height
                    return Ok(height.into());
                } else {
                    // Transaction is pending, log and wait
                    info!("Transaction is pending, waiting for block...");
                }
            }

            // Wait before next retry
            sleep(Duration::from_secs(1));
        }
    }
}
