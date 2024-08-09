use crate::error::Error;
use crate::schemas::leap::Request;
use crate::utils::{get_sighash_script_from_privkey, QUEUE_TYPE, SECP256K1};
use crate::Aggregator;

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
    packed::{Byte32, Bytes as PackedBytes, CellInput, OutPoint, Script},
    prelude::*,
    H256,
};
use molecule::prelude::Entity;

use std::collections::HashMap;

impl Aggregator {
    pub(crate) fn create_clear_queue_tx(
        &self,
        _rgbpp_queue_cells: Vec<Request>,
        _queue_cell: OutPoint,
    ) -> Result<H256, Error> {
        // get queue cell
        let (queue_cell, queue_cell_data) = self.get_rgbpp_queue_cell()?;

        // build new queue
        let request_ids = vec![];
        let existing_outbox = queue_cell_data
            .outbox()
            .as_builder()
            .set(request_ids)
            .build();
        let queue_data = queue_cell_data.as_builder().outbox(existing_outbox).build();

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
        let secp256k1_cell_dep = self.get_rgbpp_cell_dep(SECP256K1)?;
        let queue_type_cell_dep = self.get_rgbpp_cell_dep(QUEUE_TYPE)?;

        // build transaction
        let mut tx_builder = TransactionBuilder::default();
        tx_builder
            .cell_deps(vec![secp256k1_cell_dep, queue_type_cell_dep])
            .inputs(inputs)
            .outputs(outputs)
            .outputs_data(outputs_data)
            .witness(PackedBytes::default());

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
                NetworkInfo::new(NetworkType::Testnet, self.config.rgbpp_uri.clone());
            let fee_rate = self.fee_rate()?;
            let configuration = {
                let mut config =
                    TransactionBuilderConfiguration::new_with_network(network_info.clone())
                        .map_err(|e| Error::TransactionBuildError(e.to_string()))?;
                config.fee_rate = fee_rate;
                config
            };
            let (capacity_provider_script, capacity_provider_key) =
                get_sighash_script_from_privkey(self.config.rgbpp_ckb_provider_key_path.clone())?;
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
            let (_, message_queue_key) =
                get_sighash_script_from_privkey(self.config.rgbpp_queue_lock_key_path.clone())?;
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
            info!("clear queue tx send: {:?}", tx_hash.pack());

            Ok(tx_hash)
        }
    }
}
