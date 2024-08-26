use crate::schemas::leap::{Request, Requests};
use crate::Aggregator;

use aggregator_common::{
    error::Error,
    utils::{privkey::get_sighash_lock_args_from_privkey, SECP256K1, TOKEN_MANAGER_TYPE},
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::info;
use ckb_sdk::{
    core::TransactionBuilder,
    transaction::signer::{SignContexts, TransactionSigner},
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup,
};
use ckb_types::{
    packed::{Byte32, CellInput, Script, WitnessArgs},
    prelude::*,
    H256,
};

use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

impl Aggregator {
    pub(crate) fn check_storage(&self) -> Result<(), Error> {
        if let Some(tx_hash) = self.store.get_staged_tx()? {
            let tx = self
                .branch_rpc_client
                .get_transaction(tx_hash.clone())
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
                            let height = self.wait_for_transaction_packing(tx_hash.clone())?;
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
        let queue_type_cell_dep = self.get_branch_cell_dep(TOKEN_MANAGER_TYPE)?;

        // build transaction
        let mut tx_builder = TransactionBuilder::default();
        tx_builder
            .cell_deps(vec![secp256k1_cell_dep, queue_type_cell_dep])
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
            let script_groups: Vec<ScriptGroup> = lock_groups
                .into_values()
                .chain(type_groups.into_values())
                .collect();
            let tx_view = tx_builder.build();
            let mut tx_with_groups = TransactionWithScriptGroups::new(tx_view, script_groups);

            // sign
            let (_, message_queue_key) = get_sighash_lock_args_from_privkey(
                self.config
                    .branch_chain_token_manager_outbox_lock_key_path
                    .clone(),
            )?;
            TransactionSigner::new(&network_info)
                .sign_transaction(
                    &mut tx_with_groups,
                    &SignContexts::new_sighash(vec![message_queue_key]),
                )
                .map_err(|e| Error::TransactionSignError(e.to_string()))?;

            // send tx
            let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
            info!(
                "clear queue tx: {}",
                serde_json::to_string_pretty(&tx_json).unwrap()
            );
            // record staged tx
            self.store.record_staged_tx(tx_json.hash)?;

            let tx_hash = self
                .branch_rpc_client
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
