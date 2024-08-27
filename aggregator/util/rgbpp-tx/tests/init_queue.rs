mod send_request;

use send_request::{get_rgbpp_cell_dep, get_sighash_lock_args_from_privkey, prepare_scripts};

use aggregator_rgbpp_tx::{
    leap::{self},
    SIGHASH_TYPE_HASH,
};
use ckb_jsonrpc_types::TransactionView;
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::CkbRpcClient as RpcClient,
    transaction::{
        builder::{ChangeBuilder, DefaultChangeBuilder},
        input::InputIterator,
        signer::{SignContexts, TransactionSigner},
        TransactionBuilderConfiguration,
    },
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup,
};
use ckb_types::{
    bytes::Bytes,
    core::ScriptHashType,
    h160, h256,
    packed::{Byte32, Bytes as PackedBytes, CellOutput, Script},
    prelude::*,
};

use std::collections::HashMap;

#[test]
#[ignore]
fn init_queue() {
    let ckb_provider_lock_privkey =
        h256!("0x6b726167f8d0b2b8722b9a3f0cfdc67d43f1622666a6fda6b32da76a3824e52e");
    let ckb_provider_lock_args = h160!("0xa0c73e84d827b87073a2a0e6448921870d114d02").as_bytes();
    let ckb_provider_lock = Script::new_builder()
        .code_hash(SIGHASH_TYPE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(Bytes::from(ckb_provider_lock_args).pack())
        .build();

    let rgbpp_uri = "https://testnet.ckb.dev";

    let _message_queue_type_id =
        h256!("0xd9911b00409a9f443ae7ed6b00d59dd1e33979e4c986478cf7863fcb7f62941b");
    let message_queue_code_hash =
        h256!("0x2da1e80cec3e553a76e22d826b63ce5f65d77622de48caa5a2fe724b0f9a18f2");

    // output
    let queue_lock = Script::new_builder()
        .code_hash(SIGHASH_TYPE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(
            h160!("0xe0e94ed5960af51263e51d6e080989fd11bf48f5")
                .as_bytes()
                .pack(),
        )
        .build();
    let args = Bytes::from(hex::decode("4242").unwrap());
    let queue_type_script = Script::new_builder()
        .code_hash(message_queue_code_hash.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(args.pack())
        .build();
    let cell = CellOutput::new_builder()
        .lock(queue_lock.clone())
        .type_(Some(queue_type_script).pack())
        .capacity(1000_0000_0000.pack())
        .build();
    let cell_data = {
        let queue = leap::CrossChainQueue::new_builder().build();
        queue.as_bytes()
    };

    // cell deps
    let scripts = prepare_scripts();
    let secp256k1_cell_dep = get_rgbpp_cell_dep("secp256k1_blake160", &scripts);
    let queue_type_cell_dep = get_rgbpp_cell_dep("queue_type", &scripts);

    // create transaction
    let mut tx_builder = TransactionBuilder::default();
    tx_builder
        .cell_deps(vec![secp256k1_cell_dep, queue_type_cell_dep])
        .output(cell)
        .output_data(cell_data.pack());

    // group
    let mut lock_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();

    // balance transaction
    let network_info = NetworkInfo::new(NetworkType::Testnet, rgbpp_uri.to_string());
    let fee_rate = 3000;
    let configuration = {
        let mut config =
            TransactionBuilderConfiguration::new_with_network(network_info.clone()).unwrap();
        config.fee_rate = fee_rate;
        config
    };
    let (capacity_provider_script_args, capacity_provider_key) =
        get_sighash_lock_args_from_privkey(ckb_provider_lock_privkey);
    let capacity_provider_script = Script::new_builder()
        .code_hash(SIGHASH_TYPE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(capacity_provider_script_args.pack())
        .build();
    let mut change_builder =
        DefaultChangeBuilder::new(&configuration, capacity_provider_script.clone(), Vec::new());
    change_builder.init(&mut tx_builder);
    let iterator = InputIterator::new(vec![ckb_provider_lock], &network_info);
    let mut tx_with_groups = {
        let mut check_result = None;
        for (input_index, input) in iterator.enumerate() {
            let input = input.unwrap();
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
                let script_groups: Vec<ScriptGroup> = lock_groups.into_values().collect();

                let tx_view = change_builder.finalize(tx_builder);

                check_result = Some(TransactionWithScriptGroups::new(tx_view, script_groups));
                break;
            }
        }
        check_result
    }
    .unwrap();

    // sign
    TransactionSigner::new(&network_info)
        .sign_transaction(
            &mut tx_with_groups,
            &SignContexts::new_sighash(vec![capacity_provider_key]),
        )
        .unwrap();

    // send tx
    let tx_with_groups = tx_with_groups.get_tx_view().clone();
    let tx_json = TransactionView::from(tx_with_groups.clone());
    println!(
        "init queue tx: {}",
        serde_json::to_string_pretty(&tx_json).unwrap()
    );
    let rgbpp_rpc_client = RpcClient::new(rgbpp_uri);
    let tx_hash = rgbpp_rpc_client
        .send_transaction(tx_with_groups.data().into(), None)
        .unwrap();
    println!("tx send: {:?}", tx_hash.pack());
}
