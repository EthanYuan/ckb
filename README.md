# Branch Chain

A Branch Chain is a BTC Layer-2 chain that actually runs smart contracts. It is characterized by high TPS (Transactions Per Second), low fees, fewer block-producing nodes, and a challenge-based security model. 

For more information, see the [branch chain](https://github.com/ckb-cell/utxo-stack-doc/blob/master/docs/branch-chain.md).

## Features

- Compatible with RGB++ layer smart contracts
- BTC as cell capacity token, 1 satoshi can be used for 1 byte of on-chain space
- Single sequencer
- Blocks produced every 2 seconds

---

## Quick Start

### Build from Source

Source code:

```shell
# get ckb source code
git clone https://github.com/EthanYuan/ckb.git
cd ckb
git checkout v0.116.1-branch-chain
```

Build:

```shell
make prod
```

For reference, see the [build from source](https://docs-old.nervos.org/docs/basics/guides/get-ckb#build-from-source)

### Init

```shell
mkdir branch-dev
cd branch-dev
```

```shell
ckb init --chain dev --genesis-message branch-dev
```

### Configure 

The following settings are used to configure the `block_assembler` in the `ckb.toml` file:

```toml
[block_assembler]
code_hash = "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8"
args = "0xc8328aabcd9b9e8e64fbc566c4385c3bdeb219d7"
hash_type = "type"
message = "0x"
```

The `args` value in the `block_assembler` configuration is directly taken from the `args` specified in the `lock script` of a pre-defined cell in the genesis configuration (`[genesis.issued_cells]`). 

In the genesis block, there are two `issued_cells` with sufficient capacity representing BTC. For more details, see the [genesis configuration](/resource/specs/dev.toml).

**Please NOTE that this setup is intended for testing purposes only and should not be used in a production environment.**

### Start Node with Indexer

```shell
ckb run --indexer
```
Restarting in the same directory will reuse the data.

## Use RPC

Find RPC port in the log output, the following command assumes 8114 is used:

```shell
curl -d '{"id": 1, "jsonrpc": "2.0", "method":"get_tip_header","params": []}' \
  -H 'content-type:application/json' 'http://localhost:8114'
```

For more RPC commands, please refer to the [RPC documentation](rpc/README.md).

---

## Documentations

- [RGBPP Layer](rgbpp-layer.md)
