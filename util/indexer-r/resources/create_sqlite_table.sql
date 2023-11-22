CREATE TABLE block(
    id INTEGER PRIMARY KEY,
    block_hash BLOB UNIQUE NOT NULL,
    block_number BIGINT NOT NULL,
    compact_target INT,
    parent_hash BLOB,
    nonce BLOB,
    timestamp BIGINT,
    version INT,
    transactions_root BLOB,
    epoch_number INT,
    epoch_index SMALLINT,
    epoch_length SMALLINT,
    dao BLOB,
    proposals_hash BLOB,
    extra_hash BLOB
);

CREATE TABLE block_association_proposal(
    id INTEGER PRIMARY KEY,
    block_hash BLOB NOT NULL,
    proposal BLOB NOT NULL
);

CREATE TABLE block_association_uncle(
    id INTEGER PRIMARY KEY,
    block_hash BLOB NOT NULL,
    uncle_hash BLOB NOT NULL
);

CREATE TABLE ckb_transaction(
    id INTEGER PRIMARY KEY,
    tx_hash BLOB UNIQUE NOT NULL,
    version INT NOT NULL,
    input_count SMALLINT NOT NULL,
    output_count SMALLINT NOT NULL,
    witnesses BLOB,
    block_hash BLOB NOT NULL,
    tx_index INT NOT NULL
);

CREATE TABLE tx_association_header_dep(
    id INTEGER PRIMARY KEY,
    tx_hash BLOB NOT NULL,
    block_hash BLOB NOT NULL
);

CREATE TABLE tx_association_cell_dep(
    id INTEGER PRIMARY KEY,
    tx_hash BLOB NOT NULL,
    out_point BLOB NOT NULL,
    dep_type SMALLINT NOT NULL
);

CREATE TABLE output(
    id INTEGER PRIMARY KEY,
    out_point BLOB UNIQUE NOT NULL,
    capacity BIGINT NOT NULL,
    data BLOB,
    lock_script_hash BLOB,
    type_script_hash BLOB,
    tx_hash BLOB NOT NULL,
    output_index INT NOT NULL   
);

CREATE TABLE input(
    out_point BLOB PRIMARY KEY,
    since BLOB NOT NULL,
    tx_hash BLOB NOT NULL,
    input_index INT NOT NULL
);

CREATE TABLE script(
    id INTEGER PRIMARY KEY,
    script_hash BLOB UNIQUE NOT NULL,
    code_hash BLOB,
    args BLOB,
    hash_type SMALLINT
);