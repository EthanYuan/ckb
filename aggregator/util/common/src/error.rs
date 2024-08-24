#[allow(clippy::enum_variant_names)]
#[derive(thiserror::Error, Debug)]
#[repr(i32)]
pub enum Error {
    #[error("live cell not found: {0}")]
    LiveCellNotFound(String),
    #[error("binary file read error: {0}")]
    BinaryFileReadError(String),
    #[error("error while decoding queue cell data: {0}")]
    QueueCellDataDecodeError(String),
    #[error("outbox has unprocessed requests, cannot add new ones")]
    QueueOutboxHasUnprocessedRequests,
    #[error("queue cell data error: {0}")]
    QueueCellDataError(String),
    #[error("missing script info: {0}")]
    MissingScriptInfo(String),
    #[error("transaction build error: {0}")]
    TransactionBuildError(String),
    #[error("transaction sign error: {0}")]
    TransactionSignError(String),
    #[error("transaction send error: {0}")]
    TransactionSendError(String),
    #[error("transaction parse error: {0}")]
    TransactionParseError(String),
    #[error("rpc error: {0}")]
    RpcError(String),
    #[error("Timeout: {0}")]
    TimedOut(String),
    #[error("Lock not found: {0}")]
    LockNotFound(String),
    #[error("Branch script not found: {0}")]
    BranchScriptNotFound(String),
    #[error("database error: {0}")]
    DatabaseError(String),
    #[error("other error: {0}")]
    Other(String),
}
