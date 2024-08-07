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
    #[error("missing script info: {0}")]
    MissingScriptInfo(String),
    #[error("transaction build error: {0}")]
    TransactionBuildError(String),
    #[error("transaction sign error: {0}")]
    TransactionSignError(String),
    #[error("transaction send error: {0}")]
    TransactionSendError(String),
    #[error("rpc error: {0}")]
    RpcError(String),
    #[error("Timeout: {0}")]
    TimedOut(String),
    #[error("other error: {0}")]
    Other(String),
}
