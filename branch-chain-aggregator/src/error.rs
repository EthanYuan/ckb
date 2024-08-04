#[allow(clippy::enum_variant_names)]
#[derive(thiserror::Error, Debug)]
#[repr(i32)]
pub enum Error {
    #[error("error ocurred while requesing json-rpc")]
    JsonRpcRequestError,
    #[error("encounter error while searching quest cells")]
    FetchRequestCellsError,
    #[error("encounter error while searching message queue cell: {0}")]
    FetchRgbppMessageQueueCellError(String),
    #[error("queue cell not found")]
    QueueCellNotFound,
    #[error("binary file read error: {0}")]
    BinaryFileReadError(String),
    #[error("error while decoding queue cell data")]
    QueueCellDataDecodeError,
}
