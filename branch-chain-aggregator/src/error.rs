#[allow(clippy::enum_variant_names)]
#[derive(thiserror::Error, Debug)]
#[repr(i32)]
pub enum Error {
    #[error("error ocurred while requesing json-rpc")]
    JsonRpcRequestError,
    #[error("encounter error while searching live cells")]
    FetchLiveCellsError,
}
