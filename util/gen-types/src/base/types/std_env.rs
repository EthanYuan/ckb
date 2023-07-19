use crate::{
    base::{DepType, ScriptHashType},
    packed,
};

use ckb_error::OtherError;

impl TryFrom<u8> for ScriptHashType {
    type Error = OtherError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(ScriptHashType::Data),
            1 => Ok(ScriptHashType::Type),
            2 => Ok(ScriptHashType::Data1),
            4 => Ok(ScriptHashType::Data2),
            _ => Err(OtherError::new(::std::format!(
                "Invalid script hash type {v}"
            ))),
        }
    }
}

impl TryFrom<packed::Byte> for ScriptHashType {
    type Error = OtherError;

    fn try_from(v: packed::Byte) -> Result<Self, Self::Error> {
        Into::<u8>::into(v).try_into()
    }
}

impl TryFrom<packed::Byte> for DepType {
    type Error = OtherError;

    fn try_from(v: packed::Byte) -> Result<Self, Self::Error> {
        match Into::<u8>::into(v) {
            0 => Ok(DepType::Code),
            1 => Ok(DepType::DepGroup),
            _ => Err(OtherError::new(::std::format!("Invalid dep type {v}"))),
        }
    }
}
