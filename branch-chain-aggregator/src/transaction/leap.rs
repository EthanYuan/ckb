use crate::error::Error;
use crate::schemas::leap::Request;
use crate::Aggregator;

use ckb_types::H256;

impl Aggregator {
    pub(crate) fn create_leap_tx(&self, _requests: Vec<Request>) -> Result<H256, Error> {
        // search queue cell on RGB++

        // Check if the requests of the last leap tx are duplicated, and if so, return immediately.

        Ok(H256::default())
    }
}
