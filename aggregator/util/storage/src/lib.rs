#![allow(missing_docs)]

use aggregator_common::error::Error::{self, DatabaseError};
use ckb_types::H256;
use rocksdb::{
    ops::{Delete, Get, Iterate, Open, Put},
    IteratorMode, Options, DB,
};

use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Storage {
    db: Arc<DB>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchRequestStatus {
    Pending = 0x00,
    Commit = 0x01,
}

impl From<BranchRequestStatus> for u8 {
    fn from(status: BranchRequestStatus) -> Self {
        status as u8
    }
}

impl TryFrom<u8> for BranchRequestStatus {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(BranchRequestStatus::Pending),
            0x01 => Ok(BranchRequestStatus::Commit),
            _ => Err(()),
        }
    }
}

impl Storage {
    /// Opens or creates a new database instance
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, path).map_err(|err| DatabaseError(err.to_string()))?);
        Ok(Storage { db })
    }

    /// Inserts a new branch request with a default status of `Pending`
    pub fn insert_branch_request(&self, height: u64, tx: H256) -> Result<(), Error> {
        let key = height.to_be_bytes();
        let mut value = vec![BranchRequestStatus::Pending.into()]; // Use enum value to represent status
        value.extend_from_slice(tx.as_bytes());
        self.db
            .put(key, value)
            .map_err(|err| DatabaseError(err.to_string()))?;

        // If this is the first pending request or if the earliest_pending is not set, update it
        if self.get_earliest_pending()?.is_none() {
            self.db
                .put(b"earliest_pending", &key)
                .map_err(|err: rocksdb::Error| Error::DatabaseError(err.to_string()))?;
        }

        Ok(())
    }

    /// Updates the status of a specific height to `Commit`, with validation of `tx`
    pub fn commit_branch_request(&self, height: u64, expected_tx: H256) -> Result<(), Error> {
        let key = height.to_be_bytes();

        let value = self
            .db
            .get(key)
            .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))?;
        let value = match value {
            Some(val) => val,
            None => return Err(DatabaseError("Height not found in the database".into())),
        };

        let stored_tx = H256::from_slice(&value[1..33])
            .map_err(|_| DatabaseError("Failed to parse stored transaction".into()))?;

        // Verify that the provided tx matches the stored tx
        if stored_tx != expected_tx {
            return Err(DatabaseError(
                "Transaction hash does not match the stored value".into(),
            ));
        }

        let mut value = value.to_vec(); // Convert DBVector to Vec<u8> for mutability
        value[0] = BranchRequestStatus::Commit.into(); // Update status to `Commit`
        self.db
            .put(key, value)
            .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))?; // Write back to the database

        // Check if the next height exists and is pending
        let next_height = height + 1;
        let next_key = next_height.to_be_bytes();
        if let Some(next_value) = self
            .db
            .get(next_key)
            .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))?
        {
            if BranchRequestStatus::try_from(next_value[0]).unwrap() == BranchRequestStatus::Pending
            {
                self.db
                    .put(b"earliest_pending", next_height.to_be_bytes())
                    .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))?;
            } else {
                return Err(DatabaseError(
                    "Unexpected commit status found for next height".into(),
                ));
            }
        } else {
            // No next height or it is not pending, clear the earliest_pending
            self.db
                .delete(b"earliest_pending")
                .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))?;
        }

        Ok(())
    }

    /// Retrieves the earliest height with `Pending` status
    pub fn get_earliest_pending(&self) -> Result<Option<(u64, H256)>, Error> {
        match self
            .db
            .get(b"earliest_pending")
            .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))?
        {
            Some(height_bytes) => {
                let height = u64::from_be_bytes(height_bytes.as_ref().try_into().map_err(
                    |err: std::array::TryFromSliceError| DatabaseError(err.to_string()),
                )?);
                let key = height.to_be_bytes();
                if let Some(value) = self
                    .db
                    .get(key)
                    .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))?
                {
                    let tx = H256::from_slice(&value[1..33])
                        .map_err(|_| DatabaseError("Failed to parse stored tx hash".into()))?;
                    Ok(Some((height, tx)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Retrieves the highest height in the database
    pub fn get_last_branch_request(
        &self,
    ) -> Result<Option<(u64, BranchRequestStatus, H256)>, Error> {
        let mut iter = self.db.iterator(IteratorMode::End);

        while let Some((key, value)) = iter.next() {
            // Only process keys that have the correct length for a u64 height (8 bytes)
            if key.len() == 8 {
                let height = u64::from_be_bytes(key.as_ref().try_into().map_err(
                    |err: std::array::TryFromSliceError| DatabaseError(err.to_string()),
                )?);

                let status = BranchRequestStatus::try_from(value[0])
                    .map_err(|_| DatabaseError("Failed to parse stored status".into()))?;
                let tx = H256::from_slice(&value[1..33])
                    .map_err(|_| DatabaseError("Failed to parse stored transaction".into()))?;
                return Ok(Some((height, status, tx)));
            }
        }
        Ok(None)
    }

    pub fn get_staged_tx(&self) -> Result<Option<H256>, Error> {
        // Retrieve the staged transaction hash from the store
        self.db
            .get(b"staged_tx")
            .map(|opt| opt.map(|bytes| H256::from_slice(&bytes).expect("Invalid H256")))
            .map_err(|err| Error::DatabaseError(err.to_string()))
    }

    /// Records a transaction that has been constructed but not yet confirmed
    pub fn record_staged_tx(&self, tx_hash: H256) -> Result<(), Error> {
        self.db
            .put(b"staged_tx", tx_hash.as_bytes())
            .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))
    }

    /// Removes the staged transaction record after it has been confirmed
    pub fn clear_staged_tx(&self) -> Result<(), Error> {
        self.db
            .delete(b"staged_tx")
            .map_err(|err: rocksdb::Error| DatabaseError(err.to_string()))
    }

    /// Checks if a given transaction hash has already been stored for a specific height
    pub fn is_tx_stored(&self, height: u64, tx_hash: H256) -> Result<bool, Error> {
        let key = height.to_be_bytes();

        // Retrieve the stored value by height
        if let Some(value) = self
            .db
            .get(key)
            .map_err(|err: rocksdb::Error| Error::DatabaseError(err.to_string()))?
        {
            let stored_tx =
                H256::from_slice(&value[1..33]) // Extract tx field from the value
                    .map_err(|_| {
                        Error::DatabaseError("Failed to parse stored transaction".into())
                    })?;

            debug_assert!(stored_tx == tx_hash);

            // Compare the stored transaction hash with the provided one
            Ok(stored_tx == tx_hash)
        } else {
            Ok(false)
        }
    }

    /// Counts the total number of entries where the key is a `height` (u64 encoded as 8 bytes)
    pub fn count_entries(&self) -> Result<u64, Error> {
        let mut count = 0;
        let iter = self.db.iterator(IteratorMode::Start);
        for (key, _) in iter {
            if key.len() == 8 {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_store() -> Storage {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        Storage::new(temp_dir.path()).expect("failed to create store")
    }

    #[test]
    fn test_insert_and_get_last_branch_request() {
        let store = setup_store();

        // Insert branch requests
        store
            .insert_branch_request(1, H256::default())
            .expect("failed to insert request 1");
        store
            .insert_branch_request(2, H256::default())
            .expect("failed to insert request 2");
        store
            .insert_branch_request(3, H256::default())
            .expect("failed to insert request 3");

        // Get the last branch request
        let last_request = store
            .get_last_branch_request()
            .expect("failed to get last branch request");

        assert_eq!(
            last_request,
            Some((3, BranchRequestStatus::Pending, H256::default()))
        );
    }

    #[test]
    fn test_commit_height() {
        let store = setup_store();

        // Insert branch requests
        store
            .insert_branch_request(1, H256::default())
            .expect("failed to insert request 1");
        store
            .insert_branch_request(2, H256::default())
            .expect("failed to insert request 2");

        // Commit the first request
        store
            .commit_branch_request(1, H256::default())
            .expect("failed to commit height 1");

        // Check if the earliest pending is now 2
        let earliest_pending = store
            .get_earliest_pending()
            .expect("failed to get earliest pending");
        assert_eq!(earliest_pending, Some((2, H256::default())));
    }

    #[test]
    fn test_get_earliest_pending_after_all_commit() {
        let store = setup_store();

        // Insert and commit branch requests
        store
            .insert_branch_request(1, H256::default())
            .expect("failed to insert request 1");
        store
            .insert_branch_request(2, H256::default())
            .expect("failed to insert request 2");
        store
            .commit_branch_request(1, H256::default())
            .expect("failed to commit height 1");
        store
            .commit_branch_request(2, H256::default())
            .expect("failed to commit height 2");

        // Check if there is no pending request left
        let earliest_pending = store
            .get_earliest_pending()
            .expect("failed to get earliest pending");
        assert_eq!(earliest_pending, None);
    }

    #[test]
    fn test_earliest_pending_with_only_inserted_requests() {
        let store = setup_store();

        // Insert branch requests without committing them
        store
            .insert_branch_request(1, H256::default())
            .expect("failed to insert request 1");
        store
            .insert_branch_request(2, H256::default())
            .expect("failed to insert request 2");

        // Check if the earliest pending is the first inserted request
        let earliest_pending = store
            .get_earliest_pending()
            .expect("failed to get earliest pending");
        assert_eq!(earliest_pending, Some((1, H256::default())));
    }

    #[test]
    fn test_commit_height_invalid_tx() {
        let store = setup_store();

        // Insert branch requests
        store
            .insert_branch_request(1, H256::default())
            .expect("failed to insert request 1");

        // Attempt to commit with an invalid tx
        let result = store.commit_branch_request(1, H256::from_slice(&[1u8; 32]).unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_get_last_branch_request_empty_store() {
        let store = setup_store();

        // Attempt to get the last branch request in an empty store
        let last_request = store
            .get_last_branch_request()
            .expect("failed to get last branch request");
        assert_eq!(last_request, None);
    }

    #[test]
    fn test_count_height_entries() {
        let store = setup_store();

        // Insert some branch requests
        store
            .insert_branch_request(1, H256::default())
            .expect("failed to insert request 1");
        store
            .insert_branch_request(2, H256::default())
            .expect("failed to insert request 2");
        store
            .insert_branch_request(3, H256::default())
            .expect("failed to insert request 3");

        // Count the height entries
        let count = store
            .count_entries()
            .expect("failed to count height entries");

        // Verify the count
        assert_eq!(count, 3);
    }

    #[test]
    fn test_count_height_entries_with_non_height_keys() {
        let store = setup_store();

        // Insert some branch requests
        store
            .insert_branch_request(1, H256::default())
            .expect("failed to insert request 1");
        store
            .insert_branch_request(2, H256::default())
            .expect("failed to insert request 2");

        // Insert a non-height key (e.g., staged_tx)
        store
            .db
            .put(b"staged_tx", H256::default().as_bytes())
            .expect("failed to insert non-height key");

        // Count the height entries
        let count = store
            .count_entries()
            .expect("failed to count height entries");

        // Verify the count
        assert_eq!(count, 2); // The staged_tx key should not be counted
    }

    #[test]
    fn test_count_height_entries_empty_store() {
        let store = setup_store();

        // Count the height entries in an empty store
        let count = store
            .count_entries()
            .expect("failed to count height entries");

        // Verify the count
        assert_eq!(count, 0);
    }
}
