use std::collections::HashMap;

use crate::{db::rw_db::ClientID};

use super::{
    airlock::ResourceID,
    optimistic_airlock::{OptimisticAirLockID, OptimisticCommitInfo},
};

pub trait OptimisticLockHistoryBuilder {
    fn append_commit(&mut self, commit_info: &OptimisticCommitInfo) -> bool;
}

pub trait OptimisticLockHistoryAccessor {
    fn check_commit(&self, lock_id: &OptimisticAirLockID) -> bool;
}

pub struct OptimisticAirLockTracker {
    res_lock_map: HashMap<ResourceID, ClientID>,
    _client_id: ClientID,
}

impl OptimisticLockHistoryBuilder for OptimisticAirLockTracker {
    fn append_commit(&mut self, commit_info: &OptimisticCommitInfo) -> bool {
        let lock_id = commit_info.get_lock_id();
        let client_id = lock_id.get_client_id();
        let res_ids = lock_id.get_res_ids();
        if res_ids.iter().all(|res_id| {
            !self
                .res_lock_map
                .contains_key(res_id)
        }) {
            //all target resources don't exist in history res_lock_map
            res_ids.iter().for_each(|res_id| {
                self.res_lock_map
                    .insert(*res_id, client_id);
            });
            true
        } else {
            false
        }
    }
}

impl OptimisticLockHistoryAccessor for OptimisticAirLockTracker {
    fn check_commit(&self, lock_id: &OptimisticAirLockID) -> bool {
        let res_ids = lock_id.get_res_ids();
        let client_id = lock_id.get_client_id();
        res_ids
            .iter()
            .map(|res_id| {
                let cid_option = self.res_lock_map.get(res_id);
                if let Some(cid) = cid_option {
                    *cid == client_id
                } else {
                    // no resource found
                    panic!("resource {} not found in lock commit history", res_id)
                }
            })
            .all(|res| res)
    }
}

impl OptimisticAirLockTracker {
    pub fn new(client_id_new: ClientID) -> Self {
        Self {
            _client_id: client_id_new,
            res_lock_map: HashMap::new(),
        }
    }
}
