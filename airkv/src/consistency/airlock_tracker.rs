use std::{
    cell::RefCell,
    collections::{hash_map, HashMap},
    rc::Rc,
};

use crate::common::error::GResult;

use super::airlock::{
    AirLockCheck, AirLockID, AirLockRequest, AirLockStatus, ClientID, CommitInfo, LockHolder,
    ResourceID,
};

pub trait LockHistoryBuilder {
    fn append_lock_reqs(&mut self, reqs: Vec<AirLockRequest>) -> GResult<()>;
    fn append_lock_req(&mut self, req: AirLockRequest) -> bool;
    fn append_commit(&mut self, commit_info: &CommitInfo) -> bool;
}

pub trait LockHistoryAccessor {
    fn check_commit(&self, lock_id: &AirLockID) -> bool;
    fn valid_lock_holder(&self, req: &AirLockCheck) -> AirLockStatus<AirLockID>;
}

pub struct AirLockTracker {
    res_lock_map: HashMap<ResourceID, Rc<RefCell<LockHolder>>>,
    client_active_lock_map: HashMap<ClientID, Rc<RefCell<LockHolder>>>,
    cur_client_id: ClientID,
    cur_client_last_success_commit: Option<AirLockID>,
}

impl LockHistoryBuilder for AirLockTracker {
    fn append_lock_reqs(&mut self, reqs: Vec<AirLockRequest>) -> GResult<()> {
        reqs.into_iter().for_each(|req| {
            self.append_lock_req(req);
        });
        Ok(())
    }

    // this method is called when building/refreshing local meta cache
    // append lock req to airlocktracker
    // return true => current lock request successfully holds the lock
    // seturn false => current lock request fails to hold the lock
    fn append_lock_req(&mut self, req: AirLockRequest) -> bool {
        let req_client = req.get_client();
        if self.client_active_lock_map.contains_key(req_client) {
            let holder = self
                .client_active_lock_map
                .get(req_client)
                .unwrap_or_else(|| panic!("client (id: {} ) not found ", req_client));
            if holder.borrow().is_renewed_req(&req) {
                // the new request is an renew request
                holder
                    .borrow_mut()
                    .renew_lease(*req.get_request_time());
                true
            } else {
                // in this branch, the new lock request is an acquire request
                self.append_acquire_req(&req)
            }
        } else {
            // in this branch, the new lock request is an acquire request
            self.append_acquire_req(&req)
        }
    }

    // this method is called when building/refreshing local meta cache
    // append_commit will check whether the commit is valid and attach commit status to the corresponding lock
    // return true => this commit is a valid one
    // return false => this commit is invalid
    fn append_commit(&mut self, commit_info: &CommitInfo) -> bool {
        let client_id = commit_info.get_client();
        match self.client_active_lock_map.entry(*client_id) {
            hash_map::Entry::Occupied(entry) => {
                let commit_success = {
                    let holder = entry.get().borrow();
                    let holder_lock_id = holder.get_lock_id();
                    if holder_lock_id == commit_info.get_lock_id() {
                        if client_id == &self.cur_client_id {
                            // if the commit is launched by the current client, record it in cur_client_last_success_commit
                            // cur_client_last_success_commit is used to response check_commit
                            self.cur_client_last_success_commit =
                                Some(commit_info.get_lock_id().clone());
                        }
                        //remove committed lock holder from res_lock_map and client_active_lock_map
                        let res_ids = holder.get_res_ids();
                        res_ids.iter().for_each(|res_id| {
                            let old_holder = self.res_lock_map.remove(res_id).unwrap();
                            //TODO: remove this check
                            assert!(old_holder.borrow().get_lock_id() == holder_lock_id)
                        });
                        true
                    } else {
                        // println!(
                        //     "WARN: unexpected commit info for lock {}, because the active lock {} is not the target one",
                        //     commit_info.get_lock_id(),
                        //     holder_lock_id
                        // );

                        // two cases can go to this branch
                        // 1. some renew lock requests(heartbeat) fail while the critical operation is executing
                        // 2. there is a long gc/network lag before the critical operation submit the commit info
                        false
                    }
                };

                if commit_success {
                    entry.remove_entry();
                }
                commit_success
            }
            hash_map::Entry::Vacant(_) => false,
        }
    }
}

impl AirLockTracker {
    pub fn new(client_id_new: ClientID) -> Self {
        Self {
            res_lock_map: HashMap::new(),
            client_active_lock_map: HashMap::new(),
            cur_client_id: client_id_new,
            cur_client_last_success_commit: None,
        }
    }

    pub fn can_acquire(&self, req: &AirLockRequest) -> bool {
        let res_ids = req.get_res_ids();
        let acquire_result = res_ids
            .iter()
            .map(|id| self.try_acquire(id, req))
            .collect::<Vec<AirLockStatus<AirLockID>>>();
        acquire_result.iter().all(|res| res.is_success())
    }

    fn append_acquire_req(&mut self, req: &AirLockRequest) -> bool {
        let res_ids = req.get_res_ids();
        let client_id = req.get_client();
        let timestamp = req.get_request_time();
        let can_acquired = self.can_acquire(req);

        if can_acquired {
            let new_holder = Rc::new(RefCell::new(LockHolder::new(
                *client_id,
                *timestamp,
                res_ids.clone(),
                false,
            )));
            res_ids.iter().for_each(|res_id| {
                self.res_lock_map
                    .insert(*res_id, Rc::clone(&new_holder));
            });
            self.client_active_lock_map
                .insert(*client_id, new_holder);
            true
        } else {
            false
        }
    }

    fn try_acquire(&self, res_id: &ResourceID, req: &AirLockRequest) -> AirLockStatus<AirLockID> {
        match self.res_lock_map.get(res_id) {
            Some(holder) => holder.borrow().valid_acquire(req),
            None => AirLockStatus::Acquired(AirLockID::new(
                *req.get_client(),
                *req.get_request_time(),
            )),
        }
    }
}

impl LockHistoryAccessor for AirLockTracker {
    // this method is called by AirLockManager.acquire_auto_release_lock after appending the lock request to the meta segment
    fn valid_lock_holder(&self, check: &AirLockCheck) -> AirLockStatus<AirLockID> {
        let client_id = check.get_client();
        match self.client_active_lock_map.get(client_id) {
            Some(holder) => holder.borrow().valid_check(check),
            None => AirLockStatus::Failed,
        }
    }

    // this method is called by AirLockManager.acquire_auto_release_lock after submitting commit request
    fn check_commit(&self, lock_id: &AirLockID) -> bool {
        self.cur_client_last_success_commit.is_some()
            && self.cur_client_last_success_commit.as_ref().unwrap() == lock_id
    }
}
