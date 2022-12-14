use crate::{
    consistency::{
        airlock::AirLockRequest,
        airlock_tracker::{AirLockTracker, LockHistoryBuilder},
        optimistic_airlock_tracker::OptimisticAirLockTracker,
    },
    lsmt::{level_seg_desc::LsmTreeDesc, tree_delta::TreeDelta},
    storage::segment::SegID, db::rw_db::ClientID,
};

pub struct MetaCache {
    last_seg_pos: u64,
    airlock_tracker: AirLockTracker,
    optimistic_airlock_tracker: OptimisticAirLockTracker,
    tree_desc: LsmTreeDesc,
}

impl MetaCache {
    pub fn new(client_id_new: ClientID) -> Self {
        Self {
            last_seg_pos: 0,
            airlock_tracker: AirLockTracker::new(client_id_new),
            optimistic_airlock_tracker: OptimisticAirLockTracker::new(client_id_new),
            tree_desc: LsmTreeDesc::default(),
        }
    }

    pub fn get_last_pos(&self) -> u64 {
        self.last_seg_pos
    }

    pub fn append_lock_req(&mut self, req: AirLockRequest) -> bool {
        self.airlock_tracker.append_lock_req(req)
    }

    pub fn append_tree_delta(&mut self, delta: &TreeDelta) {
        self.tree_desc.append_tree_delta(delta)
    }

    pub fn forward_last_seg_pos(&mut self, inc_size: u64) {
        self.last_seg_pos += inc_size;
    }

    pub fn get_tail(&self) -> SegID {
        self.tree_desc.get_tail()
    }

    pub fn get_tree_desc(&self) -> LsmTreeDesc {
        //TODO[L0]: optimize LsmTreeDesc to speed up the process of getting a tree desc snapshot
        self.tree_desc.clone()
    }

    pub fn get_airlock_tracker_mut(&mut self) -> &mut AirLockTracker {
        &mut self.airlock_tracker
    }

    pub fn get_optimistic_airlock_tracker_mut(&mut self) -> &mut OptimisticAirLockTracker {
        &mut self.optimistic_airlock_tracker
    }

    pub fn get_airlock_tracker(&self) -> &AirLockTracker {
        &self.airlock_tracker
    }
}
