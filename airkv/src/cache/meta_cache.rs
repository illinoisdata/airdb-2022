use crate::{
    common::error::GResult,
    consistancy::{airlock::AirLockRequest, airlock_tracker::AirLockTracker},
    lsmt::{level_seg_desc::LsmTreeDesc, tree_delta::TreeDelta}, storage::{segment::SegID},
};

pub struct MetaIncrement {
    inc_size: u64,
    lock_reqs: Vec<AirLockRequest>,
    tree_deltas: Vec<TreeDelta>,
}

impl MetaIncrement {
    pub fn new(inc_size_new: u64, reqs_new: Vec<AirLockRequest>, deltas_new: Vec<TreeDelta>) -> Self {
        Self {
            inc_size: inc_size_new,
            lock_reqs: reqs_new,
            tree_deltas: deltas_new,
        }
    }
}

#[derive(Default)]
pub struct MetaCache {
    last_seg_pos: u64,
    airlock_tracker: AirLockTracker,
    tree_desc: LsmTreeDesc,
}

impl MetaCache {
    pub fn new(airlock_tracker_new: AirLockTracker, tree_desc_new: LsmTreeDesc) -> Self {
        Self {
            last_seg_pos: 0,
            airlock_tracker: airlock_tracker_new,
            tree_desc: tree_desc_new,
        }
    }

    pub fn get_last_pos(&self) -> u64 {
        self.last_seg_pos
    }

    pub fn append_inc(&mut self, inc: MetaIncrement) -> GResult<()> {
        self.tree_desc.append_tree_deltas(&inc.tree_deltas)?;
        self.airlock_tracker.append_lock_reqs(&inc.lock_reqs)?;
        self.last_seg_pos += inc.inc_size;
        Ok(())
    }

    pub fn get_tail(&self) -> SegID {
        self.tree_desc.get_tail()
    }

    pub fn get_tree_desc(&self) -> LsmTreeDesc {
        //TODO[L0]: optimize LsmTreeDesc to speed up the process of getting a tree desc snapshot
        self.tree_desc.clone()
    }
}
