use crate::{lsmt::{tree_delta::TreeDelta, level_seg_desc::LsmTreeDesc}, consistancy::airlock::AirLockRequest, common::error::GResult};

use super::segment::SegID;

pub trait Meta {
    fn refresh_meta(&mut self) -> GResult<()>;

    fn get_tail_from_cache(&self) -> GResult<SegID>;
    fn get_tree_desc_from_cache(&self) -> GResult<LsmTreeDesc>;

    fn get_refreshed_tail(&mut self) -> GResult<SegID>;
    fn get_refreshed_tree_desc(&mut self) -> GResult<LsmTreeDesc>;
    fn verify_lock_status(&self, lock_req: &AirLockRequest) -> GResult<()>;

    fn append_tree_delta(&self, delta: &TreeDelta) -> GResult<()>;
    fn append_lock_request(&self, lock_req: &AirLockRequest) -> GResult<()>;
}