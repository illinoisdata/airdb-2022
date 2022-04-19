use crate::{
    common::error::GResult,
    consistency::airlock::{AirLockCheck, AirLockID, AirLockRequest, AirLockStatus, CommitInfo},
    io::storage_connector::StorageConnector,
    lsmt::level_seg_desc::LsmTreeDesc,
};

use super::segment::SegID;

pub trait Meta {
    fn refresh_meta(&mut self, conn: &dyn StorageConnector) -> GResult<()>;

    fn get_tail_from_cache(&self) -> SegID;
    fn get_tree_desc_from_cache(&self) -> LsmTreeDesc;

    fn get_refreshed_tail(&mut self, conn: &dyn StorageConnector) -> GResult<SegID>;
    fn get_refreshed_tree_desc(&mut self, conn: &dyn StorageConnector) -> GResult<LsmTreeDesc>;

    fn can_acquire_lock_by_cache(&self, req: &AirLockRequest) -> bool;

    fn verify_lock_status(
        &mut self,
        conn: &dyn StorageConnector,
        lock_check: &AirLockCheck,
    ) -> GResult<AirLockStatus<AirLockID>>;

    fn check_commit(&mut self, conn: &dyn StorageConnector, lock_id: &AirLockID) -> bool;

    fn append_commit_info(
        &self,
        conn: &dyn StorageConnector,
        commit_info: CommitInfo,
    ) -> GResult<()>;

    fn append_lock_request(
        &self,
        conn: &dyn StorageConnector,
        lock_req: &AirLockRequest,
    ) -> GResult<()>;
}
