use crate::{
    common::error::GResult,
    io::storage_connector::StorageConnector,
    storage::{meta::Meta, meta_segment::MetaSegment, segment_manager::SegmentManager},
};

use super::airlock::{
    AirLockCheck, AirLockID, AirLockRequest, AirLockStatus, ClientID, CriticalOperation, ResourceID,
};

pub struct AirLockManager {}

#[allow(dead_code)]
impl AirLockManager {
    // for short operations
    // true => successfully commit
    // false => failed to commit (can't acquire locks or execution failure)

    pub fn run_with_single_lock(
        conn: &dyn StorageConnector,
        seg_manager: &mut SegmentManager,
        critical_op: &dyn CriticalOperation,
    ) -> bool {
        match AirLockManager::acquire_auto_release_lock(
            conn,
            seg_manager.get_mut_meta_seg(),
            critical_op.get_res(),
            critical_op.get_client(),
        ) {
            AirLockStatus::Acquired(lock_id) => {
                // TODO: merge check_uninit with acquire lock to promote efficiency
                if critical_op.check_uninit(conn, seg_manager) {
                    //run critical codes/transaction
                    match critical_op.run(conn, seg_manager, lock_id.clone()) {
                        Ok(_) => {
                            // verify if the commit is valid
                            seg_manager.get_mut_meta_seg().check_commit(conn, &lock_id)
                        }
                        Err(err) => {
                            println!("ERROR: commit failed due to: {:?}", err);
                            false
                        }
                    }
                } else {
                    // rarely reach this branch
                    println!("WARN: lock acquired but the resource has already been initialized");
                    false
                }
            }
            AirLockStatus::Renewed(lock_id) => {
                println!("WARN: expect AirLockStatus::Acquired but get AirLockStatus::Renewed when executing run_with_single_lock for lock id {}", lock_id);
                false
            }
            _ => false,
        }
    }

    // for long operations
    #[allow(unused)]
    pub fn run_with_renew_lock(
        conn: &dyn StorageConnector,
        seg_manager: &mut SegmentManager,
        critical_op: &dyn CriticalOperation,
    ) -> GResult<()> {
        todo!()
    }

    fn acquire_auto_release_lock(
        conn: &dyn StorageConnector,
        meta: &mut MetaSegment,
        critical_res_ids: Vec<ResourceID>,
        client_id: ClientID,
    ) -> AirLockStatus<AirLockID> {
        let lock_req: &AirLockRequest = &AirLockRequest::new(critical_res_ids, client_id);
        match meta.append_lock_request(conn, lock_req) {
            Ok(_) => {
                match meta.verify_lock_status(conn, &AirLockCheck::new(lock_req.get_lock_id())) {
                    Ok(lock_status) => lock_status,
                    Err(error) => {
                        println!(
                            "ERROR: error happens when verifying lock status: {:?}",
                            error
                        );
                        AirLockStatus::Failed
                    }
                }
            }
            Err(error) => {
                println!(
                    "ERROR: error happens when appending lock requests to the meta segment: {:?}",
                    error
                );
                AirLockStatus::Failed
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::error::GResult;


    #[test]
    fn lock_test() -> GResult<()> {
        

        Ok(())
    }

}