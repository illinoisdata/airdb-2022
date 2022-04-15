use crate::{
    cache::meta_cache::MetaCache,
    common::{
        bytebuffer::ByteBuffer,
        dataslice::DataSlice,
        error::{AppendError, GResult, GenericError},
        readbuffer::ReadBuffer,
        serde::Serde,
    },
    consistency::{
        airlock::{AirLockCheck, AirLockID, AirLockRequest, AirLockStatus, ClientID, CommitInfo},
        airlock_tracker::{LockHistoryAccessor, LockHistoryBuilder},
    },
    io::{file_utils::Range, storage_connector::StorageConnector},
    lsmt::level_seg_desc::LsmTreeDesc,
};

use super::{
    data_entry::AppendRes,
    meta::Meta,
    segment::{SegID, Segment, SegmentInfo},
};

/// There are two types of append data in MetaSegment
enum MetaAppendType {
    CommitInfo = 0,
    LockReq = 1,
}

pub struct MetaSegment {
    meta_cache: MetaCache,
    seg_info: SegmentInfo,
}

impl MetaSegment {
    pub fn new(seg_info_new: SegmentInfo, client_id: ClientID) -> Self {
        Self {
            meta_cache: MetaCache::new(client_id),
            seg_info: seg_info_new,
        }
    }
}

impl Meta for MetaSegment {
    fn get_refreshed_tail(&mut self, conn: &dyn StorageConnector) -> GResult<SegID> {
        // refresh meta
        self.refresh_meta(conn)?;
        // get tail from cache
        Ok(self.get_tail_from_cache())
    }

    fn get_refreshed_tree_desc(&mut self, conn: &dyn StorageConnector) -> GResult<LsmTreeDesc> {
        // refresh meta
        self.refresh_meta(conn)?;
        Ok(self.get_tree_desc_from_cache())
    }

    fn get_tail_from_cache(&self) -> SegID {
        self.meta_cache.get_tail()
    }

    fn get_tree_desc_from_cache(&self) -> LsmTreeDesc {
        self.meta_cache.get_tree_desc()
    }

    fn append_commit_info(
        &self,
        conn: &dyn StorageConnector,
        commit_info: CommitInfo,
    ) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        // write MetaAppendType
        buffer.write_u8(MetaAppendType::CommitInfo as u8);
        commit_info.serialize(&mut buffer)?;
        //TODO: properly deal with append response
        let res = conn.append(self.seg_info.get_seg_path(), buffer.to_view());
        match res {
            AppendRes::Success(_) => Ok(()),
            x => Err(Box::new(AppendError::new(x.to_string())) as GenericError),
        }
    }

    fn append_lock_request(
        &self,
        conn: &dyn StorageConnector,
        lock_req: &AirLockRequest,
    ) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        // write MetaAppendType
        buffer.write_u8(MetaAppendType::LockReq as u8);
        lock_req.serialize(&mut buffer)?;
        //TODO: properly deal with append response
        let res = conn.append(self.seg_info.get_seg_path(), buffer.to_view());
        match res {
            super::data_entry::AppendRes::Success(_) => Ok(()),
            x => Err(Box::new(AppendError::new(x.to_string())) as GenericError),
        }
    }

    fn verify_lock_status(
        &mut self,
        conn: &dyn StorageConnector,
        lock_req: &AirLockCheck,
    ) -> GResult<AirLockStatus<AirLockID>> {
        //refresh meta
        self.refresh_meta(conn)?;
        //check the lock status from lock tracker
        Ok(self
            .meta_cache
            .get_airlock_tracker()
            .valid_lock_holder(lock_req))
    }

    fn refresh_meta(&mut self, conn: &dyn StorageConnector) -> GResult<()> {
        let last_pos = self.meta_cache.get_last_pos();
        let path = self.seg_info.get_seg_path();
        let mut inc_buffer = ByteBuffer::wrap(conn.read_range(path, &Range::new(last_pos, 0))?);
        let inc_size = inc_buffer.len();

        while inc_buffer.has_remaining() {
            let meta_append_type = inc_buffer.read_u8();
            match meta_append_type {
                x if x == MetaAppendType::LockReq as u8 => {
                    let lock_req = AirLockRequest::deserialize(&mut inc_buffer);
                    self.meta_cache.append_lock_req(lock_req);
                }
                y if y == MetaAppendType::CommitInfo as u8 => {
                    let commit_info = CommitInfo::deserialize(&mut inc_buffer);
                    // check if it is a valid commit
                    if self
                        .meta_cache
                        .get_airlock_tracker()
                        .append_commit(&commit_info)
                    {
                        // append commit content
                        self.meta_cache.append_tree_delta(commit_info.get_content());
                    }
                }
                _ => panic!(
                    "Unexpected meta_append_type {}, only support {} and {} ",
                    meta_append_type,
                    MetaAppendType::LockReq as u8,
                    MetaAppendType::CommitInfo as u8
                ),
            }
        }
        self.meta_cache.forward_last_seg_pos(inc_size as u64);
        Ok(())
    }

    fn check_commit(&mut self, conn: &dyn StorageConnector, lock_id: &AirLockID) -> bool {
        //refresh meta
        self.refresh_meta(conn).expect("failed to refresh meta");
        self.meta_cache.get_airlock_tracker().check_commit(lock_id)
    }
}

impl Segment for MetaSegment {
    fn get_seginfo(&self) -> &SegmentInfo {
        &self.seg_info
    }

    fn read_all(&mut self, _conn: &dyn StorageConnector) -> GResult<DataSlice> {
        //won't support
        todo!()
    }

    fn read_range(&mut self, _conn: &dyn StorageConnector, _range: &Range) -> GResult<DataSlice> {
        //won't support
        todo!()
    }

    fn seal(&self, _conn: &dyn StorageConnector) -> GResult<()> {
        //won't support
        todo!()
    }
}

// //TODO: add lock request to enable this test
// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     use chrono::Utc;
//     use tempfile::TempDir;
//     use uuid::Uuid;

//     use crate::{
//         common::error::GResult,
//         consistency::airlock::{AirLockID, AirLockRequest, CommitInfo},
//         io::{
//             fake_store_service_conn::FakeStoreServiceConnector, file_utils::UrlUtil,
//             storage_connector::StorageConnector,
//         },
//         lsmt::{
//             level_seg_desc::SegDesc,
//             tree_delta::{LevelDelta, TreeDelta},
//         },
//         storage::{
//             meta::Meta,
//             meta_segment::MetaSegment,
//             segment::{SegmentInfo, SegmentType},
//         },
//     };

//     #[test]
//     fn meta_segment_test() -> GResult<()> {
//         //test tail Segment
//         let mut first_conn = FakeStoreServiceConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         first_conn.open(fake_props)?;

//         let fake_seg_id = 0u32;
//         let fake_client_id = Uuid::new_v4();
//         let temp_dir = TempDir::new()?;
//         let fake_path = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
//         let mut seg = MetaSegment::new(
//             SegmentInfo::new(fake_seg_id, fake_path, 0, SegmentType::MetaSegment),
//             &first_conn,
//             fake_client_id,
//         );

//         // simulate operations from clients and compactors
//         // 1. add tail
//         let default_tail = SegDesc::default();
//         let default_tail_id = default_tail.get_id();
//         // seg.append_tree_delta(&TreeDelta::new_tail_delta(default_tail.clone()))?;
//         let commit_info = CommitInfo::new(
//             AirLockID::new(fake_client_id, Utc::now()),
//             TreeDelta::new_tail_delta(default_tail.clone()),
//         );
//         seg.append_commit_info(commit_info);
//         assert_eq!(default_tail_id, seg.get_refreshed_tail()?);

//         // 2. update tail
//         let new_tail = SegDesc::new(default_tail_id + 1, None, None);
//         let new_tail_id = new_tail.get_id();
//         // seg.append_tree_delta(&TreeDelta::update_tail_delta(
//         //     default_tail.clone(),
//         //     new_tail.clone(),
//         // ))?;
//         let commit_info_1 = CommitInfo::new(
//             AirLockID::new(fake_client_id, Utc::now()),
//             TreeDelta::update_tail_delta(default_tail.clone(), new_tail.clone()),
//         );
//         seg.append_commit_info(commit_info_1);

//         assert_eq!(new_tail_id, seg.get_refreshed_tail()?);

//         let new_tail_1 = SegDesc::new(new_tail_id + 1, None, None);
//         let new_tail_id_1 = new_tail_1.get_id();
//         // seg.append_tree_delta(&TreeDelta::update_tail_delta(new_tail.clone(), new_tail_1))?;

//         let commit_info_2 = CommitInfo::new(
//             AirLockID::new(fake_client_id, Utc::now()),
//             TreeDelta::update_tail_delta(new_tail.clone(), new_tail_1),
//         );
//         seg.append_commit_info(commit_info_2);
//         assert_eq!(new_tail_id_1, seg.get_refreshed_tail()?);

//         // 3. add lock request
//         let fake_client_id = Uuid::new_v4();
//         let fake_resources = vec![0u32, 1u32];
//         let lock_req = AirLockRequest::new(fake_resources, fake_client_id);
//         seg.append_lock_request(&lock_req)?;
//         assert_eq!(new_tail_id_1, seg.get_refreshed_tail()?);
//         //TODO: refresh meta and verify the lock request.

//         // 4. update the lsm structure after compaction
//         // compact default_tail(id: 0) + new_tail(id: 1) => L1 segment(id: 10)
//         let l1_seg = SegDesc::new(10, None, None);
//         let l1_seg_id = l1_seg.get_id();
//         let levels_delta = vec![
//             LevelDelta::new(0, false, vec![default_tail, new_tail]),
//             LevelDelta::new(1, true, vec![l1_seg]),
//         ];

//         let commit_info_3 = CommitInfo::new(
//             AirLockID::new(fake_client_id, Utc::now()),
//             TreeDelta::new(levels_delta),
//         );

//         seg.append_commit_info(commit_info_3);
//         // seg.append_tree_delta(&TreeDelta::new(levels_delta))?;
//         seg.refresh_meta()?;
//         let cur_tail_id = seg.get_tail_from_cache();
//         let lsm_desc = seg.get_refreshed_tree_desc()?;
//         assert_eq!(new_tail_id_1, cur_tail_id);
//         // check tail segment
//         assert_eq!(new_tail_id_1, lsm_desc.get_tail());
//         // check level number
//         assert_eq!(2, lsm_desc.get_level_num());
//         // check segments in level 0
//         assert!(lsm_desc.get_level_segs(0).is_empty());
//         assert!(lsm_desc.get_level_desc(0).get_seg_num() == 0);
//         // check segments in level 1
//         assert_eq!(vec![l1_seg_id], lsm_desc.get_level_segs(1));
//         Ok(())
//     }
// }
