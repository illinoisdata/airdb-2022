use crate::{
    cache::meta_cache::{MetaCache, MetaIncrement},
    common::{
        bytebuffer::ByteBuffer, dataslice::DataSlice, error::GResult, readbuffer::ReadBuffer,
        serde::Serde,
    },
    consistancy::airlock::AirLockRequest,
    io::{file_utils::Range, storage_connector::StorageConnector},
    lsmt::{level_seg_desc::LsmTreeDesc, tree_delta::TreeDelta},
};

use super::{
    meta::Meta,
    segment::{SegID, Segment, SegmentInfo},
};

/// There are two types of append data in MetaSegment
enum MetaAppendType {
    TreeDelta = 0,
    LockReq = 1,
}

pub struct MetaSegment<'a> {
    meta_cache: MetaCache,
    seg_info: SegmentInfo,
    storage_connector: &'a (dyn StorageConnector),
}

impl<'a> MetaSegment<'a> {
    pub fn new(
        seg_info_new: SegmentInfo,
        storage_connector_new: &'a (dyn StorageConnector),
    ) -> Self {
        Self {
            meta_cache: MetaCache::default(),
            seg_info: seg_info_new,
            storage_connector: storage_connector_new,
        }
    }
}

#[allow(unused_variables)]
impl<'a> Meta for MetaSegment<'a> {
    fn get_refreshed_tail(&mut self) -> GResult<SegID> {
        // refresh meta
        self.refresh_meta()?;
        // get tail from cache
        self.get_tail_from_cache()
    }

    fn get_refreshed_tree_desc(&mut self) -> GResult<LsmTreeDesc> {
        // refresh meta
        self.refresh_meta()?;
        self.get_tree_desc_from_cache()
    }

    fn get_tail_from_cache(&self) -> GResult<SegID> {
        Ok(self.meta_cache.get_tail())
    }

    fn get_tree_desc_from_cache(&self) -> GResult<LsmTreeDesc> {
        Ok(self.meta_cache.get_tree_desc())
    }

    fn append_tree_delta(&self, delta: &TreeDelta) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        // write MetaAppendType
        buffer.write_u8(MetaAppendType::TreeDelta as u8);
        delta.serialize(&mut buffer)?;
        self.storage_connector
            .append(self.seg_info.get_seg_path(), buffer.to_view())?;
        Ok(())
    }

    fn append_lock_request(&self, lock_req: &AirLockRequest) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        // write MetaAppendType
        buffer.write_u8(MetaAppendType::LockReq as u8);
        lock_req.serialize(&mut buffer)?;
        self.storage_connector
            .append(self.seg_info.get_seg_path(), buffer.to_view())?;
        Ok(())
    }

    fn verify_lock_status(&self, lock_req: &AirLockRequest) -> GResult<()> {
        todo!()
    }

    fn refresh_meta(&mut self) -> GResult<()> {
        let last_pos = self.meta_cache.get_last_pos();
        let path = self.seg_info.get_seg_path();
        let mut inc_buffer = ByteBuffer::wrap(
            self.get_connector()
                .read_range(path, &Range::new(last_pos, 0))?,
        );
        let inc_size = inc_buffer.len();
        let mut lock_reqs: Vec<AirLockRequest> = Vec::new();
        let mut tree_deltas: Vec<TreeDelta> = Vec::new();
        while inc_buffer.has_remaining() {
            let meta_append_type = inc_buffer.read_u8();
            match meta_append_type {
                x if x == MetaAppendType::LockReq as u8 => {
                    lock_reqs.push(AirLockRequest::deserialize(&mut inc_buffer))
                }
                y if y == MetaAppendType::TreeDelta as u8 => {
                    tree_deltas.push(TreeDelta::deserialize(&mut inc_buffer))
                }
                _ => panic!(
                    "Unexpected meta_append_type {}, only support {} and {} ",
                    meta_append_type,
                    MetaAppendType::LockReq as u8,
                    MetaAppendType::TreeDelta as u8
                ),
            }
        }

        self.meta_cache
            .append_inc(MetaIncrement::new(inc_size as u64,lock_reqs, tree_deltas))
    }
}

#[allow(unused_variables)]
impl<'a> Segment for MetaSegment<'a> {
    fn get_seginfo(&self) -> &SegmentInfo {
        &self.seg_info
    }

    fn get_connector(&self) -> &dyn StorageConnector {
        self.storage_connector
    }

    fn read_all(&mut self) -> GResult<DataSlice> {
        //won't support
        todo!()
    }

    fn read_range(&mut self, range: &Range) -> GResult<DataSlice> {
        //won't support
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{
        common::error::GResult,
        consistancy::airlock::AirLockRequest,
        io::{
            fake_store_service_conn::FakeStoreServiceConnector, file_utils::UrlUtil,
            storage_connector::StorageConnector,
        },
        lsmt::{
            level_seg_desc::SegDesc,
            tree_delta::{LevelDelta, TreeDelta},
        },
        storage::{
            meta::Meta,
            meta_segment::MetaSegment,
            segment::{SegmentInfo, SegmentType},
        },
    };

    #[test]
    fn meta_segment_test() -> GResult<()> {
        //test tail Segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;

        let fake_seg_id = 0u32;
        let temp_dir = TempDir::new()?;
        let fake_path = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        let mut seg = MetaSegment::new(
            SegmentInfo::new(fake_seg_id, fake_path, 0, SegmentType::MetaSegment),
            &first_conn,
        );

        // simulate operations from clients and compactors
        // 1. add tail
        let default_tail = SegDesc::default();
        let default_tail_id = default_tail.get_id();
        seg.append_tree_delta(&TreeDelta::new_tail_delta(default_tail.clone()))?;
        assert_eq!(default_tail_id, seg.get_refreshed_tail()?);

        // 2. update tail
        let new_tail = SegDesc::new(default_tail_id + 1, None, None);
        let new_tail_id = new_tail.get_id();
        seg.append_tree_delta(&TreeDelta::update_tail_delta(
            default_tail.clone(),
            new_tail.clone(),
        ))?;
        assert_eq!(new_tail_id, seg.get_refreshed_tail()?);

        let new_tail_1 = SegDesc::new(new_tail_id + 1, None, None);
        let new_tail_id_1 = new_tail_1.get_id();
        seg.append_tree_delta(&TreeDelta::update_tail_delta(new_tail.clone(), new_tail_1))?;
        assert_eq!(new_tail_id_1, seg.get_refreshed_tail()?);

        // 3. add lock request
        let fake_client_id = Uuid::new_v4();
        let fake_timestamp = Utc::now();
        let fake_resources = vec![0u32, 1u32];
        let lock_req = AirLockRequest::new(fake_resources, fake_client_id, fake_timestamp);
        seg.append_lock_request(&lock_req)?;
        assert_eq!(new_tail_id_1, seg.get_refreshed_tail()?);
        //TODO: refresh meta and verify the lock request.

        // 4. update the lsm structure after compaction
        // compact default_tail(id: 0) + new_tail(id: 1) => L1 segment(id: 10)
        let l1_seg = SegDesc::new(10, None, None);
        let l1_seg_id = l1_seg.get_id();
        let levels_delta = vec![
            LevelDelta::new(0, false, vec![default_tail, new_tail]),
            LevelDelta::new(1, true, vec![l1_seg]),
        ];
        seg.append_tree_delta(&TreeDelta::new(levels_delta))?;
        seg.refresh_meta()?;
        let cur_tail_id = seg.get_tail_from_cache()?;
        let lsm_desc = seg.get_refreshed_tree_desc()?;
        assert_eq!(new_tail_id_1, cur_tail_id);
        // check tail segment
        assert_eq!(new_tail_id_1, lsm_desc.get_tail());
        // check level number
        assert_eq!(2, lsm_desc.get_level_num());
        // check segments in level 0
        assert!(lsm_desc.get_level_segs(0).is_empty());
        assert!(lsm_desc.get_level_desc(0).get_seg_num() == 0);
        // check segments in level 1 
        assert_eq!(vec![l1_seg_id], lsm_desc.get_level_segs(1));
        Ok(())
    }
  
}
