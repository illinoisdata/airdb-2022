use std::collections::HashMap;

use url::Url;

use crate::{common::error::GResult, io::storage_connector::StorageConnector, db::rw_db::ClientID};

use super::{
    data_segment::DataSegment,
    meta::Meta,
    meta_segment::MetaSegment,
    seg_util::SegIDUtil,
    segment::{SegID, Segment, SegmentInfo},
};

pub type DataCache = HashMap<SegID, DataSegment>;

#[allow(unused)]
pub struct SegmentManager {
    data_cache: DataCache,
    meta_cache: MetaSegment,
    home_dir: Url,
}

impl SegmentManager {
    pub fn new(client_id_new: ClientID, home_dir_new: Url) -> SegmentManager {
        SegmentManager {
            data_cache: HashMap::default(),
            meta_cache: MetaSegment::new(
                SegmentInfo::new_meta(home_dir_new.clone()),
                client_id_new,
            ),
            home_dir: home_dir_new,
        }
    }

    pub fn get_home_dir(&self) -> &Url {
        &self.home_dir
    }

    //TODO: find a better way to get datasegment
    pub fn get_data_seg(&mut self, seg_id: SegID) -> &mut DataSegment {
        let dir = &self.home_dir;
        self.data_cache
            .entry(seg_id)
            .or_insert_with(|| DataSegment::new(SegmentInfo::new_from_id(seg_id, dir.clone())))
    }

    pub fn create_new_tail_seg(
        &mut self,
        conn: &dyn StorageConnector,
        seg_id: SegID,
    ) -> GResult<()> {
        let dir = &self.home_dir;
        self.data_cache
            .entry(seg_id)
            .or_insert_with(|| DataSegment::new(SegmentInfo::new_from_id(seg_id, dir.clone())))
            .create(conn)
    }

    // TODO:  find a better way to get metasegment
    pub fn get_cached_tail_seg(&mut self) -> &mut DataSegment {
        self.get_data_seg(self.meta_cache.get_tail_from_cache())
    }

    pub fn get_cached_tail_id(&self) -> SegID {
        self.get_meta_seg().get_tail_from_cache()
    }

    pub fn has_valid_tail(&self) -> bool {
        !SegIDUtil::is_uninit_tail(self.get_meta_seg().get_tail_from_cache())
    }

    pub fn get_updated_tail_seg(&mut self, conn: &dyn StorageConnector) -> &mut DataSegment {
        match self.meta_cache.get_refreshed_tail(conn) {
            Ok(seg_id) => self.get_data_seg(seg_id),
            Err(err) => panic!("get refreshed tail failed: {}", err),
        }
    }

    pub fn get_mut_meta_seg(&mut self) -> &mut MetaSegment {
        &mut self.meta_cache
    }

    pub fn get_meta_seg(&self) -> &MetaSegment {
        &self.meta_cache
    }

    pub fn refresh_meta(&mut self, conn: &dyn StorageConnector) -> GResult<()> {
        self.meta_cache.refresh_meta(conn)
    }
}
