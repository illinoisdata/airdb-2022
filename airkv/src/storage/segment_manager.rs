use std::collections::HashMap;

use crate::io::storage_connector::StorageConnector;

use super::{
    data_segment::DataSegment,
    meta_segment::MetaSegment,
    segment::{SegID, SegmentInfo},
};

#[allow(dead_code)]
pub struct SegmentManager<'a> {
    storage_connector: &'a (dyn StorageConnector),
    data_cache: HashMap<SegID, DataSegment<'a>>,
    meta_cache: MetaSegment<'a>,
}

#[allow(dead_code)]
impl<'a> SegmentManager<'a> {
    fn get_data_seg(&mut self, seg_info: SegmentInfo) -> &'a mut DataSegment {
        let seg_id = seg_info.get_id();
        self.data_cache
            .entry(seg_id)
            .or_insert_with(|| DataSegment::new(seg_info, self.storage_connector))
    }

    fn get_meta_set(&mut self) -> &'a mut MetaSegment {
        &mut self.meta_cache
    }
}
