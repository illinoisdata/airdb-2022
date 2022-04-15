
use super::segment::{SegID, SegmentType};

pub static DATA_SEG_ID_MIN: SegID = 1;

pub struct SegIDUtil {}

#[allow(unused)]
impl SegIDUtil {
    pub fn is_meta(seg_id: SegID) -> bool {
        SegIDUtil::get_seg_type(seg_id).is_meta() 
    }

    pub fn get_level(seg_id: SegID) -> u8 {
        //TODO: return the real level
        // use 0 at the moment(because we don't implement compaction yet)
        0
    }

    pub fn get_seg_type(seg_id: SegID) -> SegmentType {
        SegmentType::try_from(seg_id >> 30)  
    }

    pub fn gen_next_tail(old_tail_id: SegID) -> SegID {
        old_tail_id + 1
    }

    pub fn gen_prev_tail(tail_id: SegID) -> SegID {
        if tail_id == DATA_SEG_ID_MIN {
            panic!("tail_id {} has no prev tail", tail_id)
        } else {
            tail_id - 1
        }
    }
}