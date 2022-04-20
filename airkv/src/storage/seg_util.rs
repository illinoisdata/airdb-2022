use crate::lsmt::level_seg_desc::PLACEHOLDER_DATASEG_ID;

use super::segment::{SegID, SegmentType};

pub static DATA_SEG_ID_MIN: SegID = 1 << 30;
pub static META_SEG_ID: SegID = 0;  

pub struct SegIDUtil {}

#[allow(unused)]
impl SegIDUtil {
    pub fn is_meta(seg_id: SegID) -> bool {
        SegIDUtil::get_seg_type(seg_id).is_meta()
    }

    pub fn get_level(seg_id: SegID) -> u8 {
        // the third to tenth bits denote the level number
        ((seg_id << 2) >> 24) as u8
    }

    pub fn get_global_first_tailid() -> SegID {
        DATA_SEG_ID_MIN
    }

    pub fn get_dataseg_id_min(level: u8) -> SegID {
        let seg_type = if level == 0 {
            SegmentType::DataSegmentL0
        } else {
            SegmentType::DataSegmentLn
        };
        ((seg_type as SegID) << 30) | ((level as SegID) << 22)
    }

    pub fn get_seg_type(seg_id: SegID) -> SegmentType {
        // the first two bits denote segment type
        SegmentType::try_from(seg_id >> 30)
    }

    pub fn is_uninit_tail(seg_id: SegID) -> bool {
        seg_id == PLACEHOLDER_DATASEG_ID
    }

    pub fn is_new_tail(target_tail:SegID, cur_tail:SegID) -> bool {
        target_tail > cur_tail
    }

    pub fn gen_next_tail(old_tail_id: SegID) -> SegID {
        if SegIDUtil::is_uninit_tail(old_tail_id) {
            DATA_SEG_ID_MIN
        } else {
            old_tail_id + 1
        }
    }

    pub fn gen_prev_tail(tail_id: SegID) -> Option<SegID> {
        if tail_id == DATA_SEG_ID_MIN || SegIDUtil::is_uninit_tail(tail_id) {
            // if it is the first tail or in uninit status, there is no prev tail
            None
        } else {
            Some(tail_id - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        common::error::GResult,
        storage::seg_util::{SegIDUtil, DATA_SEG_ID_MIN},
    };

    #[test]
    fn seg_util_test() -> GResult<()> {
        assert_eq!(DATA_SEG_ID_MIN, SegIDUtil::get_dataseg_id_min(0));
        assert_eq!(0, SegIDUtil::get_level(DATA_SEG_ID_MIN));
        assert_eq!(
            (2u32 << 30) | (1u32) << 22,
            SegIDUtil::get_dataseg_id_min(1)
        );
        assert_eq!(1, SegIDUtil::get_level(SegIDUtil::get_dataseg_id_min(1)));
        assert_eq!(2, SegIDUtil::get_level(SegIDUtil::get_dataseg_id_min(2)));

        Ok(())
    }
}
