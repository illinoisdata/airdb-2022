use url::Url;

use crate::{consistency::airlock::ResourceID, db::rw_db::ClientID};

use super::segment::{SegID, SegmentType};

pub static DATA_SEG_ID_MIN: SegID = 1 << 62;
pub static META_SEG_ID: SegID = 0;
pub static PLACEHOLDER_DATASEG_ID: SegID = 0;

/**
 * For each 64-bit SegID(the first three parts make up the header)
 * 1. the first two bits denote segment type
 * 2. the third to tenth bits denote level number
 * 3. the eleventh to the 32nd bits denote the pure segment id
 * 4. the last 32 bits denote client id (only necessary for optimistic lock strategy)
 */
pub struct SegIDUtil {}

#[allow(unused)]
impl SegIDUtil {
    pub fn is_meta(seg_id: SegID) -> bool {
        SegIDUtil::get_seg_type(seg_id).is_meta()
    }

    pub fn is_optimistic_segid(seg_id: SegID) -> bool {
        (seg_id << 32) != 0
    }

    pub fn get_non_optimistic_segid(seg_id: SegID) -> u32 {
        (seg_id >> 32) as u32
    }

    pub fn from_non_optimistic_segid(seg_id: u32) -> SegID {
        (seg_id as SegID) << 32
    }

    pub fn get_seg_dir(seg_id: SegID, home_dir: &Url) -> Url {
        let level = SegIDUtil::get_level(seg_id);
        if SegIDUtil::is_meta(seg_id) {
            home_dir
                .join(&format!("meta_{}", seg_id))
                .unwrap_or_else(|_| {
                    panic!(
                        "Cannot generate a path for meta dir {}, seg id {}",
                        home_dir, seg_id
                    )
                })
        } else {
            let pure_id = SegIDUtil::get_pure_id(seg_id);
            let client_id_option = SegIDUtil::get_client_id(seg_id);
            if let Some(client_id) = client_id_option {
                home_dir
                    .join(&format!("data{}_{}_{}", level, pure_id, client_id))
                    .unwrap_or_else(|_| {
                        panic!(
                            "Cannot generate a path for home dir {}, pure segid {} in level {} with clientid {}",
                            home_dir, pure_id, level, client_id
                        )
                    })
            } else {
                home_dir
                    .join(&format!("data{}_{}", level, pure_id))
                    .unwrap_or_else(|_| {
                        panic!(
                            "Cannot generate a path for home dir {}, pure segid {} and level {}",
                            home_dir, pure_id, level
                        )
                    })
            }
        }
    }

    // get the pure id for each seg
    pub fn get_pure_id(seg_id: SegID) -> u32 {
        // the eleventh bit to the end denote the pure id
        ((seg_id << 10) >> 42) as u32
    }

    pub fn get_client_id(seg_id: SegID) -> Option<ClientID> {
        if SegIDUtil::is_optimistic_segid(seg_id) {
            Some(((seg_id << 32) >> 32) as ClientID)
        } else {
            None
        }
    }

    pub fn get_level(seg_id: SegID) -> u8 {
        // the third to tenth bits denote the level number
        ((seg_id << 2) >> 56) as u8
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
        ((seg_type as SegID) << 62) | ((level as SegID) << 54)
    }

    pub fn get_seg_type(seg_id: SegID) -> SegmentType {
        // the first two bits denote segment type
        SegmentType::try_from((seg_id >> 62) as u32)
    }

    pub fn is_uninit_tail(seg_id: SegID) -> bool {
        seg_id == PLACEHOLDER_DATASEG_ID
    }

    pub fn is_new_tail(new_tail: SegID, old_tail: SegID) -> bool {
        new_tail > old_tail
    }

    pub fn gen_next_tail(old_tail_id: SegID) -> SegID {
        if SegIDUtil::is_uninit_tail(old_tail_id) {
            DATA_SEG_ID_MIN
        } else {
            old_tail_id + (1u64 << 32)
        }
    }

    // the first 32 bits of the segid
    pub fn get_segid_header(seg_id: SegID) -> u32 {
        (seg_id >> 32) as u32
    }

    pub fn get_resid_from_segid(seg_id: SegID) -> ResourceID {
        (seg_id >> 32) << 32
    }

    pub fn has_prev_tail(tail_id: SegID) -> bool {
        !(SegIDUtil::get_segid_header(tail_id) == SegIDUtil::get_segid_header(DATA_SEG_ID_MIN)
            || SegIDUtil::is_uninit_tail(tail_id))
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
            (2u64 << 62) | (1u64 << 54),
            SegIDUtil::get_dataseg_id_min(1)
        );
        assert_eq!(1, SegIDUtil::get_level(SegIDUtil::get_dataseg_id_min(1)));
        assert_eq!(2, SegIDUtil::get_level(SegIDUtil::get_dataseg_id_min(2)));

        Ok(())
    }
}
