use crate::{
    common::{bytebuffer::ByteBuffer, error::GResult, readbuffer::ReadBuffer, serde::Serde},
    compaction::compaction_task::TaskDesc,
    storage::{seg_util::SegIDUtil, segment::SegID},
};

use super::level_seg_desc::{LevelSegDesc, SegDesc};

pub static TAIL_LEVEL_ID: u8 = u8::MAX;
pub struct LevelDelta {
    /// when level_id == TAIL_LEVEL_ID(u8::MAX), it denotes the update of the tail seg
    level_id: u8,
    /// true: add the segs
    /// false: delete the segs
    operation: bool,
    /// related segs
    /// assume the size of vec is less than the max value of u16
    segs: Vec<SegDesc>,
}

impl LevelDelta {
    pub fn new(level_id_new: u8, operation_new: bool, segs_new: Vec<SegDesc>) -> Self {
        Self {
            level_id: level_id_new,
            operation: operation_new,
            segs: segs_new,
        }
    }

    pub fn new_tail(seg_new: SegDesc) -> Self {
        Self {
            level_id: TAIL_LEVEL_ID,
            operation: true,
            segs: vec![seg_new],
        }
    }

    // only is_add() delta can be transfered into level desc
    pub fn to_level_desc(&self) -> LevelSegDesc {
        assert!(self.is_add());
        LevelSegDesc::new(self.segs.len() as u32, self.segs.clone())
    }

    pub fn is_add(&self) -> bool {
        self.operation
    }

    pub fn get_level(&self) -> u8 {
        self.level_id
    }

    ///This method give out
    pub fn get_segs(&self) -> &[SegDesc] {
        &self.segs
    }

    pub fn is_tail_update(&self) -> bool {
        self.level_id == TAIL_LEVEL_ID
    }
}

impl Serde<LevelDelta> for LevelDelta {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        buff.write_u8(self.level_id);
        buff.write_bool(self.operation);
        let seg_num = self.segs.len();
        buff.write_u16(seg_num as u16);
        for i in 0..seg_num as usize {
            self.segs[i].serialize(buff)?;
        }
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> LevelDelta {
        let level_id_read = buff.read_u8();
        let operation_read = buff.read_bool();
        let seg_num = buff.read_u16();
        let mut segs_read: Vec<SegDesc> = Vec::with_capacity(seg_num as usize);

        for _i in 0..seg_num {
            segs_read.push(SegDesc::deserialize(buff));
        }
        LevelDelta::new(level_id_read, operation_read, segs_read)
    }
}

pub struct TreeDelta {
    levels_delta: Vec<LevelDelta>,
}

impl TreeDelta {
    pub fn new(levels_delta_new: Vec<LevelDelta>) -> Self {
        Self {
            levels_delta: levels_delta_new,
        }
    }

    pub fn new_from_compation(task_desc: &TaskDesc) -> Self {
        //TODO: add min max info to delta
        let from_level = LevelDelta::new(
            task_desc.get_compact_level(),
            false,
            task_desc
                .get_src_segs()
                .iter()
                .map(|segid| SegDesc::new_from_id(*segid))
                .collect(),
        );
        let to_level = LevelDelta::new(
            task_desc.get_compact_level() + 1,
            true,
            vec![SegDesc::new_from_id(task_desc.get_dest_seg())],
        );
        Self {
            levels_delta: vec![from_level, to_level],
        }
    }

    pub fn new_tail_delta(tail: SegDesc) -> Self {
        Self {
            levels_delta: vec![LevelDelta::new_tail(tail)],
        }
    }

    pub fn new_tail_delta_from_id(tail: SegID) -> Self {
        Self {
            levels_delta: vec![LevelDelta::new_tail(SegDesc::new_from_id(tail))],
        }
    }

    fn update_tail_delta(old_tail: SegDesc, new_tail: SegDesc) -> Self {
        // add new tail
        // move old tail to L0
        let level_vec: Vec<LevelDelta> = vec![
            LevelDelta::new_tail(new_tail),
            LevelDelta::new(0, true, vec![old_tail]),
        ];
        Self {
            levels_delta: level_vec,
        }
    }

    // TODO: avoid using this method, use update_tail_delta_for_newinstead
    pub fn update_tail_delta_from_segid(new_tail: SegID, old_tail: SegID) -> Self {
        if SegIDUtil::has_prev_tail(new_tail) {
            TreeDelta::update_tail_delta(
                SegDesc::new_from_id(old_tail),
                SegDesc::new_from_id(new_tail),
            )
        } else {
            // no old tail
            TreeDelta::new_tail_delta_from_id(new_tail)
        }
    }

    pub fn update_tail_delta_for_new(new_tail: SegDesc, old_tail: SegID) -> Self {
        if SegIDUtil::has_prev_tail(new_tail.get_id()) {
            TreeDelta::update_tail_delta(SegDesc::new_from_id(old_tail), new_tail)
        } else {
            // no old tail
            TreeDelta::new_tail_delta(new_tail)
        }
    }

    // // TODO: avoid using this method, use update_tail_delta_for_newinstead
    // pub fn update_tail_delta_from_segid(new_tail: SegID) -> Self {
    //     let old_tail_op = SegIDUtil::gen_prev_tail(new_tail);
    //     match old_tail_op {
    //         Some(old_tail) => TreeDelta::update_tail_delta(
    //             SegDesc::new_from_id(old_tail),
    //             SegDesc::new_from_id(new_tail),
    //         ),
    //         None => {
    //             // no old tail
    //             TreeDelta::new_tail_delta_from_id(new_tail)
    //         }
    //     }
    // }

    // pub fn update_tail_delta_for_new(new_tail: SegDesc) -> Self {
    //     let old_tail_op = SegIDUtil::gen_prev_tail(new_tail.get_id());
    //     match old_tail_op {
    //         Some(old_tail) => TreeDelta::update_tail_delta(
    //             SegDesc::new_from_id(old_tail),
    //             new_tail,
    //         ),
    //         None => {
    //             // no old tail
    //             TreeDelta::new_tail_delta(new_tail)
    //         }
    //     }
    // }

    pub fn get_levels_delta(&self) -> &[LevelDelta] {
        &self.levels_delta
    }
}

impl Serde<TreeDelta> for TreeDelta {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        let level_num = self.levels_delta.len();
        buff.write_u8(level_num as u8);
        for i in 0..level_num {
            self.levels_delta[i].serialize(buff)?;
        }
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> TreeDelta {
        let level_num = buff.read_u8();
        let mut levels_delta: Vec<LevelDelta> = Vec::with_capacity(level_num as usize);

        for _i in 0..level_num {
            levels_delta.push(LevelDelta::deserialize(buff));
        }
        TreeDelta::new(levels_delta)
    }
}
