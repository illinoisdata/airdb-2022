use crate::common::{bytebuffer::ByteBuffer, error::GResult, readbuffer::ReadBuffer, serde::Serde};

use super::level_seg_desc::{SegDesc, LevelSegDesc};

pub struct LevelDelta {
    /// when level_id == u8::MAX, it denotes the update of the tail seg
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
            level_id: u8::MAX,
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
    pub fn get_segs(&self) -> &Vec<SegDesc> {
        &self.segs
    }

    pub fn is_tail_update(&self) -> bool {
        self.level_id == u8::MAX
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

    pub fn new_tail_delta(tail: SegDesc) -> Self {
        Self {
            levels_delta: vec![LevelDelta::new_tail(tail)],
        }
    }

    pub fn update_tail_delta(old_tail: SegDesc, new_tail: SegDesc) -> Self {
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
