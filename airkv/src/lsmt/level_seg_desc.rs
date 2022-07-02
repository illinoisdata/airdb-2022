use std::{
    cmp::Ordering,
    collections::HashSet,
    fmt::{Display, Formatter},
};

use itertools::Itertools;

use crate::{
    common::{bytebuffer::ByteBuffer, error::GResult, readbuffer::ReadBuffer, serde::Serde},
    consistency::airlock::ResourceID,
    storage::{
        seg_util::{SegIDUtil, PLACEHOLDER_DATASEG_ID},
        segment::SegID,
    },
};

use super::tree_delta::{LevelDelta, TreeDelta};

// SegDesc describes a data segment, it won't be used to describe the meta segment
#[derive(Clone, Debug)]
pub struct SegDesc {
    seg_id: SegID,
    ///
    /// Segments in L1-LN hold min max stats    
    ///
    /// TODO: change the type of min/max into fixed-length array  
    ///
    min: Option<Vec<u8>>,
    max: Option<Vec<u8>>,
}

impl Default for SegDesc {
    fn default() -> Self {
        Self {
            seg_id: PLACEHOLDER_DATASEG_ID,
            min: None,
            max: None,
        }
    }
}

impl Ord for SegDesc {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.min.is_some() {
            // when min/max stats exist
            let range_order = self.min.as_ref().unwrap().cmp(other.min.as_ref().unwrap());
            if range_order.is_eq() {
                // when min/max stats of both sides are equal
                self.seg_id.cmp(&other.seg_id)
            } else {
                range_order
            }
        } else {
            // when there is no min/max stats
            self.seg_id.cmp(&other.seg_id)
        }
    }
}

impl PartialOrd for SegDesc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SegDesc {
    fn eq(&self, other: &Self) -> bool {
        self.seg_id.eq(&other.seg_id)
    }
}

impl Eq for SegDesc {}

impl SegDesc {
    pub fn new(id_new: SegID, min_new: Option<Vec<u8>>, max_new: Option<Vec<u8>>) -> Self {
        Self {
            seg_id: id_new,
            min: min_new,
            max: max_new,
        }
    }

    pub fn new_from_id(seg_id_new: SegID) -> Self {
        Self {
            seg_id: seg_id_new,
            min: None,
            max: None,
        }
    }

    pub fn get_id(&self) -> SegID {
        self.seg_id
    }

    pub fn has_stats(&self) -> bool {
        self.min.is_some()
    }

    pub fn get_min_slice(&self) -> &[u8] {
        self.min.as_ref().unwrap()
    }

    pub fn get_max_slice(&self) -> &[u8] {
        self.max.as_ref().unwrap()
    }
}

impl Serde<SegDesc> for SegDesc {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        //TODO: find a better way to distinguish optimistic/non-optimistic seg_id
        let is_optimistic = SegIDUtil::is_optimistic_segid(self.seg_id);
        buff.write_bool(is_optimistic);
        if is_optimistic {
            buff.write_u64(self.seg_id);
        } else {
            buff.write_u32(SegIDUtil::get_non_optimistic_segid(self.seg_id));
        }
        let has_stats = self.min.is_some() && self.max.is_some();
        buff.write_bool(has_stats);
        if has_stats {
            let min_value = self.min.as_ref().unwrap();
            let max_value = self.max.as_ref().unwrap();

            buff.write_u16(min_value.len() as u16);
            buff.write_bytes(min_value);
            buff.write_u16(max_value.len() as u16);
            buff.write_bytes(max_value);
        } else {
            // TODO: remove this check later
            assert!(self.min.is_none());
            assert!(self.max.is_none());
        }
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> SegDesc {
        let is_optimistic = buff.read_bool();
        let seg_id_read = if is_optimistic {
            buff.read_u64()
        } else {
            SegIDUtil::from_non_optimistic_segid(buff.read_u32())
        };
        let has_stat = buff.read_bool();
        if has_stat {
            let min_size = buff.read_u16();
            let min_read = buff.read_bytes(min_size as usize);
            let max_size = buff.read_u16();
            let max_read = buff.read_bytes(max_size as usize);
            SegDesc::new(seg_id_read, Some(min_read), Some(max_read))
        } else {
            SegDesc::new(seg_id_read, None, None)
        }
    }
}

#[derive(Clone, Debug)]
pub struct LevelSegDesc {
    /// The number of segments in the level.
    seg_num: u32,
    segs_desc: Vec<SegDesc>,
}

impl Display for LevelSegDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LevelSegDesc(seg_num: {}, segs_desc: {:?})",
            self.seg_num,
            self.segs_desc
                .iter()
                .map(|seg| SegIDUtil::get_readable_segid(seg.get_id()))
                .collect::<Vec<String>>(),
        )
    }
}

impl LevelSegDesc {
    pub fn new(seg_num_new: u32, segs_desc_new: Vec<SegDesc>) -> Self {
        Self {
            seg_num: seg_num_new,
            segs_desc: segs_desc_new,
        }
    }

    pub fn set_seg_num(&mut self, num: u32) {
        self.seg_num = num;
    }

    pub fn get_seg_num(&self) -> u32 {
        self.seg_num
    }

    pub fn get_segs(&self) -> Vec<SegDesc> {
        //TODO: find a optimized structure for segs_desc
        self.segs_desc.clone()
    }

    pub fn append_level_delta(&mut self, level_delta: &LevelDelta) {
        // the level_delta denote a normal tree structure update
        if level_delta.is_add() {
            //TODO: optimize LevelSegDesc structure to better support tree search
            self.append_segs(level_delta.get_segs())
        } else {
            //TODO: optimize LevelSegDesc to speed up deletion
            self.remove_segs(level_delta.get_segs())
        }
    }

    fn append_segs(&mut self, segs: &[SegDesc]) {
        // self.segs_desc.extend_from_slice(segs);
        //TODO: optimize for performance
        segs.iter().for_each(|seg| {
            let insert_seg = seg.clone();
            let target_pos = self.segs_desc.iter().enumerate().find(|(_id, seg)| {
                SegIDUtil::get_pure_id(seg.get_id()) > SegIDUtil::get_pure_id(insert_seg.get_id())
            });
            if let Some((idx, _seg)) = target_pos {
                self.segs_desc.insert(idx, insert_seg)
            } else {
                // can't find an element larger than insert_seg, append to the tail
                self.segs_desc.push(insert_seg);
            }
        });
        self.seg_num = self.segs_desc.len() as u32;
    }

    fn remove_segs(&mut self, segs: &[SegDesc]) {
        // TODO(L0): optimize to speed up deletion and throw exception if target segs are not found in segs_desc
        let remove_res: HashSet<ResourceID> = HashSet::from_iter(
            segs.iter()
                .map(|seg_desc| SegIDUtil::get_resid_from_segid(seg_desc.get_id())),
        );
        self.segs_desc
            .retain(|x| !remove_res.contains(&SegIDUtil::get_resid_from_segid(x.get_id())));
        self.seg_num = self.segs_desc.len() as u32;
    }
}

//TODO: design a space-efficient way to clone or share the LsmTreeDesc
#[derive(Clone, Default, Debug)]
pub struct LsmTreeDesc {
    /// The number of levels in the LSM-Tree
    level_num: u8,
    levels_desc: Vec<LevelSegDesc>,
    tail_desc: SegDesc,
}

impl Display for LsmTreeDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LsmTreeDesc(level_num: {}, levels_desc: {:?}, tail_desc: {})",
            self.level_num,
            self.levels_desc
                .iter()
                .map(|level| format!("{}", level))
                .collect::<Vec<String>>(),
            SegIDUtil::get_readable_segid(self.tail_desc.get_id())
        )
    }
}

impl LsmTreeDesc {
    pub fn new_from_tail(tail_new: SegDesc) -> Self {
        Self {
            level_num: 0,
            levels_desc: Vec::new(),
            tail_desc: tail_new,
        }
    }

    pub fn new(
        level_num_new: u8,
        levels_desc_new: Vec<LevelSegDesc>,
        tail_desc_new: SegDesc,
    ) -> Self {
        Self {
            level_num: level_num_new,
            levels_desc: levels_desc_new,
            tail_desc: tail_desc_new,
        }
    }

    pub fn has_tail(&self) -> bool {
        self.tail_desc.get_id() != PLACEHOLDER_DATASEG_ID
    }

    pub fn get_level_num(&self) -> u8 {
        self.level_num
    }

    pub fn get_tail(&self) -> SegID {
        self.tail_desc.seg_id
    }

    pub fn get_level_desc(&self, level: u8) -> &LevelSegDesc {
        &self.levels_desc[level as usize]
    }

    pub fn append_tree_deltas(&mut self, tree_deltas: Vec<TreeDelta>) {
        for tree_delta in tree_deltas.iter() {
            self.append_tree_delta(tree_delta)
        }
    }

    pub fn append_tree_delta(&mut self, tree_delta: &TreeDelta) {
        for level_delta in tree_delta.get_levels_delta().iter() {
            if level_delta.is_tail_update() {
                // the level_delta denote a tail update
                self.tail_desc = level_delta.get_segs()[0].clone();
            } else {
                let level = level_delta.get_level();
                assert!(
                    level < self.level_num || (level == self.level_num && level_delta.is_add())
                );
                if level == self.level_num {
                    // add a new level
                    self.levels_desc.push(level_delta.to_level_desc());
                    self.level_num = self.levels_desc.len() as u8;
                } else {
                    // modify an existing level
                    self.levels_desc[level as usize].append_level_delta(level_delta);
                }
            }
        }
    }

    pub fn get_read_sequence(&self, search_key: &[u8]) -> Vec<SegDesc> {
        //TODO: optimize to improve performance
        self.levels_desc
            .iter()
            .flat_map(|level_desc| level_desc.get_segs())
            // .filter(|seg| {
            //     !seg.has_stats() || {
            //         seg.get_min_slice() <= search_key && seg.get_min_slice() >= search_key
            //         // seg.get_min_slice().cmp(search_key) == Ordering::Less
            //         //     && seg.get_max_slice().cmp(search_key) == Ordering::Greater
            //     }
            // })
            .sorted_by_key(|seg_desc| -(SegIDUtil::get_pure_id(seg_desc.get_id()) as i32))
            .collect()
    }
}
