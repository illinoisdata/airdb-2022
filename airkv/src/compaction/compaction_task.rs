use std::fmt::{Display, Formatter};
use std::{cmp::Reverse, collections::BinaryHeap};

use rand::prelude::{IteratorRandom, SliceRandom};

use crate::db::rw_db::ClientID;
use crate::storage::seg_util::SegIDUtil;
use crate::storage::segment::SegmentType;
use crate::{
    db::rw_db::Key,
    lsmt::level_seg_desc::LsmTreeDesc,
    storage::segment::{IdxEntry, SegID},
};

//TODO: support more levels. At present, we only support three levels.
pub static COMPACTION_SEGNUM_THRESHOLD: [u32; 3] = [200, 40, 80];
pub static COMPACTION_FANOUT: [u32; 3] = [50, 5, 0];

pub struct TaskDesc {
    // the level number of segments which need to be compacted
    from_level: u8,
    src_segs: Vec<SegID>,
    dest_seg: SegID,
}

impl Display for TaskDesc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TaskDesc(from_leve: {}, src_segs: {:?}, dest_seg: {})",
            self.from_level,
            self.src_segs
                .iter()
                .map(|segid| SegIDUtil::get_pure_id(*segid)),
            SegIDUtil::get_pure_id(self.dest_seg)
        )
    }
}

impl TaskDesc {
    pub fn new(from_level_new: u8, src_segs_new: Vec<SegID>, dest_seg_new: SegID) -> Self {
        Self {
            from_level: from_level_new,
            src_segs: src_segs_new,
            dest_seg: dest_seg_new,
        }
    }

    pub fn get_src_segs(&self) -> &[SegID] {
        &self.src_segs
    }

    pub fn get_dest_seg(&self) -> SegID {
        self.dest_seg
    }

    pub fn get_compact_level(&self) -> u8 {
        self.from_level
    }
}

pub struct TaskScheduler {}

impl TaskScheduler {
    pub fn create_compact_task(lsm_structure: &LsmTreeDesc, client_id: ClientID) -> Option<TaskDesc> {
        let level_candidate = TaskScheduler::find_compaction_level_candidate(lsm_structure);
        match level_candidate {
            Some(level) => TaskScheduler::find_compaction_segs(level, lsm_structure).map(
                |(target_level, segs)| {
                    let first_seg = *segs.first().unwrap();
                    TaskDesc::new(
                        target_level,
                        segs,
                        SegIDUtil::gen_compaction_segid(first_seg, Some(client_id)),
                    )
                },
            ),
            None => None,
        }
    }

    fn find_compaction_level_candidate(lsm_structure: &LsmTreeDesc) -> Option<u8> {
        let level_num = lsm_structure.get_level_num();
        let candidate = (0..level_num)
            .map(|level| {
                let fill_score = lsm_structure.get_level_desc(level).get_seg_num() as f32
                    / COMPACTION_SEGNUM_THRESHOLD[level as usize] as f32;
                (level, fill_score)
            })
            .filter(|(_level, score)| *score > 0.75)
            .max_by(|a, b| a.1.partial_cmp(&(b.1)).unwrap());
        candidate.map(|(level, _score)| level)
    }

    fn find_compaction_segs(
        target_level: u8,
        lsm_struct: &LsmTreeDesc,
    ) -> Option<(u8, Vec<SegID>)> {
        //assume segs in lsm_struct are sorted
        if target_level == 0 {
            match TaskScheduler::find_compaction_candidates_l0(lsm_struct) {
                Some(seg_vec) => Some((target_level, seg_vec)),
                None => {
                    panic!(
                        "ERROR: Can't find compaction candidates in level 0: {:?}",
                        lsm_struct.get_level_desc(0)
                    );
                }
            }
        } else if target_level == 1 {
            let fanout = COMPACTION_FANOUT[target_level as usize];
            let segs = lsm_struct.get_level_desc(target_level).get_segs();
            let rand_rng = &mut rand::thread_rng();
            let groups: Vec<Vec<u32>> = segs
                .iter()
                .map(|seg_desc| SegIDUtil::get_pure_id(seg_desc.get_id()))
                .fold(Vec::new(), |mut acc, seg_id| {
                    if seg_id % fanout == 0 || acc.is_empty() {
                        acc.push(Vec::new());
                    }
                    acc.last_mut().unwrap().push(seg_id);
                    acc
                });
            let candidates: Vec<&Vec<u32>> = groups
                .iter()
                .filter(|group| group.len() == fanout as usize)
                .collect();

            if !candidates.is_empty() {
                let target_group: Vec<SegID> = candidates
                    .choose(rand_rng)
                    .unwrap()
                    .iter()
                    .map(|pure_id| {
                        SegIDUtil::gen_segid(
                            SegmentType::DataSegmentLn,
                            target_level,
                            *pure_id,
                            None,
                        )
                    })
                    .collect();

                Some((target_level, target_group))
            } else {
                // get back to level 0 compaction
                //TODO: try to trace back to L0 compaction by L1 fill-in status
                TaskScheduler::find_compaction_candidates_l0(lsm_struct).map(|segs| (0, segs))
            }
        } else {
            println!("ERROR: only support top 2 layer compaction for now");
            panic!(
                "only support top 2 layer compaction for now, found target_level {}",
                target_level
            );
        }
    }

    fn find_compaction_candidates_l0(lsm_struct: &LsmTreeDesc) -> Option<Vec<SegID>> {
        let fanout = COMPACTION_FANOUT[0];
        //assume segs in lsm_struct are sorted
        let segs = lsm_struct.get_level_desc(0).get_segs();
        let rand_rng = &mut rand::thread_rng();
        let max_id = SegIDUtil::get_pure_id(segs.last().unwrap().get_id());
        let target_pure_id = segs
            .iter()
            .map(|seg_desc| SegIDUtil::get_pure_id(seg_desc.get_id()))
            .filter(|seg_id| (seg_id % fanout == 0) && (seg_id + fanout - 1) <= max_id)
            .choose(rand_rng);
        target_pure_id.map(|pure_id| {
            (pure_id..(pure_id + fanout))
                .map(|pid| SegIDUtil::gen_segid(SegmentType::DataSegmentL0, 0, pid, None))
                .collect()
        })
    }

}

pub struct CompactionUtil {}

impl CompactionUtil {
    pub fn pop_next_valid_entry(
        queue: &mut BinaryHeap<Reverse<IdxEntry>>,
        last_key: &Key,
    ) -> Option<IdxEntry> {
        while !queue.is_empty() {
            let Reverse(next_entry) = queue.pop().unwrap();
            if next_entry.get_key() != last_key {
                return Some(next_entry);
            }
        }
        None
    }

    pub fn is_valid_next_entry(cur_entry: &IdxEntry, last_key: &Option<Key>) -> bool {
        last_key.is_none() || cur_entry.get_key() != last_key.as_ref().unwrap()
    }
}
