use std::time::Instant;

use crate::{
    common::error::GResult,
    io::{file_utils::Range, storage_connector::StorageConnector},
    lsmt::level_seg_desc::LsmTreeDesc,
    storage::{
        data_entry::EntryAccess,
        seg_util::SegIDUtil,
        segment::{BlockNum, Entry, SegLen, SEG_BLOCK_NUM_LIMIT},
        segment_manager::SegmentManager,
    },
};

type Version = u32;

#[derive(Debug)]
pub struct Snapshot {
    tail_len: SegLen,
    tail_block: BlockNum,
    tail_max_block_size: BlockNum,
    lsmt_desc: LsmTreeDesc,
}

impl Snapshot {
    pub fn new(
        tail_size_new: SegLen,
        tail_block_new: BlockNum,
        tail_max_block_size_new: BlockNum,
        lsmt_desc_new: LsmTreeDesc,
    ) -> Self {
        Self {
            tail_len: tail_size_new,
            tail_block: tail_block_new,
            tail_max_block_size: tail_max_block_size_new,
            lsmt_desc: lsmt_desc_new,
        }
    }

    pub fn get_version(&self) -> Version {
        SegIDUtil::get_pure_id(self.lsmt_desc.get_tail()) * (self.tail_max_block_size as Version)
            + (self.tail_block as Version)
    }

    pub fn get_entry(
        &self,
        conn: &dyn StorageConnector,
        seg_manager: &mut SegmentManager,
        key: &[u8],
    ) -> GResult<Option<Entry>> {
        // search in the tail segment
        let tail_search_res = if self.tail_len != 0 {
            // search in the tail segment
            // unsafe {
            // if self.tail_block < SEG_BLOCK_NUM_LIMIT {
            seg_manager
                .get_data_seg(self.lsmt_desc.get_tail())
                .search_entry_in_range(conn, key, &Range::new(0, self.tail_len))?
            // } else {
            //     seg_manager
            //         .get_data_seg(self.lsmt_desc.get_tail())
            //         .search_entry(conn, key, false)?
            // }
            // }
        } else {
            None
        };

        if tail_search_res.is_some() {
            Ok(tail_search_res)
        } else {
            // search in level0-N
            let candidates = self.lsmt_desc.get_read_sequence(key);
            let search_res = candidates
                .iter()
                .map(|seg| {
                    seg_manager
                        .get_data_seg(seg.get_id())
                        .search_entry(conn, key, false)
                        .unwrap_or_else(|_| {
                            panic!(
                                "failed to search for file {}",
                                SegIDUtil::get_readable_segid(seg.get_id())
                            )
                        })
                })
                .find(|res| res.is_some());

            match search_res {
                Some(res) => Ok(res),
                None => {
                    // println!("current tree: {}", self.lsmt_desc);
                    // println!(
                    //     "current candidate segs: {:?}",
                    //     candidates
                    //         .iter()
                    //         .map(|seg| SegIDUtil::get_readable_segid(seg.get_id()))
                    //         .collect::<Vec<String>>()
                    // );
                    Ok(None)
                }
            }
        }
    }

    fn get_entry_from_level(
        &self,
        conn: &dyn StorageConnector,
        seg_manager: &mut SegmentManager,
        level_id: u8,
        key: &[u8],
    ) -> GResult<Option<Entry>> {
        let level_desc = self.lsmt_desc.get_level_desc(level_id);
        let search_res = level_desc
            .get_segs()
            .iter()
            .map(|seg| {
                seg_manager
                    .get_data_seg(seg.get_id())
                    .search_entry(conn, key, false)
            })
            .find(|res| match res {
                Ok(entry_option) => entry_option.is_some(),
                Err(_) => true,
            });

        match search_res {
            Some(res) => res,
            None => Ok(None),
        }
    }

    pub fn get_lsmt_desc(&self) -> &LsmTreeDesc {
        &self.lsmt_desc
    }

    pub fn get_tail_len(&self) -> SegLen {
        self.tail_len
    }
}
