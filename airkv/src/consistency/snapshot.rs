use crate::{
    common::error::GResult,
    io::{file_utils::Range, storage_connector::StorageConnector},
    lsmt::level_seg_desc::LsmTreeDesc,
    storage::{
        data_entry::EntryAccess,
        segment::{Entry, SegLen},
        segment_manager::SegmentManager,
    },
};

#[derive(Debug)]
pub struct Snapshot {
    tail_len: SegLen,
    lsmt_desc: LsmTreeDesc,
}

impl Snapshot {
    pub fn new(tail_size_new: SegLen, lsmt_desc_new: LsmTreeDesc) -> Self {
        Self {
            tail_len: tail_size_new,
            lsmt_desc: lsmt_desc_new,
        }
    }

    pub fn get_entry(
        &self,
        conn: &dyn StorageConnector,
        seg_manager: &mut SegmentManager,
        key: &[u8],
    ) -> GResult<Option<Entry>> {
        let level_num = self.lsmt_desc.get_level_num();
        // search in the tail segment
        let tail_search_res = if self.tail_len != 0 {
            // search in the tail segment
            seg_manager
                .get_data_seg(self.lsmt_desc.get_tail())
                .search_entry_in_range(conn, key, &Range::new(0, self.tail_len))?
        } else {
            None
        };

        if tail_search_res.is_some() {
            Ok(tail_search_res)
        } else {
            // search in level0-N
            let mut result: Option<Entry> = None;
            let mut cur_level = 0u8;
            while result.is_none() && cur_level < level_num {
                result = self.get_entry_from_level(conn, seg_manager, cur_level, key)?;
                cur_level += 1;
            }
            Ok(result)
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
                    .search_entry(conn, key)
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
