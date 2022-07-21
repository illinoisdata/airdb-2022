use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    rc::Rc,
};

use tokio::runtime::{Builder, Runtime};
use url::Url;

use crate::{
    common::{
        bytebuffer::ByteBuffer, dataslice::DataSlice, error::GResult,
        reverse_bytebuffer::ReversedByteBuffer, serde::Serde,
    },
    compaction::compaction_task::{CompactionUtil, TaskDesc, TaskScheduler},
    consistency::optimistic_airlock::{OptimisticAirLockID, OptimisticCommitInfo},
    db::rw_db::DBFactory,
    io::storage_connector::StorageConnector,
    lsmt::tree_delta::TreeDelta,
    storage::{
        meta::Meta,
        seg_util::SegIDUtil,
        segment::{IdxEntry, ReadEntryIterator},
        segment_manager::SegmentManager,
    },
};

use super::rw_db::{ClientID, DBProps, Key};

pub static COMPACTION_WORKER_THREADS_NUM: usize = 10;

pub trait CompactionDB {
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()>;

    fn get_task(&mut self) -> Option<TaskDesc>;

    fn execute(&mut self, task_desc: &TaskDesc) -> GResult<bool>;
}

pub struct CompactionDBImpl<T: StorageConnector> {
    store_connector: T,
    seg_manager: SegmentManager,
    client_id: ClientID,
    props: DBProps,
    runtime: Runtime,
}

impl<T: StorageConnector> CompactionDBImpl<T> {
    pub fn new_from_connector(home_dir_new: Url, connector_new: T) -> CompactionDBImpl<T> {
        // get client id
        let client_id_new = DBFactory::gen_client_id(&home_dir_new, &connector_new, false);
        println!(
            "INFO: create a new compaction client with id {}",
            client_id_new
        );
        Self {
            store_connector: connector_new,
            seg_manager: SegmentManager::new(client_id_new, home_dir_new),
            client_id: client_id_new,
            props: DBProps::default(),
            runtime: Builder::new_multi_thread()
                .worker_threads(COMPACTION_WORKER_THREADS_NUM)
                .build()
                .unwrap(),
        }
    }
}

impl<T: StorageConnector> CompactionDB for CompactionDBImpl<T> {
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()> {
        // match props.get("SEG_BLOCK_NUM_LIMIT") {
        //     Some(block_num) => self.props.set_seg_block_num_limit(block_num.parse()?),
        //     None => {}
        // }
        self.store_connector.open(props)?;
        // TODO: find a way to create the meta segment
        // for now, just assume we have already created the meta before launching the client
        // refresh meta
        self.seg_manager.refresh_meta(&self.store_connector)?;
        //TODO: finish other initial work
        Ok(())
    }

    fn get_task(&mut self) -> Option<TaskDesc> {
        //TODO: optimize: avoid copy in get_refreshed_tree_desc
        TaskScheduler::create_compact_task(
            &(self
                .seg_manager
                .get_mut_meta_seg()
                .get_refreshed_tree_desc(&self.store_connector)),
            self.client_id,
        )
    }

    fn execute(&mut self, task_desc: &TaskDesc) -> GResult<bool> {
        // get all segs that need to be compacted, the segs are sorted by time
        // the newer the seg is, the larger the index is
        println!("INFO: start to run compaction for task: {}", task_desc);
        let src_segs = task_desc.get_src_segs();
        let dest_seg = task_desc.get_dest_seg();
        let home_dir = self.seg_manager.get_home_dir();
        let mut min_key: Option<Key> = None;
        let mut max_key: Option<Key> = None;
        let merged_buffer = if task_desc.get_compact_level() == 0 {
            // TODO: use multiple threads to sort each seg and remove duplicates in each seg in level 0
            let mut sorted_queues: Vec<BinaryHeap<Reverse<IdxEntry>>> = src_segs
                .iter()
                .map(|seg_id| {
                    let mut min_heap = BinaryHeap::new();

                    let seg = self
                        .store_connector
                        .read_all(&SegIDUtil::get_seg_dir(*seg_id, home_dir))
                        .unwrap_or_else(|_| {
                            panic!(
                                "failed to read seg {}",
                                SegIDUtil::get_seg_dir(*seg_id, home_dir)
                            )
                        });

                    let entries = ReadEntryIterator::new(ByteBuffer::wrap(seg));
                    entries.enumerate().for_each(|(idx, entry)| {
                        min_heap.push(Reverse(IdxEntry::new(u32::MAX - (idx as u32), entry)));
                    });
                    min_heap
                })
                .collect();

            // merge partial-sorted segs into a large sorted seg in level 1
            let mut merge_min_heap = BinaryHeap::new();
            let seg_number = sorted_queues.len();
            // record last key for each seg
            let mut last_keys: Vec<Key> = vec![vec![0]; seg_number];
            (0..seg_number).for_each(|time_seq| {
                let queue = sorted_queues.get_mut(time_seq).unwrap();
                let Reverse(mut first_element) = queue.pop().unwrap();
                let first_entry = first_element.get_entry();
                last_keys[time_seq] = first_entry.get_key().clone();
                first_element.update_idx((seg_number - time_seq) as u32);
                merge_min_heap.push(Reverse(first_element))
            });

            let mut merged_last_key: Option<Key> = None;
            let mut buffer = ByteBuffer::new();
            while !merge_min_heap.is_empty() {
                let Reverse(idx_entry) = merge_min_heap.pop().unwrap();
                if CompactionUtil::is_valid_next_entry(&idx_entry, &merged_last_key) {
                    if merged_last_key.is_none() {
                        min_key = Some(idx_entry.get_key().clone());
                    }
                    merged_last_key = Some(idx_entry.get_key().clone());
                    let entry = idx_entry.get_entry();
                    entry.serialize(&mut buffer)?;
                }
                let cur_queue_id = seg_number - idx_entry.get_idx() as usize;
                let fill_in_entry = CompactionUtil::pop_next_valid_entry(
                    sorted_queues.get_mut(cur_queue_id).unwrap(),
                    &last_keys[cur_queue_id],
                );
                if let Some(mut fill_in) = fill_in_entry {
                    fill_in.update_idx(idx_entry.get_idx());
                    last_keys[cur_queue_id] = fill_in.get_key().clone();
                    merge_min_heap.push(Reverse(fill_in));
                }
            }
            max_key = merged_last_key;
            buffer
        } else {
            // merge several sorted segs into a large sorted seg
            let mut sorted_queues: Vec<ReadEntryIterator> = src_segs
                .iter()
                .map(|seg_id| {
                    let seg = self
                        .store_connector
                        .read_all(&SegIDUtil::get_seg_dir(*seg_id, home_dir))
                        .unwrap_or_else(|_| {
                            panic!(
                                "failed to read seg {}",
                                SegIDUtil::get_seg_dir(*seg_id, home_dir)
                            )
                        });
                    let data_buffer = ByteBuffer::wrap(seg);
                    ReadEntryIterator::new(data_buffer)
                })
                .collect();

            // merge several sorted segs into a large sorted seg
            let mut merge_min_heap = BinaryHeap::new();
            let seg_number = sorted_queues.len();
            // put the smallest entry from each segs into merge_min_heap
            (0..seg_number).for_each(|time_seq| {
                let queue = sorted_queues.get_mut(time_seq).unwrap();
                let first_entry = queue.next().unwrap();
                let first_element = IdxEntry::new((seg_number - time_seq) as u32, first_entry);
                merge_min_heap.push(Reverse(first_element))
            });

            let mut merged_last_key: Option<Key> = None;
            let mut buffer = ByteBuffer::new();
            while !merge_min_heap.is_empty() {
                let Reverse(idx_entry) = merge_min_heap.pop().unwrap();
                if CompactionUtil::is_valid_next_entry(&idx_entry, &merged_last_key) {
                    if merged_last_key.is_none() {
                        min_key = Some(idx_entry.get_key().clone());
                    }
                    merged_last_key = Some(idx_entry.get_key().clone());
                    let entry = idx_entry.get_entry();
                    entry.serialize(&mut buffer)?;
                }
                // fill in merge_min_heap with the next element in the queue which pop the last entry
                let cur_queue_id = seg_number - idx_entry.get_idx() as usize;
                let fill_in_entry = sorted_queues.get_mut(cur_queue_id).unwrap().next();
                if let Some(fill_in) = fill_in_entry {
                    merge_min_heap.push(Reverse(IdxEntry::new(idx_entry.get_idx(), fill_in)));
                }
            }

            max_key = merged_last_key;
            buffer
        };

        // flush to storage
        self.store_connector
            .write_all(
                &SegIDUtil::get_seg_dir(dest_seg, home_dir),
                merged_buffer.to_view(),
            )
            .unwrap_or_else(|_| {
                panic!(
                    "failed to write seg {}",
                    SegIDUtil::get_seg_dir(dest_seg, home_dir)
                )
            });

        //try to submit tree delta
        let delta: TreeDelta = TreeDelta::new_from_compation(task_desc, min_key, max_key);
        let lock_id = OptimisticAirLockID::new(
            self.client_id,
            vec![SegIDUtil::get_resid_from_segid(dest_seg)],
        );
        self.seg_manager
            .get_mut_meta_seg()
            .append_optimistic_commit_info(
                &self.store_connector,
                OptimisticCommitInfo::new(lock_id.clone(), delta),
            )?;

        //refresh meta to check submit status
        let commit_res = self
            .seg_manager
            .get_mut_meta_seg()
            .check_optimistic_commit(&self.store_connector, &lock_id);

        //TODO: whether to remove deprecated compaction segs
        if commit_res {
            println!("INFO: compaction success for task {}", task_desc)
        } else {
            println!("WARN: compaction failed for task {}", task_desc)
        }
        Ok(commit_res)
    }
}
