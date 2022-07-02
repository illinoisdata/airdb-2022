use std::{collections::HashMap, slice::Iter, time::Instant};

use super::{
    data_entry::{AppendRes, EntryAccess},
    segment::{Entry, ReadEntryIterator, SegSize, SegmentInfo},
};
use crate::{
    cache::{
        data_cache::{CacheHitStatus, DataCache},
        entry_cache::EntryCache,
    },
    common::{
        bytebuffer::ByteBuffer, dataslice::DataSlice, error::GResult,
        read_bytebuffer::ReadByteBuffer, reverse_bytebuffer::ReversedByteBuffer,
    },
    db::rw_db::Key,
    io::{file_utils::Range, storage_connector::StorageConnector},
    storage::segment::Segment,
};

pub static mut APPEND_TIME: u128 = 0;
pub static mut TAIL_CREATE_TIME: u128 = 0;
pub static mut TAIL_LOCK_COMMIT_TIME: u128 = 0;
pub static mut TAIL_LOCK_CHECK_TIME: u128 = 0;

pub struct DataSegment {
    seg_info: SegmentInfo,
    data_cache: DataCache,
    entry_cache: EntryCache,
}

impl DataSegment {
    pub fn new(seg_info_new: SegmentInfo) -> Self {
        Self {
            seg_info: seg_info_new,
            data_cache: DataCache::default(),
            entry_cache: EntryCache::new(),
        }
    }
}

impl Segment for DataSegment {
    fn get_seginfo(&self) -> &SegmentInfo {
        &self.seg_info
    }

    fn read_all(&mut self, conn: &dyn StorageConnector) -> GResult<DataSlice> {
        // access cache first
        match self.data_cache.get_full()? {
            CacheHitStatus::Hit { data } => Ok(data),
            // CacheHitStatus::HitPartial { miss_range } | CacheHitStatus::Miss { miss_range } => {
            //     // cache miss => update cache and read cache again
            // let data = conn.read_range(self.get_path(), &Range::transfer_from(&miss_range))?;
            //     self.data_cache
            //         .update(true, &mut DataRange::new(miss_range, data))?;
            //     // read cache again, expect a cache hit
            //     match self.data_cache.get_full()? {
            //         CacheHitStatus::Hit { data } => Ok(data),
            //         _ => panic!("unexpected cache status"),
            //     }
            // }
            CacheHitStatus::HitPartial { miss_range } => {
                // cache miss => update cache and read cache again
                assert!(miss_range.end == 0);

                if miss_range.start == 0 {
                    // read all
                    let data = conn.read_all(self.get_path())?;
                    if data.is_empty() {
                        self.data_cache.set_full();
                    } else {
                        self.data_cache.update_from_slice(
                            true,
                            miss_range.start..(miss_range.start + data.len() as u64),
                            &data,
                        )?;
                    }
                } else {
                    // trick: read one more byte to avoid invalid range index
                    let read_range = (miss_range.start - 1)..miss_range.end;
                    let data =
                        conn.read_range(self.get_path(), &Range::transfer_from(&read_range))?;
                    if data.len() > 1 {
                        self.data_cache.update_from_slice(
                            true,
                            miss_range.start..(miss_range.start + data.len() as u64 - 1),
                            &data[1..],
                        )?;
                    } else {
                        self.data_cache.set_full();
                    }
                }

                match self.data_cache.get_full()? {
                    CacheHitStatus::Hit { data } => Ok(data),
                    _ => panic!("unexpected cache status"),
                }
            }
            CacheHitStatus::Miss { miss_range: _ } => {
                // cache miss => update cache and read cache again
                let data = conn.read_all(self.get_path())?;
                self.data_cache = DataCache::new(true, 0..data.len() as u64, data);
                // read cache again, expect a cache hit
                match self.data_cache.get_full()? {
                    CacheHitStatus::Hit { data } => Ok(data),
                    _ => panic!("unexpected cache status"),
                }
            }
        }
    }

    fn read_range(&mut self, conn: &dyn StorageConnector, range: &Range) -> GResult<DataSlice> {
        //TODO: remove this line
        assert!(!range.reach_seg_end());
        let cache_range = &range.transfer_to();
        match self.data_cache.get(cache_range)? {
            CacheHitStatus::Hit { data } => Ok(data),
            CacheHitStatus::HitPartial { miss_range } | CacheHitStatus::Miss { miss_range } => {
                // cache miss => update cache and read cache again
                let seg_range = &Range::transfer_from(&miss_range);
                let data = conn.read_range(self.get_path(), seg_range)?;
                self.data_cache
                    .update_from_slice(false, miss_range, &data)?;
                // read cache again, expect a cache hit
                match self.data_cache.get(cache_range)? {
                    CacheHitStatus::Hit { data } => Ok(data),
                    _ => panic!(
                        "unexpected cache status for range {:?}, cahced_range {:?}",
                        cache_range,
                        self.data_cache.get_range()
                    ),
                }
            }
        }
    }

    fn seal(&self, conn: &dyn StorageConnector) -> GResult<()> {
        conn.seal(self.get_path())
    }
}

impl EntryAccess for DataSegment {
    /// append entries to the end of the segment
    /// only the tail segment can call this function
    // fn append_entries<T>(&self, conn: &dyn StorageConnector, entries: T) -> AppendRes<SegSize>
    // where
    //     T: Iterator<Item = Entry>,
    fn append_entries(
        &self,
        conn: &dyn StorageConnector,
        entries: Iter<Entry>,
    ) -> AppendRes<SegSize> {
        let mut buffer = ByteBuffer::new();
        for entry in entries {
            //in order to support backward read for tail/L0 segment
            // write in this order: value -> value length -> key -> key length
            let value = entry.get_value_slice();
            let key = entry.get_key_slice();
            buffer.write_bytes(value);
            buffer.write_u16(value.len() as u16);
            buffer.write_bytes(key);
            buffer.write_u16(key.len() as u16);
        }
        // let start = Instant::now();
        let res = self.append_all(conn, buffer.to_view());
        // unsafe {
        //     APPEND_TIME += start.elapsed().as_millis();
        // }
        res
    }

    // write entries from the beginning of the segment
    // only L1-LN segment can call this function
    // fn write_all_entries<T>(&self, conn: &dyn StorageConnector, entries: T) -> GResult<()>
    // where
    //     T: Iterator<Item = Entry>,
    fn write_all_entries(&self, conn: &dyn StorageConnector, entries: Iter<Entry>) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        for entry in entries {
            let value = entry.get_value_slice();
            let key = entry.get_key_slice();
            buffer.write_u16(key.len() as u16);
            buffer.write_bytes(key);
            buffer.write_u16(value.len() as u16);
            buffer.write_bytes(value);
        }
        self.write_all(conn, buffer.to_view())
    }

    fn read_all_entries(
        &mut self,
        conn: &dyn StorageConnector,
    ) -> GResult<Box<dyn Iterator<Item = Entry>>> {
        let data = self.read_all(conn)?;
        if self.seg_info.append_seg() {
            let data_buffer = ReversedByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        } else {
            let data_buffer = ReadByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        }
    }

    fn read_range_entries(
        &mut self,
        conn: &dyn StorageConnector,
        range: &Range,
    ) -> GResult<Box<dyn Iterator<Item = Entry>>> {
        let data = self.read_range(conn, range)?;
        if self.seg_info.append_seg() {
            let data_buffer = ReversedByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        } else {
            let data_buffer = ReadByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        }
    }

    fn search_entry(
        &mut self,
        conn: &dyn StorageConnector,
        key: &[u8],
        is_seg_mutable: bool,
    ) -> GResult<Option<Entry>> {
        //use hashmap cache to search
        if self.entry_cache.is_full() {
            Ok(self.entry_cache.get(key))
        } else if self.entry_cache.is_empty() {
            let data = conn.read_all(self.get_path())?;
            let data_len = data.len();
            if self.seg_info.append_seg() {
                //TODO: remove DataSlice wrap
                let data_buffer = ReversedByteBuffer::wrap(DataSlice::wrap_vec(data));
                let iter = ReadEntryIterator::new(Box::new(data_buffer));
                iter.for_each(|entry| {
                    if !self.entry_cache.contains(entry.get_key()) {
                        self.entry_cache.insert(entry.get_key().clone(), entry);
                    }
                });
                // update is_full, range properties
                if !is_seg_mutable {
                    self.entry_cache.set_full();
                }
                //TODO: take care of the upper bound (inclusive or not)
                self.entry_cache.set_upper_bound(data_len as u64);
            } else {
                //TODO: remove DataSlice wrap
                let data_buffer = ReadByteBuffer::wrap(DataSlice::wrap_vec(data));
                let iter = ReadEntryIterator::new(Box::new(data_buffer));
                iter.for_each(|entry| {
                    self.entry_cache.insert(entry.get_key().clone(), entry);
                });
                // update is_full, range properties
                if !is_seg_mutable {
                    self.entry_cache.set_full();
                }
                //TODO: take care of the upper bound (inclusive or not)
                self.entry_cache.set_upper_bound(data_len as u64);
            }
            Ok(self.entry_cache.get(key))
        } else {
            let old_bound = self.entry_cache.get_upper_bound();
            let data = conn.read_range(self.get_path(), &Range::new(old_bound, 0))?;
            let data_len = data.len();
            assert!(self.seg_info.append_seg());
            //TODO: remove DataSlice wrap
            let data_buffer = ReversedByteBuffer::wrap(DataSlice::wrap_vec(data));
            let iter = ReadEntryIterator::new(Box::new(data_buffer));
            let mut tmp_map: HashMap<Key, Entry> = HashMap::new();
            iter.for_each(|entry| {
                if !tmp_map.contains_key(entry.get_key()) {
                    tmp_map.insert(entry.get_key().clone(), entry);
                }
            });
            self.entry_cache.extend(tmp_map);
            // update is_full, range properties
            if !is_seg_mutable {
                self.entry_cache.set_full();
            }
            //TODO: take care of the upper bound (inclusive or not)
            self.entry_cache
                .set_upper_bound(self.entry_cache.get_upper_bound() + data_len as u64);
            Ok(self.entry_cache.get(key))
        }
    }

    //call this method only when the end of the sealed seg won't be reached
    fn search_entry_in_range(
        &mut self,
        conn: &dyn StorageConnector,
        key: &[u8],
        range: &Range,
    ) -> GResult<Option<Entry>> {
        //TODO: use index to search
        assert!(range.get_offset() == 0);
        if range.get_length() + range.get_offset() <= self.entry_cache.get_upper_bound() {
            Ok(self.entry_cache.get(key))
        } else {
            let old_bound = self.entry_cache.get_upper_bound();
            let data = conn.read_range(
                self.get_path(),
                &Range::new(
                    old_bound,
                    range.get_length() + range.get_offset() - old_bound,
                ),
            )?;
            let data_len = data.len();
            assert!(self.seg_info.append_seg());
            //TODO: remove DataSlice wrap
            let data_buffer = ReversedByteBuffer::wrap(DataSlice::wrap_vec(data));
            let iter = ReadEntryIterator::new(Box::new(data_buffer));
            let mut tmp_map: HashMap<Key, Entry> = HashMap::new();
            iter.for_each(|entry| {
                if !tmp_map.contains_key(entry.get_key()) {
                    tmp_map.insert(entry.get_key().clone(), entry);
                }
            });
            self.entry_cache.extend(tmp_map);
            //TODO: take care of the upper bound (inclusive or not)
            self.entry_cache
                .set_upper_bound(self.entry_cache.get_upper_bound() + data_len as u64);
            Ok(self.entry_cache.get(key))
        }

        // let mut entries = self.read_range_entries(conn, range)?;
        // let start = Instant::now();
        // let res = entries.find(|entry| entry.get_key_slice() == key);
        // unsafe {
        //     IN_MEM_SEARCH_TIME += start.elapsed().as_millis();
        // }
        // Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::collections::HashMap;
    use url::Url;

    use tempfile::TempDir;

    use crate::{
        cache::data_cache::{CacheHitStatus, DataCache},
        common::error::GResult,
        io::{
            azure_conn::AzureConnector, fake_store_service_conn::FakeStoreServiceConnector,
            file_utils::UrlUtil, storage_connector::StorageConnector,
        },
        storage::{
            data_entry::{AppendRes, EntryAccess},
            seg_util::DATA_SEG_ID_MIN,
            segment::{Entry, SegID, Segment, SegmentInfo, SegmentType},
        },
    };

    use super::DataSegment;

    #[test]
    fn cache_range_test() -> GResult<()> {
        let mut data_cache = DataCache::default();
        assert!(!data_cache.is_full());
        assert_eq!(data_cache.get_range(), &(0..0));
        data_cache.update_from_slice(true, 0..0, &[1, 2, 4])?;
        match data_cache.get_full()? {
            CacheHitStatus::Hit { data } => assert_eq!(data.copy_range(0..3), vec![1, 2, 4]),
            _ => panic!("unexpected cache hit status"),
        }
        Ok(())
    }

    // let data = conn.read_range(self.get_path(), &Range::transfer_from(&read_range))?;

    #[test]
    fn range_all_corner_test() -> GResult<()> {
        let test_path = format!("az:///{}/{}", "airkv", "test_blob_corner");
        let url = Url::parse(&test_path)?;
        let mut conn = AzureConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        conn.open(fake_props)?;
        conn.create(&url)?;
        let data = conn.read_all(&url)?;
        assert_eq!(data.len(), 0);
        conn.remove(&url)?;
        Ok(())
    }

    #[test]
    fn data_segment_l0_test() -> GResult<()> {
        //test tail Segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;

        let fake_seg_id: SegID = DATA_SEG_ID_MIN;
        let temp_dir = TempDir::new()?;
        let fake_path = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        let mut seg = DataSegment::new(SegmentInfo::new(
            fake_seg_id,
            fake_path,
            0,
            SegmentType::DataSegmentL0,
        ));

        seg.create(&first_conn)?;

        //generate and append 100 random key-values (in batches of ten)

        let mut data_entries: Vec<Entry> = Vec::new();
        fn gen_random_bytes() -> Vec<u8> {
            (0..1024).map(|_| rand::random::<u8>()).collect()
        }

        (0..100).for_each(|_i| {
            data_entries.push(Entry::new(gen_random_bytes(), gen_random_bytes()));
        });

        (0..10).for_each(|i| {
            // seg.append_entries(data_entries[i * 10..((i + 1) * 10)].to_vec().into_iter())
            //     .unwrap_or_else(|_| panic!("append failed for the {} round", i));
            let res = seg.append_entries(&first_conn, data_entries[i * 10..((i + 1) * 10)].iter());
            match res {
                AppendRes::Success(_) => {}
                _ => panic!("append failed for the {} round", i),
            }
        });

        seg.seal(&first_conn)?;

        //test read from storage (check the order is reversed)
        let mut entries_read = seg.read_all_entries(&first_conn)?.peekable();
        // ensure entries_read is not empty
        assert!(entries_read.peek().is_some());
        data_entries
            .iter()
            .rev()
            .zip(entries_read)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test read from cache
        let entries_read_cache = seg.read_all_entries(&first_conn)?;
        data_entries
            .iter()
            .rev()
            .zip(entries_read_cache)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test repeatable cache read
        let entries_read_cache_repeat = seg.read_all_entries(&first_conn)?;
        data_entries
            .iter()
            .rev()
            .zip(entries_read_cache_repeat)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test search
        (0..30).for_each(|_| {
            // get target kv
            let target_entry = &data_entries[rand::thread_rng().gen_range(0..100)];
            // search kv
            let search_entry_res = seg.search_entry(&first_conn, target_entry.get_key_slice(), false);
            assert!(search_entry_res.is_ok());
            let search_entry_op = search_entry_res.unwrap();
            assert!(search_entry_op.is_some());
            let search_entry = search_entry_op.unwrap();
            assert_eq!(target_entry.get_key_slice(), search_entry.get_key_slice());
            assert_eq!(
                target_entry.get_value_slice(),
                search_entry.get_value_slice()
            );
        });

        Ok(())
    }

    #[test]
    fn data_segment_ln_test() -> GResult<()> {
        //test LN Segment (SegmentType::DataSegmentLn)
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;

        let fake_seg_id: SegID = DATA_SEG_ID_MIN;
        let temp_dir = TempDir::new()?;
        let fake_path = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        let mut seg = DataSegment::new(SegmentInfo::new(
            fake_seg_id,
            fake_path,
            1,
            SegmentType::DataSegmentLn,
        ));

        seg.create(&first_conn)?;

        //generate and append 100 random key-values (in batches of ten)

        let mut data_entries: Vec<Entry> = Vec::new();
        fn gen_random_bytes() -> Vec<u8> {
            (0..1024).map(|_| rand::random::<u8>()).collect()
        }

        (0..100).for_each(|_i| {
            data_entries.push(Entry::new(gen_random_bytes(), gen_random_bytes()));
        });

        //write all
        seg.write_all_entries(&first_conn, data_entries.iter())?;

        seg.seal(&first_conn)?;

        //test read from storage
        let mut entries_read = seg.read_all_entries(&first_conn)?.peekable();
        // ensure entries_read is not empty
        assert!(entries_read.peek().is_some());
        data_entries
            .iter()
            .zip(entries_read)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test read from cache
        let entries_read_cache = seg.read_all_entries(&first_conn)?;
        data_entries
            .iter()
            .zip(entries_read_cache)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test repeatable cache read
        let entries_read_cache_repeat = seg.read_all_entries(&first_conn)?;
        data_entries
            .iter()
            .zip(entries_read_cache_repeat)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test search
        (0..30).for_each(|_| {
            // get target kv
            let target_entry = &data_entries[rand::thread_rng().gen_range(0..100)];
            // search kv
            let search_entry_res = seg.search_entry(&first_conn, target_entry.get_key_slice(), false);
            assert!(search_entry_res.is_ok());
            let search_entry_op = search_entry_res.unwrap();
            assert!(search_entry_op.is_some());
            let search_entry = search_entry_op.unwrap();
            assert_eq!(target_entry.get_key_slice(), search_entry.get_key_slice());
            assert_eq!(
                target_entry.get_value_slice(),
                search_entry.get_value_slice()
            );
        });

        Ok(())
    }
}
