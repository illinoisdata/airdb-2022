use super::{
    data_entry::EntryAccess,
    segment::{Entry, ReadEntryIterator, SegmentInfo},
};
use crate::{
    cache::data_cache::{CacheHitStatus, DataCache, DataRange},
    common::{
        bytebuffer::ByteBuffer, dataslice::DataSlice, error::GResult,
        read_bytebuffer::ReadByteBuffer, reverse_bytebuffer::ReversedByteBuffer,
    },
    io::{file_utils::Range, storage_connector::StorageConnector},
    storage::segment::Segment,
};

pub struct DataSegment<'a> {
    seg_info: SegmentInfo,
    storage_connector: &'a dyn StorageConnector,
    data_cache: DataCache,
}

impl<'a> DataSegment<'a> {
    pub fn new(seg_info_new: SegmentInfo, storage_connector_new: &'a dyn StorageConnector) -> Self {
        Self {
            seg_info: seg_info_new,
            storage_connector: storage_connector_new,
            data_cache: DataCache::default(),
        }
    }

    pub fn is_tail(&self) -> bool {
        self.seg_info.is_tail()
    }
}

impl<'a> Segment for DataSegment<'a> {
    fn get_seginfo(&self) -> &SegmentInfo {
        &self.seg_info
    }

    fn get_connector(&self) -> &dyn StorageConnector {
        self.storage_connector
    }

    fn read_all(&mut self) -> GResult<DataSlice> {
        // access cache first
        match self.data_cache.get_full()? {
            CacheHitStatus::Hit { data } => Ok(data),
            CacheHitStatus::HitPartial { miss_range } | CacheHitStatus::Miss { miss_range } => {
                // cache miss => update cache and read cache again
                let data = self
                    .get_connector()
                    .read_range(self.get_path(), &Range::transfer_from(&miss_range))?;
                self.data_cache
                    .update(true, &mut DataRange::new(miss_range, data))?;
                // read cache again, expect a cache hit
                match self.data_cache.get_full()? {
                    CacheHitStatus::Hit { data } => Ok(data),
                    _ => panic!("unexpected cache status"),
                }
            }
        }
    }

    fn read_range(&mut self, range: &Range) -> GResult<DataSlice> {
        let cache_range = &range.transfer_to();
        match self.data_cache.get(cache_range)? {
            CacheHitStatus::Hit { data } => Ok(data),
            CacheHitStatus::HitPartial { miss_range } | CacheHitStatus::Miss { miss_range } => {
                // cache miss => update cache and read cache again
                let seg_range = &Range::transfer_from(&miss_range);
                let data = self
                    .get_connector()
                    .read_range(self.get_path(), seg_range)?;
                self.data_cache.update(
                    seg_range.reach_seg_end(),
                    &mut DataRange::new(miss_range, data),
                )?;
                // read cache again, expect a cache hit
                match self.data_cache.get(cache_range)? {
                    CacheHitStatus::Hit { data } => Ok(data),
                    _ => panic!("unexpected cache status"),
                }
            }
        }
    }
}

impl EntryAccess for DataSegment<'_> {
    /// append entries to the end of the segment
    /// only the tail segment can call this function
    fn append_entries<T>(&self, entries: T) -> GResult<()>
    where
        T: Iterator<Item = Entry>,
    {
        let mut buffer = ByteBuffer::new();
        for entry in entries {
            //in order to support backward read for tail/L0 segment
            // write in this order: value -> value length -> key -> key length
            let value = entry.get_value();
            let key = entry.get_key();
            buffer.write_bytes(value);
            buffer.write_u16(value.len() as u16);
            buffer.write_bytes(key);
            buffer.write_u16(key.len() as u16);
        }
        self.append_all(buffer.to_view())
    }

    // write entries from the beginning of the segment
    // only L1-LN segment can call this function
    fn write_all_entries<T>(&self, entries: T) -> GResult<()>
    where
        T: Iterator<Item = Entry>,
    {
        let mut buffer = ByteBuffer::new();
        for entry in entries {
            let value = entry.get_value();
            let key = entry.get_key();
            buffer.write_u16(key.len() as u16);
            buffer.write_bytes(key);
            buffer.write_u16(value.len() as u16);
            buffer.write_bytes(value);
        }
        self.write_all(buffer.to_view())
    }

    fn read_all_entries(&mut self) -> GResult<Box<dyn Iterator<Item = Entry>>> {
        let data = self.read_all()?;
        if self.seg_info.append_seg() {
            let data_buffer = ReversedByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        } else {
            let data_buffer = ReadByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        }
    }

    fn read_range_entries(&mut self, range: &Range) -> GResult<Box<dyn Iterator<Item = Entry>>> {
        let data = self.read_range(range)?;
        if self.seg_info.append_seg() {
            let data_buffer = ReversedByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        } else {
            let data_buffer = ReadByteBuffer::wrap(data);
            Ok(Box::new(ReadEntryIterator::new(Box::new(data_buffer))))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use crate::{
        common::error::GResult,
        io::{
            fake_store_service_conn::FakeStoreServiceConnector, file_utils::UrlUtil,
            storage_connector::StorageConnector,
        },
        storage::{
            data_entry::EntryAccess,
            segment::{Entry, SegmentInfo, SegmentType},
        },
    };

    use super::DataSegment;

    #[test]
    fn data_segment_tail_test() -> GResult<()> {
        //test tail Segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;

        let fake_seg_id = 0u32;
        let temp_dir = TempDir::new()?;
        let fake_path = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        let mut seg = DataSegment::new(
            SegmentInfo::new(fake_seg_id, fake_path, 0, SegmentType::TailSegment),
            &first_conn,
        );

        //generate and append 100 random key-values (in batches of ten)

        let mut data_entries: Vec<Entry> = Vec::new();
        fn gen_random_bytes() -> Vec<u8> {
            (0..1024).map(|_| rand::random::<u8>()).collect()
        }

        (0..100).for_each(|_i| {
            data_entries.push(Entry::new(gen_random_bytes(), gen_random_bytes()));
        });

        (0..10).for_each(|i| {
            seg.append_entries(data_entries[i * 10..((i + 1) * 10)].to_vec().into_iter())
                .unwrap_or_else(|_| panic!("append failed for the {} round", i));
        });

        //test read from storage (check the order is reversed)
        let mut entries_read = seg.read_all_entries()?.peekable();
        // ensure entries_read is not empty
        assert!(entries_read.peek().is_some());
        data_entries
            .iter()
            .rev()
            .zip(entries_read)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test read from cache
        let entries_read_cache = seg.read_all_entries()?;
        data_entries
            .iter()
            .rev()
            .zip(entries_read_cache)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test repeatable cache read
        let entries_read_cache_repeat = seg.read_all_entries()?;
        data_entries
            .iter()
            .rev()
            .zip(entries_read_cache_repeat)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        Ok(())
    }

    #[test]
    fn data_segment_ln_test() -> GResult<()> {
        //test tail Segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;

        let fake_seg_id = 0u32;
        let temp_dir = TempDir::new()?;
        let fake_path = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        let mut seg = DataSegment::new(
            SegmentInfo::new(fake_seg_id, fake_path, 1, SegmentType::DataSegmentLn),
            &first_conn,
        );

        //generate and append 100 random key-values (in batches of ten)

        let mut data_entries: Vec<Entry> = Vec::new();
        fn gen_random_bytes() -> Vec<u8> {
            (0..1024).map(|_| rand::random::<u8>()).collect()
        }

        (0..100).for_each(|_i| {
            data_entries.push(Entry::new(gen_random_bytes(), gen_random_bytes()));
        });

        //write all
        seg.write_all_entries(data_entries.clone().into_iter())?;

        //test read from storage
        let mut entries_read = seg.read_all_entries()?.peekable();
        // ensure entries_read is not empty
        assert!(entries_read.peek().is_some());
        data_entries
            .iter()
            .zip(entries_read)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test read from cache
        let entries_read_cache = seg.read_all_entries()?;
        data_entries
            .iter()
            .zip(entries_read_cache)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        // test repeatable cache read
        let entries_read_cache_repeat = seg.read_all_entries()?;
        data_entries
            .iter()
            .zip(entries_read_cache_repeat)
            .for_each(|(origin, read)| assert_eq!(*origin, read));

        Ok(())
    }
}
