use url::Url;

use crate::common::bytebuffer::ByteBuffer;
use crate::common::dataslice::DataSlice;
use crate::common::error::GResult;
use crate::common::readbuffer::ReadBuffer;
use crate::common::serde::Serde;
use crate::io::file_utils::Range;
use crate::io::storage_connector::StorageConnector;

use super::data_entry::AppendRes;
use super::seg_util::SegIDUtil;

// pub static SEG_BLOCK_NUM_LIMIT: u16 = 5000;
pub static mut SEG_BLOCK_NUM_LIMIT: u16 = 50000;

pub type SegLen = u64;

pub type BlockNum = u16;
pub type SegSize = BlockNum;

/**
 * 1. the first two bits denote segment type
 * 2. the third to tenth bits denote level number
 * 3. the eleventh to the 32nd bits denote the pure segment id
 * 4. the last 32 bits denote client id (only necessary for optimistic lock strategy)
 */
pub type SegID = u64;

pub enum SegmentType {
    MetaSegment = 0,
    DataSegmentL0 = 1,
    DataSegmentLn = 2,
}

impl SegmentType {
    pub fn append_access_pattern(&self) -> bool {
        matches!(self, Self::MetaSegment | Self::DataSegmentL0)
    }

    pub fn is_meta(&self) -> bool {
        matches!(self, Self::MetaSegment)
    }

    pub fn is_data_seg(&self) -> bool {
        !matches!(self, Self::MetaSegment)
    }

    pub fn try_from(v: u32) -> Self {
        match v {
            0 => SegmentType::MetaSegment,
            1 => SegmentType::DataSegmentL0,
            2 => SegmentType::DataSegmentLn,
            default => panic!("unknown value for SegmentType {}", default),
        }
    }
}

pub struct SegmentInfo {
    seg_id: SegID,
    level: u8,
    seg_path: Url,
    seg_type: SegmentType,
}

impl SegmentInfo {
    pub fn new(seg_id_new: SegID, home_dir: Url, level_new: u8, seg_type_new: SegmentType) -> Self {
        Self {
            seg_id: seg_id_new,
            level: level_new,
            seg_path: SegmentInfo::generate_dir(&home_dir, seg_id_new),
            seg_type: seg_type_new,
        }
    }

    pub fn new_from_id(seg_id_new: SegID, home_dir: Url) -> Self {
        if SegIDUtil::is_meta(seg_id_new) {
            SegmentInfo::new_meta(home_dir)
        } else {
            SegmentInfo::new(
                seg_id_new,
                home_dir,
                SegIDUtil::get_level(seg_id_new),
                SegIDUtil::get_seg_type(seg_id_new),
            )
        }
    }

    pub fn new_meta(home_dir: Url) -> Self {
        Self {
            seg_id: 0,
            seg_path: SegmentInfo::generate_dir(&home_dir, 0),
            level: 0,
            seg_type: SegmentType::MetaSegment,
        }
    }

    pub fn get_id(&self) -> SegID {
        self.seg_id
    }

    pub fn get_seg_path(&self) -> &Url {
        &self.seg_path
    }

    pub fn get_level(&self) -> u8 {
        self.level
    }

    pub fn get_seg_type(&self) -> &SegmentType {
        &self.seg_type
    }

    pub fn append_seg(&self) -> bool {
        self.seg_type.append_access_pattern()
    }

    pub fn is_data_seg(&self) -> bool {
        self.seg_type.is_data_seg()
    }

    pub fn generate_dir(home_dir: &Url, seg_id: SegID) -> Url {
        SegIDUtil::get_seg_dir(seg_id, home_dir)
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Entry {
    pub fn new(key_new: Vec<u8>, value_new: Vec<u8>) -> Self {
        //TODO: remove this check later
        assert!(key_new.len() <= u16::MAX as usize);
        assert!(value_new.len() <= u16::MAX as usize);
        Self {
            key: key_new,
            value: value_new,
        }
    }

    pub fn get_key_slice(&self) -> &[u8] {
        &self.key
    }

    pub fn get_value_slice(&self) -> &[u8] {
        &self.value
    }

    pub fn get_key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn get_value(&self) -> &Vec<u8> {
        &self.value
    }
}

impl Serde<Entry> for Entry {
    fn serialize(&self, buffer: &mut ByteBuffer) -> GResult<()> {
        buffer.write_u16(self.key.len() as u16);
        buffer.write_bytes(&self.key);
        buffer.write_u16(self.value.len() as u16);
        buffer.write_bytes(&self.value);
        Ok(())
    }

    fn deserialize(buffer: &mut ByteBuffer) -> Entry {
        let key_len = buffer.read_u16() as usize;
        let key = buffer.read_bytes(key_len);
        let value_len = buffer.read_u16() as usize;
        let value = buffer.read_bytes(value_len);
        Entry::new(key, value)
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_key().cmp(other.get_key())
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct IdxEntry {
    idx: u32,
    entry: Entry,
}

impl IdxEntry {
    pub fn new(idx_new: u32, entry_new: Entry) -> Self {
        Self {
            idx: idx_new,
            entry: entry_new,
        }
    }

    pub fn get_key(&self) -> &Vec<u8> {
        self.entry.get_key()
    }

    pub fn get_entry(&self) -> &Entry {
        &self.entry
    }

    pub fn get_idx(&self) -> u32 {
        self.idx
    }

    pub fn update_idx(&mut self, new_idx: u32) {
        self.idx = new_idx;
    }
}

impl Ord for IdxEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        //entry with smallest entry value and largest idx will rank top
        match self.get_entry().cmp(other.get_entry()) {
            std::cmp::Ordering::Equal => self.get_idx().cmp(&other.get_idx()),
            other_cmp => other_cmp,
        }
    }
}

impl PartialOrd for IdxEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct SegmentProps {
    len: SegLen,
    block_num: BlockNum,
    is_sealed: bool,
}

impl SegmentProps {
    pub fn new(len_new: SegLen, block_num_new: BlockNum, is_sealed_new: bool) -> Self {
        Self {
            len: len_new,
            block_num: block_num_new,
            is_sealed: is_sealed_new,
        }
    }

    pub fn get_seg_len(&self) -> SegLen {
        self.len
    }

    pub fn get_block_num(&self) -> BlockNum {
        self.block_num
    }

    pub fn is_sealed(&self) -> bool {
        self.is_sealed
    }

    pub fn is_active_tail(&self) -> bool {
        !self.is_sealed
    }
}

pub trait Segment {
    fn get_seginfo(&self) -> &SegmentInfo;

    fn get_segid(&self) -> SegID {
        self.get_seginfo().get_id()
    }

    fn get_path(&self) -> &Url {
        self.get_seginfo().get_seg_path()
    }

    fn get_size(&self, conn: &dyn StorageConnector) -> GResult<u64> {
        conn.get_size(self.get_path())
    }

    fn get_props(&self, conn: &dyn StorageConnector) -> GResult<SegmentProps> {
        conn.get_props(self.get_path())
    }

    fn seal(&self, conn: &dyn StorageConnector) -> GResult<()>;

    fn read_all(&mut self, conn: &dyn StorageConnector) -> GResult<DataSlice>;

    fn read_range(&mut self, conn: &dyn StorageConnector, range: &Range) -> GResult<DataSlice>;

    fn write_all(&self, conn: &dyn StorageConnector, data: &[u8]) -> GResult<()> {
        conn.write_all(self.get_path(), data)
    }

    fn append_all(&self, conn: &dyn StorageConnector, data: &[u8]) -> AppendRes<SegSize> {
        conn.append(self.get_path(), data)
    }

    fn create(&self, conn: &dyn StorageConnector) -> GResult<()> {
        //TODO(L0): support different segment types
        conn.create(self.get_path())
    }

    fn delete_file(&self, conn: &dyn StorageConnector) -> GResult<()> {
        conn.remove(self.get_path())
    }
}

pub struct ReadEntryIterator {
    // buffer: Box<dyn ReadBuffer>,
    buffer:  ByteBuffer,
}

impl ReadEntryIterator {
    // pub fn new(buffer_new: Box<dyn ReadBuffer>) -> Self {
    //     Self { buffer: buffer_new }
    // }
    pub fn new(buffer_new: ByteBuffer) -> Self {
        Self { buffer: buffer_new }
    }

    pub fn new_from_vec(vec: Vec<u8>) -> Self {
        Self { buffer: ByteBuffer::wrap(vec) }
    }
}

impl Iterator for ReadEntryIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.has_remaining() {
            // let key_len = self.buffer.read_u16();
            // let key = self.buffer.read_bytes(key_len as usize);
            // let value_len = self.buffer.read_u16();
            // let value = self.buffer.read_bytes(value_len as usize);
            // Some(Entry::new(key, value))
            Some(Entry::deserialize(&mut self.buffer))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{common::error::GResult, storage::segment::SegmentType};

    #[test]
    fn segment_type_test() -> GResult<()> {
        assert!(SegmentType::DataSegmentL0.append_access_pattern());
        assert!(SegmentType::MetaSegment.append_access_pattern());
        assert!(!SegmentType::DataSegmentLn.append_access_pattern());

        Ok(())
    }
}
