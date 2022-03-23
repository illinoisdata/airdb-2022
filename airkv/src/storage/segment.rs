

use url::Url;

use crate::common::dataslice::DataSlice;
use crate::common::error::GResult;
use crate::common::readbuffer::ReadBuffer;
use crate::io::file_utils::Range;
use crate::io::storage_connector::StorageConnector;

pub type SegID = u32;
pub enum SegmentType {
    MetaSegment,
    TailSegment,
    DataSegmentL0,
    DataSegmentLn,
}

impl SegmentType {
    pub fn append_access_pattern(&self) -> bool {
        matches!(self, Self::MetaSegment | Self::TailSegment | Self::DataSegmentL0)
    }

    pub fn is_tail(&self) -> bool {
        matches!(self, Self::TailSegment)
    }

    pub fn is_data_seg(&self) -> bool {
        !matches!(self, Self::MetaSegment)
    }
}

pub struct SegmentInfo {
    seg_id: SegID,
    seg_path: Url,
    level: u8,
    seg_type: SegmentType,
}

impl SegmentInfo {

    pub fn new(seg_id_new: SegID, seg_path_new: Url, level_new: u8, seg_type_new: SegmentType) -> Self {
        Self{seg_id: seg_id_new, seg_path: seg_path_new, level: level_new, seg_type: seg_type_new}
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

    pub fn is_tail(&self) -> bool {
        matches!(self.seg_type, SegmentType::TailSegment)
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

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    pub fn get_value(&self) -> &[u8] {
        &self.value
    }
}



pub trait Segment {

    fn get_connector(&self) -> &dyn StorageConnector;

    fn get_seginfo(&self) -> &SegmentInfo;

    fn get_path(&self) -> &Url {
        self.get_seginfo().get_seg_path()
    }

    fn get_size(&self) -> GResult<u64> {
        self.get_connector().get_size(self.get_path())
    }

    fn read_all(&mut self) -> GResult<DataSlice>; 

    fn read_range(&mut self, range: &Range) -> GResult<DataSlice>; 

    fn write_all(&self, data: &[u8]) -> GResult<()> {
        self.get_connector().write_all(self.get_path(), data)
    }

    fn append_all(&self, data: &[u8]) -> GResult<()> {
        self.get_connector().append(self.get_path(), data)
    }

    fn create(&self) -> GResult<()> {
        //TODO(L0): support different segment types
        self.get_connector().create(self.get_path())
    }

    fn delete_file(&self) -> GResult<()> {
        self.get_connector().remove(self.get_path())
    }
}



pub struct ReadEntryIterator { 
    buffer: Box<dyn ReadBuffer>,
}

impl ReadEntryIterator {
    pub fn new(buffer_new: Box<dyn ReadBuffer>) -> Self {
        Self{buffer: buffer_new}
    }
}

impl Iterator for ReadEntryIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.has_remaining() {
            let key_len = self.buffer.read_u16();
            let key = self.buffer.read_bytes(key_len as usize);
            let value_len = self.buffer.read_u16();
            let value = self.buffer.read_bytes(value_len as usize);
            Some(Entry::new(key, value))
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
        assert!(SegmentType::TailSegment.append_access_pattern());
        assert!(SegmentType::DataSegmentL0.append_access_pattern());
        assert!(SegmentType::MetaSegment.append_access_pattern());
        assert!(!SegmentType::DataSegmentLn.append_access_pattern());

       Ok(())

    }
}
