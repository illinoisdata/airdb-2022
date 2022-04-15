use url::Url;

use crate::{
    common::error::GResult,
    storage::{data_entry::AppendRes, segment::{SegSize, SegmentProps}},
};
use std::collections::HashMap;

use super::file_utils::Range;

pub enum StorageType {
    LocalFakeStore,
    RemoteFakeStore,
    AzureStore,
}

pub trait StorageConnector {
    // open connection
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()>;

    // close connection and clear states
    fn close(&mut self) -> GResult<()>;

    // read whole segment specified in path
    fn read_all(&self, path: &Url) -> GResult<Vec<u8>>;

    // read range starting at offset for length bytes
    // TODO: support the case when range.length = 0, it means reading from an offset to the end of the segment
    fn read_range(&self, path: &Url, range: &Range) -> GResult<Vec<u8>>;

    // get the current length of the target segment
    fn get_size(&self, path: &Url) -> GResult<u64>;

    // get common properties of the target segment
    fn get_props(&self, path: &Url) -> GResult<SegmentProps>;

    // seal the target segment(change its access permission as read-only)
    fn seal(&self, path: &Url) -> GResult<()>;

    // create empty segment at a target path
    fn create(&self, path: &Url) -> GResult<()>;

    // append the byte array to the end of a target segment
    fn append(&self, path: &Url, buf: &[u8]) -> AppendRes<SegSize>;

    // write whole byte array to a segment
    fn write_all(&self, path: &Url, buf: &[u8]) -> GResult<()>;

    // remove the whole segment
    fn remove(&self, path: &Url) -> GResult<()>;
}
