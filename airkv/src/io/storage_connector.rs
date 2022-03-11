use url::Url;

use crate::common::error::GResult;
use std::collections::HashMap;

use super::file_utils::Range;

pub trait StorageConnector {
    // open connection
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()>;

    // close connection and clear states
    fn close(&mut self) -> GResult<()>;

    // read whole segment specified in path
    fn read_all(&mut self, path: &Url) -> GResult<Vec<u8>>;

    // read range starting at offset for length bytes
    fn read_range(&mut self, path: &Url, range: &Range) -> GResult<Vec<u8>>;

    // get the current length of the target segment
    fn get_size(&mut self, path: &Url) -> GResult<u64>;

    // create empty segment at a target path
    fn create(&mut self, path: &Url) -> GResult<()>;

    // append the byte array to the end of a target segment
    fn append(&mut self, path: &Url, buf: &[u8]) -> GResult<()>;

    // write whole byte array to a segment
    fn write_all(&mut self, path: &Url, buf: &[u8]) -> GResult<()>;

    // remove the whole segment
    fn remove(&mut self, path: &Url) -> GResult<()>;
}
