use crate::{common::error::GResult, io::file_utils::Range};

use super::segment::Entry;

pub trait EntryAccess {
    fn read_all_entries(&mut self) -> GResult<Box<dyn Iterator<Item = Entry>>>;

    fn read_range_entries(&mut self, range: &Range) -> GResult<Box<dyn Iterator<Item = Entry>>>;

    fn write_all_entries<T>(&self, entries: T) -> GResult<()>
    where
        T: Iterator<Item = Entry>;

    fn append_entries<T>(&self, entries: T) -> GResult<()>
    where
        T: Iterator<Item = Entry>;
}
