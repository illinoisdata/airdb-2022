use std::{fmt, slice::Iter};

use crate::{
    common::error::GResult,
    io::{file_utils::Range, storage_connector::StorageConnector},
};

use super::segment::{Entry, SegSize};

pub enum AppendRes<SegSize> {
    Success(SegSize),
    BlockCountExceedFailure,
    SegmentLengthExceedFailure,
    AppendToSealedFailure,
    SegmentNotExsitFailure,
    UnknownFailure,
}

impl AppendRes<SegSize> {
    pub fn is_success(&self) -> bool {
        match self {
            AppendRes::Success(_) => true,
            _default => false,
        }
    }

    pub fn to_status_code(&self) -> u32 {
        // status:
        // 0 => success
        // 1 => BlockCountExceedFailure
        // 2 => SegmentLengthExceedFailire
        // 3 => AppendToSealedFailure
        // 4 => SegmentNotExsitFailure
        // 5 => UnknownFailure
        match self {
            AppendRes::Success(_) => 0u32,
            AppendRes::BlockCountExceedFailure => 1u32,
            AppendRes::SegmentLengthExceedFailure => 2u32,
            AppendRes::AppendToSealedFailure => 3u32,
            AppendRes::SegmentNotExsitFailure => 4u32,
            AppendRes::UnknownFailure => 5u32,
        }
    }

    pub fn append_res_from_code(code: u32, seg_size: Option<SegSize>) -> Self {
        match code {
            0u32 => AppendRes::Success(seg_size.unwrap()),
            1u32 => AppendRes::BlockCountExceedFailure,
            2u32 => AppendRes::SegmentLengthExceedFailure,
            3u32 => AppendRes::AppendToSealedFailure,
            4u32 => AppendRes::SegmentNotExsitFailure,
            5u32 => AppendRes::UnknownFailure,
            default => panic!("unknown status code {}", default),
        }
    }

    pub fn append_res_from_azure_error(error: &str) -> Self {
        match error {
            "BlobIsSealed" => AppendRes::AppendToSealedFailure,
            "BlockCountExceedsLimit" => AppendRes::BlockCountExceedFailure,
            // TODO: verify SegmentLengthExceedFailure
            "ContentLengthLargerThanTierLimit" => AppendRes::SegmentLengthExceedFailure,
            "BlobNotFound" => AppendRes::SegmentNotExsitFailure,
            _ => AppendRes::<SegSize>::UnknownFailure,
        }
    }
}

impl fmt::Display for AppendRes<SegSize> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppendRes::Success(x) => write!(f, "success with size {}", x),
            AppendRes::BlockCountExceedFailure => write!(f, "BlockCountExceedFailure"),
            AppendRes::SegmentLengthExceedFailure => write!(f, "SegmentLengthExceedFailire"),
            AppendRes::AppendToSealedFailure => write!(f, "AppendToSealedFailure"),
            AppendRes::SegmentNotExsitFailure => write!(f, "SegmentNotExsitFailure"),
            AppendRes::UnknownFailure => write!(f, "UnknownFailure"),
        }
    }
}

pub trait EntryAccess {
    fn read_all_entries(
        &mut self,
        conn: &dyn StorageConnector,
    ) -> GResult<Box<dyn Iterator<Item = Entry>>>;

    fn read_range_entries(
        &mut self,
        conn: &dyn StorageConnector,
        range: &Range,
    ) -> GResult<Box<dyn Iterator<Item = Entry>>>;

    fn search_entry(&mut self, conn: &dyn StorageConnector, key: &[u8], is_seg_mutable: bool) -> GResult<Option<Entry>>;

    fn search_entry_in_range(
        &mut self,
        conn: &dyn StorageConnector,
        key: &[u8],
        range: &Range,
    ) -> GResult<Option<Entry>>;

    fn write_all_entries(&self, conn: &dyn StorageConnector, entries: Iter<Entry>) -> GResult<()>;

    fn append_entries(
        &self,
        conn: &dyn StorageConnector,
        entries: Iter<Entry>,
    ) -> AppendRes<SegSize>;
    
}
