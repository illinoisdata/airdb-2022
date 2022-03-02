use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom}, path::Path,
};

use url::Url;

use crate::common::error::{GResult, UrlParseFilePathError, GenericError};

pub struct Range {
    pub offset: usize,
    pub length: usize,
}

impl Range {
    pub fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }
}

pub struct FileUtil;

impl FileUtil {
    pub fn create_file(path: &Url) -> GResult<()> {
        std::fs::File::create(path.path())?;
        Ok(())
    }

    pub fn delete_file(path: &Url) -> GResult<()> {
        std::fs::remove_file(path.path())?;
        Ok(())
    }

    pub fn exist(path: &Url) -> GResult<bool> {
        Ok(Path::new(path.path()).exists())
    }

    pub fn file_size(path: &Url) -> GResult<u64> {
        let f = OpenOptions::new().read(true).open(path.path()).unwrap();
        Ok(f.metadata()?.len())
    }

    pub fn read_range_from_file(mut f: File, range: &Range) -> GResult<Vec<u8>> {
        f.seek(SeekFrom::Start(range.offset as u64))?;
        let mut buf = vec![0u8; range.length];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }
}

pub struct UrlUtil;

impl UrlUtil {
    pub fn url_from_path(path: &Path) -> GResult<Url> {
        Url::from_file_path(path.to_str().expect("Unable to stringify path")).map_err(|_| Box::new(UrlParseFilePathError) as GenericError)
     }
}