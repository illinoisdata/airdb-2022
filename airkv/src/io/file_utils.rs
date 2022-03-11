use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use url::Url;

use crate::common::error::{GResult, GenericError, UrlParseFilePathError};

pub struct Range {
    pub offset: u64,
    pub length: u64,
}

impl Range {
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    pub fn new_usize(offset: usize, length: usize) -> Self {
        Self { offset: offset as u64, length:length as u64 }
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
        let mut buf = vec![0u8; range.length as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn read_range_from_path(path: &Url, range: &Range) -> GResult<Vec<u8>> {
        let f = OpenOptions::new().read(true).open(path.path()).unwrap();
        FileUtil::read_range_from_file(f, range)
    }

    pub fn read_all_from_path(path: &Url) -> GResult<Vec<u8>> {
        let f = OpenOptions::new().read(true).open(path.path())?;
        let file_length = f.metadata()?.len();
        FileUtil::read_range_from_file(f, &Range::new(0, file_length))
    }

    pub fn write_all_to_path(path: &Url, buf: &[u8]) -> GResult<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.path())?;
        Ok(f.write_all(buf)?)
    }
}

pub struct UrlUtil;

impl UrlUtil {
    pub fn url_from_path(path: &Path) -> GResult<Url> {
        Url::from_file_path(path.to_str().expect("Unable to stringify path"))
            .map_err(|_| Box::new(UrlParseFilePathError) as GenericError)
    }
    pub fn url_from_string(path: &str) -> GResult<Url> {
        Url::parse(path).map_err(|_| Box::new(UrlParseFilePathError) as GenericError)
    }
}
