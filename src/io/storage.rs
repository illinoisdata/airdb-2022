use azure_core::prelude::Range as AzureRange;
use azure_storage::core::prelude::StorageAccountClient;
use azure_storage_blobs::prelude::AsBlobClient;
use azure_storage_blobs::prelude::AsContainerClient;
use azure_storage_blobs::prelude::BlobClient;
use bytes::Bytes;
use itertools::Itertools;
use memmap2::Mmap;
use memmap2::MmapOptions;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;
use url::Url;

use crate::common::error::ConflictingStorageScheme;
use crate::common::error::GenericError;
use crate::common::error::GResult;
use crate::common::error::InvalidAzureStorageUrl;
use crate::common::error::MissingAzureAuthetication;
use crate::common::error::UnavailableStorageScheme;
use crate::common::error::UrlParseFilePathError;

/* Data structs */

pub struct Range {
  pub offset: usize,
  pub length: usize,
}

pub enum ReadRequest {
  All {
    url: Url,
  },
  Range {
    url: Url,
    range: Range,
  },
}

/* Adaptor */

pub trait Adaptor: std::fmt::Debug {
  // read whole blob specified in path
  fn read_all(&mut self, url: &Url) -> GResult<Vec<u8>>;
  // read range starting at offset for length bytes
  fn read_range(&mut self, url: &Url, range: &Range) -> GResult<Vec<u8>>;
  // generic read for supported request type
  fn read(&mut self, request: &ReadRequest) -> GResult<Vec<u8>> {
    match request {
      ReadRequest::All { url } => self.read_all(url),
      ReadRequest::Range { url, range } => self.read_range(url, range),
    }
  }

  // create empty file at url
  fn create(&mut self, url: &Url) -> GResult<()>;
  // write whole byte array to blob
  fn write_all(&mut self, url: &Url, buf: &[u8]) -> GResult<()>;
  // write whole byte array to blob
  fn remove(&mut self, url: &Url) -> GResult<()>;
}

#[derive(Debug)]
pub struct FileSystemAdaptor;

impl Default for FileSystemAdaptor {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemAdaptor {
  pub fn new() -> FileSystemAdaptor {
    FileSystemAdaptor
  }

  fn read_range_from_file(f: File, range: &Range) -> GResult<Vec<u8>> {
    let mut buf = vec![0u8; range.length];

    // File::read_at might return fewer bytes than requested (e.g. 2GB at a time)
    // To read whole range, we request until the buffer is filled
    let mut buf_offset = 0;
    while buf_offset < range.length {
      let read_bytes = f.read_at(&mut buf[buf_offset..], (buf_offset + range.offset).try_into().unwrap())?; 
      buf_offset += read_bytes;
    }
    Ok(buf)
  }

  fn create_directory(&self, path: &Path) -> GResult<()> {
    Ok(std::fs::create_dir_all(path)?)
  }
}

impl Adaptor for FileSystemAdaptor {
  fn read_all(&mut self, url: &Url) -> GResult<Vec<u8>> {
    assert_eq!(url.scheme(), "file");
    let f = OpenOptions::new()
        .read(true)
        .open(url.path())?;
    let file_length = f.metadata()?.len();
    FileSystemAdaptor::read_range_from_file(f, &Range { offset: 0, length: file_length as usize })
  }

  fn read_range(&mut self, url: &Url, range: &Range) -> GResult<Vec<u8>> {
    assert_eq!(url.scheme(), "file");
    let f = OpenOptions::new()
        .read(true)
        .open(url.path())?;
    FileSystemAdaptor::read_range_from_file(f, range)
  }

  fn create(&mut self, url: &Url) -> GResult<()> {
    assert_eq!(url.scheme(), "file");
    std::fs::File::create(url.path())?;
    Ok(())
  }

  fn write_all(&mut self, url: &Url, buf: &[u8]) -> GResult<()> {
    assert_eq!(url.scheme(), "file");
    let url_path = url.path();
    self.create_directory(PathBuf::from(url_path).parent().unwrap())?;
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(url_path)?;
    Ok(f.write_all(buf.as_ref())?)
  }

  fn remove(&mut self, url: &Url) -> GResult<()> {
    assert_eq!(url.scheme(), "file");
    std::fs::remove_file(Path::new(url.path()))?;
    Ok(())
  }
}

// pub fn url_from_file_path(path: &Path) -> GResult<Url> {
//    url_from_file_str(path.to_str().expect("Unable to stringify path"))
// }

// pub fn url_from_file_str(path: &str) -> GResult<Url> {
//    Url::from_file_path(path).map_err(|_| Box::new(UrlParseFilePathError) as GenericError)
// }

pub fn url_from_dir_path(path: &Path) -> GResult<Url> {
   url_from_dir_str(path.to_str().expect("Unable to stringify path"))
}

pub fn url_from_dir_str(path: &str) -> GResult<Url> {
   Url::from_directory_path(path).map_err(|_| Box::new(UrlParseFilePathError) as GenericError)
}

/* File system adaptor with mmap as cache/buffer pool layer */

#[derive(Debug)]
pub struct MmapAdaptor {
  mmap_dict: HashMap<Url, Mmap>,
  fs_adaptor: FileSystemAdaptor,
}

fn new_mmap(url: &Url) -> GResult<Mmap> {
  assert_eq!(url.scheme(), "file");
  let file = File::open(url.path())?;
  let mmap = unsafe {
    MmapOptions::new()
      // .populate()
      .map(&file)?
  };
  log::debug!("Mmaped {:?}", url);
  Ok(mmap)
}

impl Default for MmapAdaptor {
    fn default() -> Self {
        Self::new()
    }
}

impl MmapAdaptor {
  pub fn new() -> MmapAdaptor {
    MmapAdaptor {
      mmap_dict: HashMap::new(),
      fs_adaptor: FileSystemAdaptor::new(),
    }
  }

  fn map<'a>(&'a mut self, url: &Url) -> GResult<&'a Mmap> {
    // this is or_insert_with_key with fallible insertion
    Ok(match self.mmap_dict.entry(url.clone()) {
      Entry::Occupied(entry) => entry.into_mut(),
      Entry::Vacant(entry) => entry.insert(new_mmap(url)?),
    })
  }

  fn try_map(&mut self, url: &Url) -> Option<&Mmap> {
    match self.map(url) {
      Ok(mmap) => Some(mmap),  // TODO: avoid copy?
      Err(e) => {
        log::warn!("MmapAdaptor failed to mmap {:?} with {}", url, e);
        None
      }
    }
  }

  fn unmap(&mut self, url: &Url) -> GResult<()> {
    self.mmap_dict.remove(url);
    Ok(())
  }
}

impl Adaptor for MmapAdaptor {
  fn read_all(&mut self, url: &Url) -> GResult<Vec<u8>> {
    match self.try_map(url) {
      Some(mmap) => Ok(mmap.to_vec()),  // TODO: avoid copy?
      None => self.fs_adaptor.read_all(url),
    }
  }

  fn read_range(&mut self, url: &Url, range: &Range) -> GResult<Vec<u8>> {
    match self.try_map(url) {
      Some(mmap) => Ok(mmap[range.offset..range.offset+range.length].to_vec()),  // TODO: avoid copy?
      None => self.fs_adaptor.read_range(url, range),
    }
  }

  fn create(&mut self, url: &Url) -> GResult<()> {
    self.unmap(url)?;
    self.fs_adaptor.create(url)
  }

  fn write_all(&mut self, url: &Url, buf: &[u8]) -> GResult<()> {
    self.unmap(url)?;
    self.fs_adaptor.write_all(url, buf)
  }

  fn remove(&mut self, url: &Url) -> GResult<()> {
    self.unmap(url)?;
    self.fs_adaptor.remove(url)
  }
}


/* Azure storage adaptor (per storage account/key) */

// https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
#[derive(Deserialize, Debug)]
pub enum AzureBlobType {  // control only at blob creation time
  BlockBlob,  // fast read/write large block(s) of data
  AppendBlob,  // fast append
  PageBlob,  // fast random read/write, basis of azure virtual disk
}

#[derive(Debug)]
pub struct AzureStorageAdaptor {
  storage_client: Arc<StorageAccountClient>,
  blob_type: AzureBlobType,

  rt: Runtime,  // TODO: move out? static/global variable?
}

impl AzureStorageAdaptor {
  pub fn new_block() -> GResult<AzureStorageAdaptor> {
    AzureStorageAdaptor::new(AzureBlobType::BlockBlob)
  }

  pub fn new_append() -> GResult<AzureStorageAdaptor> {
    AzureStorageAdaptor::new(AzureBlobType::AppendBlob)
  }

  pub fn new_page() -> GResult<AzureStorageAdaptor> {
    AzureStorageAdaptor::new(AzureBlobType::PageBlob)
  }

  fn new(blob_type: AzureBlobType) -> GResult<AzureStorageAdaptor> {
    // TODO: static storage account client?
    let account = std::env::var("AZURE_STORAGE_ACCOUNT")
      .map_err(|_| MissingAzureAuthetication::boxed("Set env variable AZURE_STORAGE_ACCOUNT"))?;
    let key = std::env::var("AZURE_STORAGE_KEY")
      .map_err(|_| MissingAzureAuthetication::boxed("Set env variable AZURE_STORAGE_KEY first!"))?;
    let http_client = azure_core::new_http_client();
    let storage_client = StorageAccountClient::new_access_key(http_client, &account, &key);
    Ok(AzureStorageAdaptor {
      storage_client,
      blob_type,
      rt: Runtime::new().expect("Failed to initialize tokio runtim"),
    })
  }

  fn parse_url(&self, url: &Url) -> GResult<(String, String)> {  // container name, blob path
    let mut path_segments = url.path_segments().ok_or_else(|| InvalidAzureStorageUrl::new("Failed to segment url"))?;
    let container = path_segments.next().ok_or_else(|| InvalidAzureStorageUrl::new("Require container name"))?;
    let blob_path = Itertools::intersperse(path_segments, "/").collect();
    Ok((container.to_string(), blob_path))
  }

  fn blob_client(&self, url: &Url) -> GResult<Arc<BlobClient>> {
    let (container_name, blob_name) = self.parse_url(url)?;
    Ok(self.storage_client.as_container_client(container_name).as_blob_client(&blob_name))
  }

  async fn read_all_async(&self, url: &Url) -> GResult<Vec<u8>> {
    let blob_response = self.blob_client(url)?
      .get()
      .execute()
      .await?;
    Ok(blob_response.data.to_vec())
  }

  async fn read_range_async(&self, url: &Url, range: &Range) -> GResult<Vec<u8>> {
    let blob_response = self.blob_client(url)?
      .get()
      .range(AzureRange::new(range.offset.try_into().unwrap(), (range.offset + range.length).try_into().unwrap()))
      .execute()
      .await?;
    Ok(blob_response.data.to_vec())
  }

  async fn write_all_async(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    let blob_client = self.blob_client(url)?;
    match &self.blob_type {
      AzureBlobType::BlockBlob => {
        // TODO: avoid copy?
        let response = blob_client.put_block_blob(Bytes::copy_from_slice(buf)).execute().await?;
        log::debug!("{:?}", response);
        Ok(())
      }
      AzureBlobType::AppendBlob => {
        let response = blob_client.put_append_blob().execute().await?;
        log::debug!("{:?}", response);
        todo!()  // TODO: best way to write to append blob?
      }
      AzureBlobType::PageBlob => {
        let response = blob_client.put_page_blob(buf.len().try_into().unwrap()).execute().await?;
        log::debug!("{:?}", response);
        todo!()  // TODO: write in 512-byte pages
      }
    }
  }

  async fn remove_async(&self, url: &Url) -> GResult<()> {
    self.blob_client(url)?
      .delete()
      .execute()
      .await?;
    Ok(())
  }
}

impl Adaptor for AzureStorageAdaptor {
  fn read_all(&mut self, url: &Url) -> GResult<Vec<u8>> {
    self.rt.block_on(self.read_all_async(url))
  }

  fn read_range(&mut self, url: &Url, range: &Range) -> GResult<Vec<u8>> {
    self.rt.block_on(self.read_range_async(url, range))
  }

  fn create(&mut self, _url: &Url) -> GResult<()> {
    Ok(())  // do nothing, azure blob creates hierarchy on blob creation
  }

  fn write_all(&mut self, url: &Url, buf: &[u8]) -> GResult<()> {
    self.rt.block_on(self.write_all_async(url, buf))
  }

  fn remove(&mut self, url: &Url) -> GResult<()> {
    self.rt.block_on(self.remove_async(url))
  }
}

/* Common io interface */

#[derive(Debug)]
pub struct ExternalStorage {
  adaptors: HashMap<String, Box<dyn Adaptor>>,
  schemes: Vec<String>,  // HACK: for error reporting
}

impl Default for ExternalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalStorage {
  pub fn new() -> ExternalStorage {
    ExternalStorage{
      adaptors: HashMap::new(),
      schemes: Vec::new()
    }
  }

  pub fn with(mut self, scheme: String, adaptor: Box<dyn Adaptor>) -> GResult<Self> {
    self.register(scheme, adaptor)?;
    Ok(self)
  }

  pub fn register(&mut self, scheme: String, adaptor: Box<dyn Adaptor>) -> GResult<()> {
    if self.adaptors.contains_key(&scheme) {
      // existing scheme
      return Err(ConflictingStorageScheme::boxed(&scheme));
    }

    // new scheme
    self.adaptors.insert(scheme.clone(), adaptor);
    self.schemes.push(scheme);
    Ok(())
  }

  pub fn read_batch_sequential(&mut self, requests: &[ReadRequest]) -> GResult<Vec<Vec<u8>>> {
    requests.iter().map(|request| self.read(request)).collect()
  }
  // // TODO: how to return iterator?
  // // Map<std::slice::Iter<'_, ReadRequest<'_>>, 
  // fn read_batch_early(&self, requests: Vec<ReadRequest>) -> Box<dyn Iterator<Item=GResult<Vec<u8>>>> {
  //   Box::new(requests.into_iter().map(|request| self.adaptor.read(&request)))
  //   // panic!("not implemented")
  // }

  fn select_adaptor<'a>(&'a mut self, url: &Url) -> GResult<&'a mut Box<dyn Adaptor>> {
    let scheme = url.scheme().to_string();
    match self.adaptors.entry(scheme) {
      Entry::Occupied(entry) => Ok(entry.into_mut()),
      Entry::Vacant(entry) => Err(Box::new(UnavailableStorageScheme::new(entry.into_key(), self.schemes.clone()))),
    }
  }
}

impl Adaptor for ExternalStorage {
  fn read_all(&mut self, url: &Url) -> GResult<Vec<u8>> {
    self.select_adaptor(url)?.read_all(url)
  }

  fn read_range(&mut self, url: &Url, range: &Range) -> GResult<Vec<u8>> {
    self.select_adaptor(url)?.read_range(url, range)
  }

  fn create(&mut self, url: &Url) -> GResult<()> {
    self.select_adaptor(url)?.create(url)
  }

  fn write_all(&mut self, url: &Url, buf: &[u8]) -> GResult<()> {
    self.select_adaptor(url)?.write_all(url, buf)
  }

  fn remove(&mut self, url: &Url) -> GResult<()> {
    self.select_adaptor(url)?.remove(url)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use itertools::izip;
  use rand::Rng;
  use rand;
  use tempfile::TempDir;
  use test_log::test;

  /* generic Adaptor unit tests */

  fn write_all_zero_ok(mut adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  fn write_all_inside_dir_ok(mut adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    let test_path = base_url.join("test_dir/test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  fn write_read_all_zero_ok(mut adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  fn write_read_all_random_ok(mut adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  fn write_twice_read_all_random_ok(mut adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let test_data_old = [1u8; 256];
    adaptor.write_all(&test_path, &test_data_old)?;

    // write more, this should completely replace previous result
    let test_data_actual = [2u8; 128];
    adaptor.write_all(&test_path, &test_data_actual)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_ne!(&test_data_old[..], &test_data_reread[..], "Old data should be removed");
    assert_eq!(
        &test_data_actual[..],
        &test_data_reread[..],
        "Reread data not matched with original one, possibly containing old data");
    Ok(())
  }

  fn write_read_range_random_ok(mut adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_range = adaptor.read_range(&test_path, &Range{ offset, length })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, &test_data_range[..], "Reread data not matched with original one"); 
    }
    Ok(())
  }

  fn write_read_generic_random_ok(mut adaptor: impl Adaptor, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read all
    let test_data_reread = adaptor.read(&ReadRequest::All { url: test_path.clone() })?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_reread = adaptor.read(&ReadRequest::Range { 
          url: test_path.clone(),
          range: Range{ offset, length },
      })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, &test_data_reread[..], "Reread data not matched with original one"); 
    }
    Ok(())
  }

  /* FileSystemAdaptor-specific tests */

  fn fsa_resources_setup() -> GResult<(Url, FileSystemAdaptor)> {
    let resource_dir = url_from_dir_str(env!("CARGO_MANIFEST_DIR"))?.join("resources/test/")?;
    Ok((resource_dir, FileSystemAdaptor::new()))
  }

  fn fsa_tempdir_setup() -> GResult<(TempDir, FileSystemAdaptor)> {
    let temp_dir = TempDir::new()?;
    let mfsa = FileSystemAdaptor::new();
    Ok((temp_dir, mfsa))
  }

  #[test]
  fn fsa_write_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_all_zero_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_all_inside_dir_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_all_inside_dir_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_all_zero_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_all_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_twice_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_twice_read_all_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_range_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_range_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_write_read_generic_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_generic_random_ok(fsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn fsa_read_all_ok() -> GResult<()> {
    let (resource_dir, mut fsa) = fsa_resources_setup()?;
    let buf = fsa.read_all(&resource_dir.join("small.txt")?)?;
    let read_string = match std::str::from_utf8(&buf) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  /* MmapAdaptor-specific tests */
  fn mfsa_resources_setup() -> GResult<(Url, MmapAdaptor)> {
    let resource_dir = url_from_dir_str(env!("CARGO_MANIFEST_DIR"))?.join("resources/test/")?;
    Ok((resource_dir, MmapAdaptor::new()))
  }

  fn mfsa_tempdir_setup() -> GResult<(TempDir, MmapAdaptor)> {
    Ok((TempDir::new()?, MmapAdaptor::new()))
  }

  #[test]
  fn mfsa_write_all_zero_ok() -> GResult<()> {
    let (temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_all_zero_ok(mfsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn mfsa_write_all_inside_dir_ok() -> GResult<()> {
    let (temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_all_inside_dir_ok(mfsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn mfsa_write_read_all_zero_ok() -> GResult<()> {
    let (temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_all_zero_ok(mfsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn mfsa_write_read_all_random_ok() -> GResult<()> {
    let (temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_all_random_ok(mfsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn mfsa_write_twice_read_all_random_ok() -> GResult<()> {
    let (temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_twice_read_all_random_ok(mfsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn mfsa_write_read_range_random_ok() -> GResult<()> {
    let (temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_range_random_ok(mfsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn mfsa_write_read_generic_random_ok() -> GResult<()> {
    let (temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_generic_random_ok(mfsa, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn mfsa_read_all_ok() -> GResult<()> {
    let (resource_dir, mut mfsa) = mfsa_resources_setup()?;
    let buf = mfsa.read_all(&resource_dir.join("small.txt")?)?;
    let read_string = match std::str::from_utf8(&buf) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  /* ExternalStorage tests */

  #[test]
  fn es_write_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;
    write_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;
    write_read_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;
    write_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_twice_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;
    write_twice_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_range_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;
    write_read_range_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_generic_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;
    write_read_generic_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_read_all_ok() -> GResult<()> {
    let (resource_dir, fsa) = fsa_resources_setup()?;
    let mut es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;
    let buf = es.read_all(&resource_dir.join("small.txt")?)?;
    let read_string = match std::str::from_utf8(&buf) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  #[test]
  fn es_read_batch_sequential() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let temp_dir_url = &url_from_dir_path(temp_dir.path())?;
    let mut es = ExternalStorage::new().with("file".to_string(), Box::new(fsa))?;

    // write some data
    let test_path = temp_dir_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    es.write_all(&test_path, &test_data)?;

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    let requests: Vec<ReadRequest> = (1..100).map(|_i| {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      ReadRequest::Range { 
          url: test_path.clone(),
          range: Range{ offset, length },
      }
    }).collect();
    let responses = es.read_batch_sequential(&requests)?;

    // check correctness
    for (request, response) in izip!(&requests, &responses) {
      match request {
        ReadRequest::Range { url: _, range } => {
          let offset = range.offset;
          let length = range.length;
          let test_data_expected = &test_data[offset..offset+length];
          assert_eq!(test_data_expected, &response[..], "Reread data not matched with original one");   
        },
        _ => panic!("This test should only has range requests"),
      };
    }

    Ok(())
  }
    // for _ in 0..100 {
    //   let offset = rng.gen_range(0..test_data.len() - 1);
    //   let length = rng.gen_range(0..test_data.len() - offset);
    //   let test_data_reread = adaptor.read(&ReadRequest::Range { 
    //       url: test_path,
    //       range: Range{ offset, length },
    //   })?;
    //   let test_data_expected = &test_data[offset..offset+length];
    //   assert_eq!(test_data_expected, &test_data_reread[..], "Reread data not matched with original one"); 
    // }

    // es.read_batch_sequential(vec![Rea])

}