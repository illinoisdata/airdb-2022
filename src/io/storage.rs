use memmap2::Mmap;
use memmap2::MmapOptions;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;

use crate::common::error::GResult;

/* Data structs */

pub struct Range {
  pub offset: usize,
  pub length: usize,
}

pub enum ReadRequest {
  All {
    path: PathBuf,
  },
  Range {
    path: PathBuf,
    range: Range,
  },
}

/* Adaptor */

pub trait Adaptor {
  // read whole blob specified in path
  fn read_all(&mut self, path: &Path) -> GResult<Vec<u8>>;
  // read range starting at offset for length bytes
  fn read_range(&mut self, path: &Path, range: &Range) -> GResult<Vec<u8>>;
  // generic read for supported request type
  fn read(&mut self, request: &ReadRequest) -> GResult<Vec<u8>> {
    match request {
      ReadRequest::All { path } => self.read_all(path),
      ReadRequest::Range { path, range } => self.read_range(path, range),
    }
  }

  // prepare to write inside this path
  fn create(&mut self, path: &Path) -> GResult<()>;
  // write whole byte array to blob
  fn write_all(&mut self, path: &Path, buf: &[u8]) -> GResult<()>;
}

pub struct FileSystemAdaptor {
  root_path: PathBuf,
}

impl FileSystemAdaptor {
  pub fn new<P: AsRef<Path>>(root_path: &P) -> FileSystemAdaptor {
    FileSystemAdaptor { root_path: root_path.as_ref().to_path_buf() }
  }
}

impl Adaptor for FileSystemAdaptor {
  fn read_all(&mut self, path: &Path) -> GResult<Vec<u8>> {
    let mut f = OpenOptions::new()
        .read(true)
        .open(self.root_path.join(path))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
  }

  fn read_range(&mut self, path: &Path, range: &Range) -> GResult<Vec<u8>> {
    let f = OpenOptions::new()
        .read(true)
        .open(self.root_path.join(path))?;
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

  fn create(&mut self, path: &Path) -> GResult<()> {
    Ok(std::fs::create_dir_all(self.root_path.join(path))?)
  }

  fn write_all(&mut self, path: &Path, buf: &[u8]) -> GResult<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(self.root_path.join(path))?;
    Ok(f.write_all(buf.as_ref())?)
  }
}

/* File system adaptor with mmap as cache/buffer pool layer */

pub struct MmapAdaptor {
  root_path: PathBuf,
  mmap_dict: HashMap<PathBuf, Mmap>,
  fs_adaptor: FileSystemAdaptor,
}

fn new_mmap(path_buf: &Path) -> GResult<Mmap> {
  log::debug!("Mmaping {:?}", path_buf);
  let file = File::open(path_buf)?;
  Ok(unsafe { MmapOptions::new().populate().map(&file)? })
}

impl MmapAdaptor {
  pub fn new<P: AsRef<Path>>(root_path: &P) -> MmapAdaptor {
    MmapAdaptor {
      root_path: root_path.as_ref().to_path_buf(),
      mmap_dict: HashMap::new(),
      fs_adaptor: FileSystemAdaptor::new(root_path),
    }
  }

  fn map<'a>(&'a mut self, path: &Path) -> GResult<&'a Mmap> {
    // this is or_insert_with_key with fallible insertion
    let path_buf = self.convert_path(path);
    Ok(match self.mmap_dict.entry(path_buf.clone()) {
      Entry::Occupied(entry) => entry.into_mut(),
      Entry::Vacant(entry) => entry.insert(new_mmap(&path_buf)?),
    })
  }

  fn try_map(&mut self, path: &Path) -> Option<&Mmap> {
    match self.map(path) {
      Ok(mmap) => Some(mmap),  // TODO: avoid copy?
      Err(e) => {
        log::warn!("MmapAdaptor failed to mmap path {:?} with {}", path, e);
        None
      }
    }
  }

  fn unmap(&mut self, path: &Path) -> GResult<()> {
    self.mmap_dict.remove(&self.convert_path(path));
    Ok(())
  }

  fn convert_path(&self, path: &Path) -> PathBuf {
    self.root_path.join(path)
  }
}

impl Adaptor for MmapAdaptor {
  fn read_all(&mut self, path: &Path) -> GResult<Vec<u8>> {
    match self.try_map(path) {
      Some(mmap) => Ok(mmap.to_vec()),  // TODO: avoid copy?
      None => self.fs_adaptor.read_all(path),
    }
  }

  fn read_range(&mut self, path: &Path, range: &Range) -> GResult<Vec<u8>> {
    match self.try_map(path) {
      Some(mmap) => Ok(mmap[range.offset..range.offset+range.length].to_vec()),  // TODO: avoid copy?
      None => self.fs_adaptor.read_range(path, range),
    }
  }

  fn create(&mut self, path: &Path) -> GResult<()> {
    self.unmap(path)?;
    self.fs_adaptor.create(path)
  }

  fn write_all(&mut self, path: &Path, buf: &[u8]) -> GResult<()> {
    self.unmap(path)?;
    self.fs_adaptor.write_all(path, buf)
  }
}

/* Common io interface */

pub struct ExternalStorage {
  adaptor: Box<dyn Adaptor>,
}

impl ExternalStorage {
  pub fn new(adaptor: Box<dyn Adaptor>) -> ExternalStorage {
    ExternalStorage{ adaptor }
  }

  pub fn read_batch_sequential(&mut self, requests: &[ReadRequest]) -> GResult<Vec<Vec<u8>>> {
    requests.iter().map(|request| self.adaptor.read(request)).collect()
  }
  // // TODO: how to return iterator?
  // // Map<std::slice::Iter<'_, ReadRequest<'_>>, 
  // fn read_batch_early(&self, requests: Vec<ReadRequest>) -> Box<dyn Iterator<Item=GResult<Vec<u8>>>> {
  //   Box::new(requests.into_iter().map(|request| self.adaptor.read(&request)))
  //   // panic!("not implemented")
  // }
}

impl Adaptor for ExternalStorage {
  fn read_all(&mut self, path: &Path) -> GResult<Vec<u8>> {
    self.adaptor.read_all(path)
  }

  fn read_range(&mut self, path: &Path, range: &Range) -> GResult<Vec<u8>> {
    self.adaptor.read_range(path, range)
  }

  fn read(&mut self, request: &ReadRequest) -> GResult<Vec<u8>> {
    self.adaptor.read(request)
  }

  fn create(&mut self, path: &Path) -> GResult<()> {
    self.adaptor.create(path)
  }

  fn write_all(&mut self, path: &Path, buf: &[u8]) -> GResult<()> {
    self.adaptor.write_all(path, buf)
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

  fn write_all_zero_ok(mut adaptor: impl Adaptor) -> GResult<()> {
    let test_path = PathBuf::from("test.bin");
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  fn write_all_inside_dir_ok(mut adaptor: impl Adaptor) -> GResult<()> {
    let test_path = PathBuf::from("test_dir/test.bin");
    let test_data = [0u8; 256];
    adaptor.create(Path::new("test_dir"))?;
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  fn write_read_all_zero_ok(mut adaptor: impl Adaptor) -> GResult<()> {
    // write some data
    let test_path = PathBuf::from("test.bin");
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  fn write_read_all_random_ok(mut adaptor: impl Adaptor) -> GResult<()> {
    // write some data
    let test_path = PathBuf::from("test.bin");
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  fn write_twice_read_all_random_ok(mut adaptor: impl Adaptor) -> GResult<()> {
    // write some data
    let test_path = PathBuf::from("test.bin");
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

  fn write_read_range_random_ok(mut adaptor: impl Adaptor) -> GResult<()> {
    // write some data
    let test_path = PathBuf::from("test.bin");
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

  fn write_read_generic_random_ok(mut adaptor: impl Adaptor) -> GResult<()> {
    // write some data
    let test_path = PathBuf::from("test.bin");
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read all
    let test_data_reread = adaptor.read(&ReadRequest::All { path: test_path.clone() })?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_reread = adaptor.read(&ReadRequest::Range { 
          path: test_path.clone(),
          range: Range{ offset, length },
      })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, &test_data_reread[..], "Reread data not matched with original one"); 
    }
    Ok(())
  }

  /* FileSystemAdaptor-specific tests */

  fn fsa_resources_setup() -> GResult<FileSystemAdaptor> {
    let mut resource_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    resource_dir.push("resources/test");
    Ok(FileSystemAdaptor::new(&resource_dir))
  }

  fn fsa_tempdir_setup() -> GResult<(TempDir, FileSystemAdaptor)> {
    let temp_dir = TempDir::new()?;
    let mfsa = FileSystemAdaptor::new(&temp_dir);
    Ok((temp_dir, mfsa))
  }

  #[test]
  fn fsa_write_all_zero_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    write_all_zero_ok(fsa)
  }

  #[test]
  fn fsa_write_all_inside_dir_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    write_all_inside_dir_ok(fsa)
  }

  #[test]
  fn fsa_write_read_all_zero_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_all_zero_ok(fsa)
  }

  #[test]
  fn fsa_write_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_all_random_ok(fsa)
  }

  #[test]
  fn fsa_write_twice_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    write_twice_read_all_random_ok(fsa)
  }

  #[test]
  fn fsa_write_read_range_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_range_random_ok(fsa)
  }

  #[test]
  fn fsa_write_read_generic_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    write_read_generic_random_ok(fsa)
  }

  #[test]
  fn fsa_read_all_ok() -> GResult<()> {
    let mut fsa = fsa_resources_setup()?;
    let buf = fsa.read_all(Path::new("small.txt"))?;
    let read_string = match std::str::from_utf8(&buf) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  /* MmapAdaptor-specific tests */

  fn mfsa_resources_setup() -> GResult<MmapAdaptor> {
    let mut resource_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    resource_dir.push("resources/test");
    Ok(MmapAdaptor::new(&resource_dir))
  }

  fn mfsa_tempdir_setup() -> GResult<(TempDir, MmapAdaptor)> {
    let temp_dir = TempDir::new()?;
    let mfsa = MmapAdaptor::new(&temp_dir);
    Ok((temp_dir, mfsa))
  }

  #[test]
  fn mfsa_write_all_zero_ok() -> GResult<()> {
    let (_temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_all_zero_ok(mfsa)
  }

  #[test]
  fn mfsa_write_all_inside_dir_ok() -> GResult<()> {
    let (_temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_all_inside_dir_ok(mfsa)
  }

  #[test]
  fn mfsa_write_read_all_zero_ok() -> GResult<()> {
    let (_temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_all_zero_ok(mfsa)
  }

  #[test]
  fn mfsa_write_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_all_random_ok(mfsa)
  }

  #[test]
  fn mfsa_write_twice_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_twice_read_all_random_ok(mfsa)
  }

  #[test]
  fn mfsa_write_read_range_random_ok() -> GResult<()> {
    let (_temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_range_random_ok(mfsa)
  }

  #[test]
  fn mfsa_write_read_generic_random_ok() -> GResult<()> {
    let (_temp_dir, mfsa) = mfsa_tempdir_setup()?;
    write_read_generic_random_ok(mfsa)
  }

  #[test]
  fn mfsa_read_all_ok() -> GResult<()> {
    let mut mfsa = mfsa_resources_setup()?;
    let buf = mfsa.read_all(Path::new("small.txt"))?;
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
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new(Box::new(fsa));
    write_all_zero_ok(es)
  }

  #[test]
  fn es_write_read_all_zero_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_all_zero_ok(es)
  }

  #[test]
  fn es_write_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_all_random_ok(es)
  }

  #[test]
  fn es_write_twice_read_all_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new(Box::new(fsa));
    write_twice_read_all_random_ok(es)
  }

  #[test]
  fn es_write_read_range_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_range_random_ok(es)
  }

  #[test]
  fn es_write_read_generic_random_ok() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_generic_random_ok(es)
  }

  #[test]
  fn es_read_all_ok() -> GResult<()> {
    let fsa = fsa_resources_setup()?;
    let mut es = ExternalStorage::new(Box::new(fsa));
    let buf = es.read_all(Path::new("small.txt"))?;
    let read_string = match std::str::from_utf8(&buf) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  #[test]
  fn es_read_batch_sequential() -> GResult<()> {
    let (_temp_dir, fsa) = fsa_tempdir_setup()?;
    let mut es = ExternalStorage::new(Box::new(fsa));

    // write some data
    let test_path = PathBuf::from("test.bin");
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    es.write_all(&test_path, &test_data)?;

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    let requests: Vec<ReadRequest> = (1..100).map(|_i| {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      ReadRequest::Range { 
          path: test_path.clone(),
          range: Range{ offset, length },
      }
    }).collect();
    let responses = es.read_batch_sequential(&requests)?;

    // check correctness
    for (request, response) in izip!(&requests, &responses) {
      match request {
        ReadRequest::Range { path: _, range } => {
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
    //       path: test_path,
    //       range: Range{ offset, length },
    //   })?;
    //   let test_data_expected = &test_data[offset..offset+length];
    //   assert_eq!(test_data_expected, &test_data_reread[..], "Reread data not matched with original one"); 
    // }

    // es.read_batch_sequential(vec![Rea])

}