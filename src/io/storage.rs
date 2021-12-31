use std::io::Read;
use std::io::Write;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

use std::io;
use std::path::Path;
use std::path::PathBuf;

/* Data structs */

pub struct Range {
  pub offset: usize,
  pub length: usize,
}

pub enum ReadRequest<'a> {
  All {
    path: &'a Path,
  },
  Range {
    path: &'a Path,
    range: Range,
  },
}

/* Adaptor */

pub trait Adaptor {
  // read whole blob specified in path
  fn read_all(&self, path: &Path) -> io::Result<Vec<u8>>;
  // read range starting at offset for length bytes
  fn read_range(&self, path: &Path, range: &Range) -> io::Result<Vec<u8>>;
  // generic read for supported request type
  fn read(&self, request: &ReadRequest) -> io::Result<Vec<u8>>;

  // write whole byte array to blob
  fn write_all(&self, path: &Path, buf: &[u8]) -> io::Result<()>;
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
  fn read_all(&self, path: &Path) -> io::Result<Vec<u8>> {
    let mut f = OpenOptions::new()
        .read(true)
        .open(self.root_path.join(path))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
  }

  fn read_range(&self, path: &Path, range: &Range) -> io::Result<Vec<u8>> {
    let f = OpenOptions::new()
        .read(true)
        .open(self.root_path.join(path))?;
    let mut buf = vec![0u8; range.length];
    f.read_at(&mut buf, range.offset.try_into().unwrap())?;
    Ok(buf)
  }

  fn read(&self, request: &ReadRequest) -> io::Result<Vec<u8>> {
    match request {
      ReadRequest::All { path } => self.read_all(path),
      ReadRequest::Range { path, range } => self.read_range(path, range),
    }
  }

  fn write_all(&self, path: &Path, buf: &[u8]) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(self.root_path.join(path))?;
    f.write_all(buf.as_ref())
  }
}

/* TODO: Buffer Pool */

pub struct ExternalStorage {
  adaptor: Box<dyn Adaptor>,
}

impl ExternalStorage {
  pub fn new(adaptor: Box<dyn Adaptor>) -> ExternalStorage {
    ExternalStorage{ adaptor }
  }

  pub fn read_batch_sequential(&self, requests: &[ReadRequest]) -> io::Result<Vec<Vec<u8>>> {
    requests.iter().map(|request| self.adaptor.read(request)).collect()
  }
  // // TODO: how to return iterator?
  // // Map<std::slice::Iter<'_, ReadRequest<'_>>, 
  // fn read_batch_early(&self, requests: Vec<ReadRequest>) -> Box<dyn Iterator<Item=io::Result<Vec<u8>>>> {
  //   Box::new(requests.into_iter().map(|request| self.adaptor.read(&request)))
  //   // panic!("not implemented")
  // }
}

impl Adaptor for ExternalStorage {
  fn read_all(&self, path: &Path) -> io::Result<Vec<u8>> {
    self.adaptor.read_all(path)
  }

  fn read_range(&self, path: &Path, range: &Range) -> io::Result<Vec<u8>> {
    self.adaptor.read_range(path, range)
  }

  fn read(&self, request: &ReadRequest) -> io::Result<Vec<u8>> {
    self.adaptor.read(request)
  }

  fn write_all(&self, path: &Path, buf: &[u8]) -> io::Result<()> {
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

  /* generic Adaptor unit tests */

  fn write_all_zero_ok(adaptor: impl Adaptor) -> io::Result<()> {
    let test_path = Path::new("test.bin");
    let test_data = [0u8; 256];
    adaptor.write_all(test_path, &test_data)?;
    Ok(())
  }

  fn write_read_all_zero_ok(adaptor: impl Adaptor) -> io::Result<()> {
    // write some data
    let test_path = Path::new("test.bin");
    let test_data = [0u8; 256];
    adaptor.write_all(test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  fn write_read_all_random_ok(adaptor: impl Adaptor) -> io::Result<()> {
    // write some data
    let test_path = Path::new("test.bin");
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  fn write_twice_read_all_random_ok(adaptor: impl Adaptor) -> io::Result<()> {
    // write some data
    let test_path = Path::new("test.bin");
    let test_data_old = [1u8; 256];
    adaptor.write_all(test_path, &test_data_old)?;

    // write more, this should completely replace previous result
    let test_data_actual = [2u8; 128];
    adaptor.write_all(test_path, &test_data_actual)?;

    // read and check
    let test_data_reread = adaptor.read_all(test_path)?;
    assert_ne!(&test_data_old[..], &test_data_reread[..], "Old data should be removed");
    assert_eq!(
        &test_data_actual[..],
        &test_data_reread[..],
        "Reread data not matched with original one, possibly containing old data");
    Ok(())
  }

  fn write_read_range_random_ok(adaptor: impl Adaptor) -> io::Result<()> {
    // write some data
    let test_path = Path::new("test.bin");
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(test_path, &test_data)?;

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_range = adaptor.read_range(test_path, &Range{ offset, length })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, &test_data_range[..], "Reread data not matched with original one"); 
    }
    Ok(())
  }

  fn write_read_generic_random_ok(adaptor: impl Adaptor) -> io::Result<()> {
    // write some data
    let test_path = Path::new("test.bin");
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(test_path, &test_data)?;

    // read all
    let test_data_reread = adaptor.read(&ReadRequest::All { path: test_path })?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_reread = adaptor.read(&ReadRequest::Range { 
          path: test_path,
          range: Range{ offset, length },
      })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, &test_data_reread[..], "Reread data not matched with original one"); 
    }
    Ok(())
  }

  /* FileSystemAdaptor-specific tests */

  fn fsa_resources_setup() -> io::Result<FileSystemAdaptor> {
    let mut resource_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    resource_dir.push("resources/test");
    Ok(FileSystemAdaptor::new(&resource_dir))
  }
    
  // }

  #[test]
  fn fsa_write_all_zero_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    write_all_zero_ok(fsa)
  }

  #[test]
  fn fsa_write_read_all_zero_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    write_read_all_zero_ok(fsa)
  }

  #[test]
  fn fsa_write_read_all_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    write_read_all_random_ok(fsa)
  }

  #[test]
  fn fsa_write_twice_read_all_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    write_twice_read_all_random_ok(fsa)
  }

  #[test]
  fn fsa_write_read_range_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    write_read_range_random_ok(fsa)
  }

  #[test]
  fn fsa_write_read_generic_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    write_read_generic_random_ok(fsa)
  }

  #[test]
  fn fsa_read_all_ok() -> io::Result<()> {
    let fsa = fsa_resources_setup()?;
    let buf = fsa.read_all(Path::new("small.txt"))?;
    let read_string = match std::str::from_utf8(&buf) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  /* ExternalStorage tests */

  #[test]
  fn es_write_all_zero_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = ExternalStorage::new(Box::new(fsa));
    write_all_zero_ok(es)
  }

  #[test]
  fn es_write_read_all_zero_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_all_zero_ok(es)
  }

  #[test]
  fn es_write_read_all_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_all_random_ok(es)
  }

  #[test]
  fn es_write_twice_read_all_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = ExternalStorage::new(Box::new(fsa));
    write_twice_read_all_random_ok(es)
  }

  #[test]
  fn es_write_read_range_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_range_random_ok(es)
  }

  #[test]
  fn es_write_read_generic_random_ok() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = ExternalStorage::new(Box::new(fsa));
    write_read_generic_random_ok(es)
  }

  #[test]
  fn es_read_all_ok() -> io::Result<()> {
    let fsa = fsa_resources_setup()?;
    let es = ExternalStorage::new(Box::new(fsa));
    let buf = es.read_all(Path::new("small.txt"))?;
    let read_string = match std::str::from_utf8(&buf) {
      Ok(v) => v,
      Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    assert_eq!("text for testing", read_string, "Retrieved string mismatched");
    Ok(())
  }

  #[test]
  fn es_read_batch_sequential() -> io::Result<()> {
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = ExternalStorage::new(Box::new(fsa));

    // write some data
    let test_path = Path::new("test.bin");
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    es.write_all(test_path, &test_data)?;

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    let requests: Vec<ReadRequest> = (1..100).map(|_i| {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      ReadRequest::Range { 
          path: test_path,
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