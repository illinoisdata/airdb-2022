use lru::LruCache;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use url::Url;

use crate::common::SharedBytes;
use crate::common::SharedByteView;
use crate::common::error::ConflictingStorageScheme;
use crate::common::error::GResult;
use crate::common::error::UnavailableStorageScheme;
use crate::io::storage::Adaptor;
use crate::io::storage::Range;


/* Common io interface */

#[derive(Eq, PartialEq, Hash)]
struct CacheKey {
  url: Url,
  page_idx: usize
}

impl std::fmt::Debug for CacheKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("CacheKey")
      .field("url", &self.url.to_string())
      .field("page_idx", &self.page_idx)
      .finish()
  }
}

pub struct ExternalStorage {
  adaptors: HashMap<String, Rc<Box<dyn Adaptor>>>,
  schemes: Vec<String>,  // HACK: for error reporting
  page_cache: RefCell<LruCache<CacheKey, SharedBytes>>,
  page_size: usize,
}

impl std::fmt::Debug for ExternalStorage {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ExternalStorage")
      .field("adaptors", &self.adaptors)
      .field("schemes", &self.schemes)
      .field("page_size", &self.page_size)
      .finish()
  }
}

impl Default for ExternalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalStorage {
  pub fn new() -> ExternalStorage {
    // ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 12 /* 4096 */)
    ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 13 /* 8192 B */)
  }

  pub fn new_with_cache(cache_size: usize, page_size: usize) -> ExternalStorage {
    ExternalStorage{
      adaptors: HashMap::new(),
      schemes: Vec::new(),
      page_cache: RefCell::new(LruCache::new(cache_size / page_size)),
      page_size,
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
    self.adaptors.insert(scheme.clone(), Rc::new(adaptor));
    self.schemes.push(scheme);
    Ok(())
  }

  fn select_adaptor(&self, url: &Url) -> GResult<Rc<Box<dyn Adaptor>>> {
    let scheme = url.scheme().to_string();
    match self.adaptors.get(&scheme) {
      Some(entry) => Ok(entry.clone()),
      None => Err(UnavailableStorageScheme::boxed(scheme, self.schemes.clone())),
    }
  }
}

impl ExternalStorage {

  pub fn warm_cache(&self, url: &Url, url_buffer: &SharedBytes) {
    self.warm_cache_at(url, url_buffer, 0);
    log::debug!("Warmed up cache for {:?}", url.to_string());
  }

  pub fn warm_cache_at(&self, url: &Url, buffer: &SharedBytes, offset: usize) {
    assert!(url.query().is_none());
    let buffer_range = Range { offset, length: buffer.len()};
    self.range_to_pages(&buffer_range)
      // .into_par_iter()
      .for_each(|page_idx| {
        let cache_key = self.cache_key(url.clone(), page_idx);
        let page_range = self.page_to_range(page_idx);
        let offset_l = page_range.offset - offset;  // underflow if offset not align
        let offset_r = std::cmp::min(buffer.len(), page_range.offset + page_range.length - offset);
        let page_bytes = buffer[offset_l .. offset_r].to_vec();
        self.page_cache.borrow_mut().put(
          cache_key,
          SharedBytes::from(page_bytes),
        );
      });
  }

  fn prepare_cache(&self, url: &Url, range: &Range) -> GResult<()> {
    if let Some(missing_range) = self.missing_cache_range(url, range) {
      let cache_bytes = self.select_adaptor(url)?.read_range(url, &missing_range)?;
      self.warm_cache_at(url, &cache_bytes, missing_range.offset);
    }
    Ok(())
  }

  fn missing_cache_range(&self, url: &Url, range: &Range) -> Option<Range> {
    let missing_pages: Vec<usize> = self.range_to_pages(range)
      .filter(|page_idx| self.miss_cache(url, *page_idx))
      .collect();
    if !missing_pages.is_empty() {
      let first_range = self.page_to_range(missing_pages[0]);
      let last_range = self.page_to_range(missing_pages[missing_pages.len() - 1]);
      let offset_l = first_range.offset;
      let offset_r = last_range.offset + last_range.length;
      return Some(Range { offset: offset_l, length: offset_r - offset_l })
    }
    None
  }

  fn miss_cache(&self, url: &Url, page_idx: usize) -> bool {
    let cache_key = self.cache_key(url.clone(), page_idx);
    self.page_cache.borrow_mut().get(&cache_key).is_none()
  }

  fn read_through_page(&self, url: &Url, page_idx: usize) -> GResult<(usize, SharedBytes)> {
    // make url with page idx
    let cache_key = self.cache_key(url.clone(), page_idx);

    // check in cache
    if let Some(cache_line) = self.page_cache.borrow_mut().get(&cache_key) {
      // cache hit
      Ok((page_idx, cache_line.clone()))
    } else {
      // cache miss even after prepare (can happen if eviction occurs in between)
      log::warn!("Cache missing after prepare {:?}", cache_key);
      let cache_bytes = self.select_adaptor(url)?
        .read_range(url, &Range { offset: page_idx * self.page_size, length: self.page_size })?;
      // self.page_cache.put(cache_key, cache_bytes.clone());  // TODO: should we insert?
      Ok((page_idx, cache_bytes))
    }
  }

  fn range_to_pages(&self, range: &Range) -> std::ops::Range<usize> {
    let last_offset = range.offset + range.length;
    range.offset / self.page_size .. last_offset / self.page_size + (last_offset % self.page_size != 0) as usize
  }

  fn page_to_range(&self, page_idx: usize) -> Range {
    Range { offset: page_idx * self.page_size, length: self.page_size }
  }

  fn cache_key(&self, url: Url, page_idx: usize) -> CacheKey {
    CacheKey { url, page_idx }
  }
}

impl ExternalStorage {
  pub fn read_all(&self, url: &Url) -> GResult<SharedBytes> {
    self.select_adaptor(url)?.read_all(url)
  }

  pub fn read_range(&self, url: &Url, range: &Range) -> GResult<SharedByteView> {
    // warm up cache
    self.prepare_cache(url, range)?;

    // collect page bytes
    let mut view = SharedByteView::default();
    for page_idx in self.range_to_pages(range) {
      let (page_idx, page_cache) = self.read_through_page(url, page_idx)?;
      let page_range = self.page_to_range(page_idx);
      let page_l = range.offset.saturating_sub(page_range.offset);
      let page_r = std::cmp::min(page_cache.len(), (range.offset + range.length).saturating_sub(page_range.offset));
      view.push(page_cache.slice(page_l, page_r - page_l))
    }
    Ok(view)
  }

  pub fn create(&self, url: &Url) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.borrow_mut().clear();
    self.select_adaptor(url)?.create(url)
  }

  pub fn write_all(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.borrow_mut().clear();
    self.select_adaptor(url)?.write_all(url, buf)
  }

  pub fn remove(&self, url: &Url) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.borrow_mut().clear();
    self.select_adaptor(url)?.remove(url)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use itertools::izip;
  use rand::Rng;

  use crate::io::storage::adaptor_test::fsa_resources_setup;
  use crate::io::storage::adaptor_test::fsa_tempdir_setup;
  use crate::io::storage::ReadRequest;
  use crate::io::storage::url_from_dir_path;

  /* generic unit tests */

  pub fn write_all_zero_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;
    Ok(())
  }

  pub fn write_read_all_zero_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let test_data = [0u8; 256];
    adaptor.write_all(&test_path, &test_data)?;

    // read and check
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");
    Ok(())
  }

  pub fn write_read_all_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
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

  pub fn write_twice_read_all_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
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

  pub fn write_read_range_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
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
      assert_eq!(test_data_expected, test_data_range.clone_all(), "Reread data not matched with original one"); 
    }
    Ok(())
  }

  pub fn write_read_generic_random_ok(adaptor: ExternalStorage, base_url: &Url) -> GResult<()> {
    // write some data
    let test_path = base_url.join("test.bin")?;
    let mut test_data = [0u8; 256];
    rand::thread_rng().fill(&mut test_data[..]);
    adaptor.write_all(&test_path, &test_data)?;

    // read all
    let test_data_reread = adaptor.read_all(&test_path)?;
    assert_eq!(&test_data[..], &test_data_reread[..], "Reread data not matched with original one");

    // test 100 random ranges
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
      let offset = rng.gen_range(0..test_data.len() - 1);
      let length = rng.gen_range(0..test_data.len() - offset);
      let test_data_reread = adaptor.read_range(&test_path, &Range{ offset, length })?;
      let test_data_expected = &test_data[offset..offset+length];
      assert_eq!(test_data_expected, test_data_reread.clone_all(), "Reread data not matched with original one"); 
    }
    Ok(())
  }


  /* ExternalStorage tests */

  #[test]
  fn es_write_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_twice_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_twice_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_range_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_range_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_generic_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    write_read_generic_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_read_all_ok() -> GResult<()> {
    let (resource_dir, fsa) = fsa_resources_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;
    let buf = es.read_all(&resource_dir.join("small.txt")?)?;
    let read_string = match std::str::from_utf8(&buf[..]) {
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
    let es = ExternalStorage::new_with_cache(65536, 100).with("file".to_string(), Box::new(fsa))?;

    // write some data
    let test_path = temp_dir_url.join("test.bin")?;
    let mut test_data = [0u8; 4096];
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
    let responses = requests.iter()
      .map(|request| match request {
        ReadRequest::Range { url, range } => es.read_range(url, range),
        _ => panic!("Unexpected read request type"),
      })
      .collect::<GResult<Vec<SharedByteView>>>()?;

    // check correctness
    for (request, response) in izip!(&requests, &responses) {
      match request {
        ReadRequest::Range { url: _, range } => {
          let offset = range.offset;
          let length = range.length;
          let test_data_expected = &test_data[offset..offset+length];
          assert_eq!(test_data_expected, response.clone_all(), "Reread data not matched with original one");   
        },
        _ => panic!("This test should only has range requests"),
      };
    }

    Ok(())
  }
}