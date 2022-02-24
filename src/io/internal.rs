use moka::sync::Cache;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::common::ArcBytes;
use crate::common::error::ConflictingStorageScheme;
use crate::common::error::GResult;
use crate::common::error::UnavailableStorageScheme;
use crate::io::storage::Adaptor;
use crate::io::storage::Range;
use crate::io::storage::ReadRequest;


/* Common io interface */

pub struct ExternalStorage {
  adaptors: HashMap<String, Arc<Box<dyn Adaptor>>>,
  schemes: Vec<String>,  // HACK: for error reporting
  page_cache: Cache<Url, Arc<Vec<u8>>>,
  page_size: usize,
}

impl std::fmt::Debug for ExternalStorage {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ExternalStorage")
      .field("adaptors", &self.adaptors)
      .field("schemes", &self.schemes)
      .field("page_cache_capacity", &self.page_cache.max_capacity())
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
    ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 12 /* 4096 */)
  }

  pub fn new_with_cache(cache_size: u64, page_size: usize) -> ExternalStorage {
    ExternalStorage{
      adaptors: HashMap::new(),
      schemes: Vec::new(),
      page_cache: Cache::builder()
        .weigher(|_key, value: &ArcBytes| -> u32 {
          value.len().try_into().unwrap()
        })
        .max_capacity(cache_size)
        .build(),
      page_size
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
    self.adaptors.insert(scheme.clone(), Arc::new(adaptor));
    self.schemes.push(scheme);
    Ok(())
  }

  pub fn read_batch_sequential(&self, requests: &[ReadRequest]) -> GResult<Vec<ArcBytes>> {
    // TODO: async this
    requests.par_iter().map(|request| self.read(request)).collect()
  }

  fn select_adaptor(&self, url: &Url) -> GResult<Arc<Box<dyn Adaptor>>> {
    let scheme = url.scheme().to_string();
    match self.adaptors.get(&scheme) {
      Some(entry) => Ok(entry.clone()),
      None => Err(UnavailableStorageScheme::boxed(scheme, self.schemes.clone())),
    }
  }
}

// cache-related logics
impl ExternalStorage {

  pub fn warm_cache(&self, url: &Url, url_buffer: &ArcBytes) {
    assert!(url.query().is_none());
    let buffer_range = Range { offset: 0, length: url_buffer.len()};
    self.range_to_pages(&buffer_range)
      // .into_par_iter()
      .for_each(|page_idx| {
        let paged_url = self.paged_url(url.clone(), page_idx);
        let page_range = self.page_to_range(page_idx);
        let page_offset_r = std::cmp::min(buffer_range.length, page_range.offset + page_range.length);
        let page_bytes = url_buffer[page_range.offset .. page_offset_r].to_vec();
        self.page_cache.insert(paged_url, Arc::from(page_bytes))
      });
    log::info!("Warmed up cache for {:?}", url.to_string());
  }

  fn read_through_page(&self, url: &Url, page_idx: usize) -> GResult<ArcBytes> {
    // make url with page idx
    let paged_url = self.paged_url(url.clone(), page_idx);

    // check in cache
    if let Some(page_bytes) = self.page_cache.get(&paged_url) {
      // cache hit
      Ok(page_bytes)
    } else {
      // cache miss... fetch from adaptor
      log::debug!("Cache missed {:?}", paged_url.to_string());
      let page_range = self.page_to_range(page_idx);
      let page_bytes = self.select_adaptor(url)?.read_range(url, &page_range)?;
      self.page_cache.insert(paged_url, page_bytes.clone());  // cheap clone of Arc
      if page_bytes.len() != self.page_size {
        log::debug!("Partially fill cache {} on range {:?} with {} bytes", url.to_string(), page_range, page_bytes.len());
      }
      Ok(page_bytes)
    }
  }

  fn range_to_pages(&self, range: &Range) -> std::ops::Range<usize> {
    let last_offset = range.offset + range.length;
    range.offset / self.page_size .. last_offset / self.page_size + (last_offset % self.page_size != 0) as usize
  }

  fn page_to_range(&self, page_idx: usize) -> Range {
    Range { offset: page_idx * self.page_size, length: self.page_size }
  }

  fn paged_url(&self, mut url: Url, page_idx: usize) -> Url {
    url.set_query(Some(&format!("page={}", page_idx)));
    url
  }
}

impl Adaptor for ExternalStorage {
  fn read_all(&self, url: &Url) -> GResult<ArcBytes> {
    // TODO: cache these?
    self.select_adaptor(url)?.read_all(url)
  }

  fn read_range(&self, url: &Url, range: &Range) -> GResult<ArcBytes> {
    // read multiple pages in parallel
    let pages: Vec<ArcBytes> = self.range_to_pages(range)
      .into_par_iter()
      .map(|page_idx| self.read_through_page(url, page_idx))
      .collect::<GResult<Vec<ArcBytes>>>()?;

    // concatenate page
    let mut buf = vec![0u8; range.length];
    for (page_bytes, page_idx) in pages.iter().zip(self.range_to_pages(range)) {
      let page_range = self.page_to_range(page_idx);
      let page_l = range.offset.saturating_sub(page_range.offset);
      let page_r = std::cmp::min(page_bytes.len(), (range.offset + range.length).saturating_sub(page_range.offset));
      let buf_l = (page_range.offset + page_l).saturating_sub(range.offset);
      let buf_r = (page_range.offset + page_r).saturating_sub(range.offset);
      buf[buf_l..buf_r].clone_from_slice(&page_bytes[page_l..page_r])
    }
    Ok(Arc::new(buf))
  }

  fn create(&self, url: &Url) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.invalidate_all();
    self.select_adaptor(url)?.create(url)
  }

  fn write_all(&self, url: &Url, buf: &[u8]) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.invalidate_all();
    self.select_adaptor(url)?.write_all(url, buf)
  }

  fn remove(&self, url: &Url) -> GResult<()> {
    // TODO: use invalidate_entries_if and support_invalidation_closures to invalid some url
    self.page_cache.invalidate_all();
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
  use crate::io::storage::adaptor_test::write_all_zero_ok;
  use crate::io::storage::adaptor_test::write_read_all_random_ok;
  use crate::io::storage::adaptor_test::write_read_all_zero_ok;
  use crate::io::storage::adaptor_test::write_read_generic_random_ok;
  use crate::io::storage::adaptor_test::write_read_range_random_ok;
  use crate::io::storage::adaptor_test::write_twice_read_all_random_ok;
  use crate::io::storage::url_from_dir_path;


  /* ExternalStorage tests */

  #[test]
  fn es_write_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;
    write_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_zero_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;
    write_read_all_zero_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;
    write_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_twice_read_all_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;
    write_twice_read_all_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_range_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;
    write_read_range_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_write_read_generic_random_ok() -> GResult<()> {
    let (temp_dir, fsa) = fsa_tempdir_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;
    write_read_generic_random_ok(es, &url_from_dir_path(temp_dir.path())?)
  }

  #[test]
  fn es_read_all_ok() -> GResult<()> {
    let (resource_dir, fsa) = fsa_resources_setup()?;
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;
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
    let es = ExternalStorage::new_with_cache(65536, 17).with("file".to_string(), Box::new(fsa))?;

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