use moka::sync::Cache;
// use rayon::prelude::*;
use std::collections::HashMap;
use std::ops::Bound;
use std::ops::Bound::Excluded;
use std::ops::Bound::Included;
use std::sync::Arc;
use std::sync::RwLock;
use unbounded_interval_tree::IntervalTree;
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
  page_cache: Cache<Url, ArcIntervalCache>,
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
    // ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 12 /* 4096 */)
    ExternalStorage::new_with_cache(1 << 33 /* 8 GB */, 1 << 27 /* 128 MB */)
  }

  pub fn new_with_cache(cache_size: u64, page_size: usize) -> ExternalStorage {
    ExternalStorage{
      adaptors: HashMap::new(),
      schemes: Vec::new(),
      page_cache: Cache::builder()
        .weigher(|_key, value: &ArcIntervalCache| -> u32 {
          value.read().unwrap().len().try_into().unwrap()
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
    // TODO: async this?
    requests
      // .par_iter()
      .iter()
      .map(|request| self.read(request))
      .collect()
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

type Interval = (Bound<usize>, Bound<usize>);

fn range_to_interval(range: &Range) -> Interval {
  (Included(range.offset), Excluded(range.offset + range.length))
}

fn intervals_to_range(interval: &Interval) -> Range {
  if let (Included(offset_l), Excluded(offset_r)) = interval {
    return Range { offset: *offset_l, length: offset_r - offset_l }
  }
  panic!("Should only contain include-exclude ranges")
}

type ArcIntervalCache = Arc<RwLock<IntervalCache>>;

struct IntervalCache {
  cache_offset: usize,
  buffer: Vec<u8>,
  intervals: IntervalTree<usize>,
}

impl std::fmt::Debug for IntervalCache {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("IntervalCache")
      .field("offset", &self.cache_offset)
      .field("length", &self.buffer.len())
      .field("num_intervals", &self.intervals.len())
      .finish()
  }
}

impl IntervalCache {
  fn new(cache_offset: usize, length: usize) -> ArcIntervalCache {
    Arc::new(RwLock::new(IntervalCache {
      cache_offset,
      buffer: vec![0u8; length],
      intervals: IntervalTree::default(),
    }))
  }

  fn from_filled(cache_offset: usize, buffer: Vec<u8>) -> ArcIntervalCache {
    let mut intervals = IntervalTree::default();
    intervals.insert((Included(cache_offset), Excluded(cache_offset + buffer.len())));
    Arc::new(RwLock::new(IntervalCache {
      cache_offset,
      buffer,
      intervals,
    }))
  }

  fn get_bytes(&self) -> &[u8] {
    &self.buffer
  }

  fn fill_if_missing(&mut self, es: &ExternalStorage, url: &Url, range: &Range) -> GResult<()> {
    let diff_intervals = self.intervals.get_interval_difference(self.intersect(range));
    let num_diff = diff_intervals.len();
    if num_diff > 0 {
      // smallest single missing interval (single large IO is better than parallel small IOs)
      let range = intervals_to_range(&(diff_intervals[0].0, diff_intervals[num_diff - 1].1));
      let offset_l = range.offset - self.cache_offset;
      let offset_r = range.offset + range.length - self.cache_offset;

      // read into the buffer interval
      log::debug!("Cache interval missed {}, range= {:?}", url.to_string(), range);
      es.select_adaptor(url)?.read_in_place(url, &range, &mut self.buffer[offset_l .. offset_r])?;

      // update interval
      self.intervals.insert(range_to_interval(&range));
    }
    Ok(())
  }

  fn len(&self) -> usize {
    self.buffer.len()
  }

  fn intersect(&self, range: &Range) -> Interval {
    let offset = std::cmp::max(range.offset, self.cache_offset);
    let offset_r = std::cmp::min(range.offset + range.length, self.cache_offset + self.buffer.len());
    range_to_interval(&Range { offset, length: offset_r - offset })
  }
}

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
        self.page_cache.insert(paged_url, IntervalCache::from_filled(page_range.offset, page_bytes))
      });
    log::debug!("Warmed up cache for {:?}", url.to_string());
  }

  fn fill_in_range(&self, cache_line: &mut ArcIntervalCache, url: &Url, range: &Range) -> GResult<()> {
    cache_line.write().unwrap().fill_if_missing(self, url, range)
  }

  fn read_through_page(&self, url: &Url, page_idx: usize, range: &Range) -> GResult<(usize, ArcIntervalCache)> {
    // make url with page idx
    let paged_url = self.paged_url(url.clone(), page_idx);

    // check in cache
    if let Some(mut cache_line) = self.page_cache.get(&paged_url) {
      // cache hit
      self.fill_in_range(&mut cache_line, url, range)?;  // data
      Ok((page_idx, cache_line))
    } else {
      // cache miss... fetch from adaptor
      log::debug!("Cache line missing {:?}", paged_url.to_string());
      let mut cache_line = IntervalCache::new(page_idx * self.page_size, self.page_size);
      self.page_cache.insert(paged_url, cache_line.clone());  // cheap clone of Arc
      self.fill_in_range(&mut cache_line, url, range)?;  // data
      Ok((page_idx, cache_line))
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
    self.select_adaptor(url)?.read_all(url)
  }

  fn read_range(&self, url: &Url, range: &Range) -> GResult<ArcBytes> {
    let mut buffer = vec![0u8; range.length];
    self.read_in_place(url, range, &mut buffer)?;
    Ok(Arc::new(buffer))
  }

  fn read_in_place(&self, url: &Url, range: &Range, buffer: &mut [u8]) -> GResult<()> {
    // read multiple pages in parallel
    let pages = self.range_to_pages(range)
      // .into_par_iter()
      .map(|page_idx| self.read_through_page(url, page_idx, range))
      .collect::<GResult<Vec<(usize, ArcIntervalCache)>>>()?;

    // concatenate page
    for (page_idx, page_cache) in pages {
      let locked_page_cache = page_cache.read().unwrap();
      let page_bytes = locked_page_cache.get_bytes();
      let page_range = self.page_to_range(page_idx);
      let page_l = range.offset.saturating_sub(page_range.offset);
      let page_r = std::cmp::min(page_bytes.len(), (range.offset + range.length).saturating_sub(page_range.offset));
      let buf_l = (page_range.offset + page_l).saturating_sub(range.offset);
      let buf_r = (page_range.offset + page_r).saturating_sub(range.offset);
      buffer[buf_l..buf_r].clone_from_slice(&page_bytes[page_l..page_r])
    }
    Ok(())
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