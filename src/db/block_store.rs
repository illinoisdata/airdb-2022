use serde::{Serialize, Deserialize};
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::rc::Rc;

use crate::db::key_buffer::KeyBuffer;
use crate::db::key_position::KeyPositionCollection;
use crate::db::key_position::PositionT;
use crate::io::storage::Adaptor;
use crate::io::storage::ExternalStorage;
use crate::io::storage::ReadRequest;
use crate::io::storage::Range;
use crate::meta::context::Context;


/* Page format */

type FlagT = u32;  // TODO: smaller/larger flag?
const FLAG_LENGTH: usize = std::mem::size_of::<FlagT>();
const CONT_FLAG: FlagT = 0;

fn write_page(page: &mut [u8], flag: FlagT, kv_chunk: &[u8]) {
  // TODO: move CONT_FLAG < 0, then write only one byte
  let chunk_length = kv_chunk.len();
  page[..FLAG_LENGTH].clone_from_slice(&flag.to_be_bytes());
  page[FLAG_LENGTH..FLAG_LENGTH+chunk_length].clone_from_slice(kv_chunk);
}

fn read_page(page: &[u8]) -> (FlagT, &[u8]) {
  // TODO: if leading bit is 1 --> CONT_FLAG
  let mut flag_bytes = [0u8; FLAG_LENGTH];
  flag_bytes[..FLAG_LENGTH].clone_from_slice(&page[..FLAG_LENGTH]);
  (FlagT::from_be_bytes(flag_bytes), &page[FLAG_LENGTH..])
}


/* Main block store */

#[derive(Serialize, Deserialize)]
pub struct BlockStoreConfig {
  prefix: PathBuf,
  block_size: usize,
  page_size: usize,
}

impl BlockStoreConfig {
  fn new(prefix_name: &str) -> BlockStoreConfig {
    BlockStoreConfig {
        prefix: PathBuf::from(prefix_name),
        block_size: 1 << 32,  // 4GB
        page_size: 32,
    }
  }

  pub fn prefix(mut self, prefix: PathBuf) -> BlockStoreConfig {
    self.prefix = prefix;
    self
  }

  pub fn block_size(mut self, block_size: usize) -> BlockStoreConfig {
    self.block_size = block_size;
    self
  }

  pub fn page_size(mut self, page_size: usize) -> BlockStoreConfig {
    self.page_size = page_size;
    self
  }

  pub fn build(self, storage: Rc<ExternalStorage>) -> BlockStore {
    BlockStore::new(storage, self)
  }
}

#[derive(Serialize, Deserialize)]
pub struct BlockStoreState {
  cfg: BlockStoreConfig,
  total_pages: usize,
}


pub struct BlockStore {
  storage: Rc<ExternalStorage>,
  state: BlockStoreState,
}

impl BlockStore {
  fn new(storage: Rc<ExternalStorage>, cfg: BlockStoreConfig) -> BlockStore {
    BlockStore{
      storage,
      state: BlockStoreState {
        cfg,
        total_pages: 0,
      },
    }
  }

  pub fn load(ctx: &Context, state: BlockStoreState) -> BlockStore {
    BlockStore{ storage: Rc::clone(&ctx.storage), state }
  }

  pub fn builder(prefix_name: &str) -> BlockStoreConfig {
    BlockStoreConfig::new(prefix_name)
  }

  pub fn begin_write(&mut self) -> Result<BlockStoreWriter, Box<dyn Error>> {
    // since we require mutable borrow, there will only be one writer in a code block.
    // this would disallow readers while the writer's lifetime as well

    // prepare the prefix directory
    self.storage.create(&self.state.cfg.prefix)?;

    // make the writer
    self.state.total_pages = 0;  // TODO: append write?
    Ok(BlockStoreWriter::new(self))
  }

  fn end_write(&mut self, written_pages: usize) {
    self.state.total_pages += written_pages;
  }

  pub fn read_all(&self) -> Result<BlockStoreReader, Box<dyn Error>> {
    self.read_within(0, self.state.total_pages * self.state.cfg.page_size)
  }

  pub fn read_within(&self, offset: PositionT, length: PositionT) -> Result<BlockStoreReader, Box<dyn Error>> {
    // read and extract dbuffer than completely fits in the range 
    let (chunk_flags, chunks_buffer) = self.read_page_range(offset, length)?;
    let chunk_size = self.chunk_size();
    Ok(BlockStoreReader::new(chunk_flags, chunks_buffer, chunk_size))
  }

  fn chunk_size(&self) -> usize {
    self.state.cfg.page_size - FLAG_LENGTH 
  }

  fn pages_per_block(&self) -> usize {
    self.state.cfg.block_size / self.state.cfg.page_size
  }

  fn block_path(&self, block_idx: usize) -> PathBuf {
    let block_name = format!("block_{}", block_idx);
    self.state.cfg.prefix.join(block_name)
  }

  fn write_block(&self, block_idx: usize, block_buffer: &[u8]) -> io::Result<()> {
      let block_path = self.block_path(block_idx);
      self.storage.write_all(&block_path, block_buffer)
  }

  fn read_page_range(&self, offset: PositionT, length: PositionT) -> io::Result<(Vec<FlagT>, Vec<u8>)> {
    // calculate first and last page indexes
    let end_offset = offset + length;
    let start_page_idx = offset / self.state.cfg.page_size + (offset % self.state.cfg.page_size != 0) as usize;
    let end_page_idx = end_offset / self.state.cfg.page_size;

    // make read requests
    let requests = self.read_page_range_requests(start_page_idx, end_page_idx);
    let section_buffers = self.storage.read_batch_sequential(&requests)?;
    let mut flags = Vec::new();
    let mut chunks_buffer = Vec::new();
    for section_buffer in section_buffers {
      assert_eq!(section_buffer.len() % self.state.cfg.page_size, 0);
      for page in section_buffer.chunks(self.state.cfg.page_size) {
        let (flag, chunk) = read_page(page);
        flags.push(flag);
        chunks_buffer.extend(chunk);
      }
    }
    Ok((flags, chunks_buffer))
  }

  fn read_page_range_requests(&self, mut start_page_idx: usize, end_page_idx: usize) -> Vec<ReadRequest> {
    let pages_per_block = self.state.cfg.block_size / self.state.cfg.page_size;
    let mut start_block_idx = start_page_idx / pages_per_block;
    let mut read_requests = Vec::new();
    while start_page_idx < end_page_idx {
      // calculate current section boundaries
      let start_section_offset = (start_page_idx % pages_per_block) * self.state.cfg.page_size;
      let end_section_page_idx = if end_page_idx / pages_per_block == start_block_idx {
        // the end is in the same block
        end_page_idx
      } else {
        // more blocks to read... read til the end of this block for now
        (start_block_idx + 1) * pages_per_block
      };
      let end_section_offset = (end_section_page_idx % pages_per_block) * self.state.cfg.page_size;
      let section_length = end_section_offset - start_section_offset;

      // add read request for this section
      read_requests.push(ReadRequest::Range {
        path: self.block_path(start_block_idx),
        range: Range{ offset: start_section_offset, length: section_length },
      });

      // step forward
      start_page_idx = end_section_page_idx;
      start_block_idx += 1;
    }
    read_requests
  }
}

// impl<'s> Metaserde for BlockStore<'s> {
//   fn to_meta(&self, ctx: &mut Context) -> Result<Vec<u8>, Box<dyn Error>> {
//     let mut s = flexbuffers::FlexbufferSerializer::new();
//     self.state.serialize(&mut s)?;
//     Ok(s.take_buffer())
//   }

//   fn from_meta(ctx: &Context, meta: &[u8]) -> Result<BlockStore<'s>, Box<dyn Error>> {
//     let r = flexbuffers::Reader::get_root(meta)?;
//     let state = BlockStoreState::deserialize(r)?;
//     Ok(BlockStore::load(ctx, state))
//   }
// }


/* Writer */

pub struct BlockStoreWriter<'a> {
  owner_store: &'a mut BlockStore,

  // writing state
  block_buffer: Vec<u8>,
  block_idx: usize,
  page_idx: usize,

  // shortcuts for calculation
  chunk_size: usize,
  pages_per_block: usize,

  // temporary full index
  key_positions: KeyPositionCollection,
}

impl<'a> BlockStoreWriter<'a> {
  fn new(owner_store: &mut BlockStore) -> BlockStoreWriter {
    let block_buffer = vec![0; owner_store.state.cfg.block_size];
    let chunk_size = owner_store.chunk_size();
    let pages_per_block = owner_store.pages_per_block();
    BlockStoreWriter{
      owner_store,
      block_buffer,
      block_idx: 0,
      page_idx: 0,
      chunk_size,
      pages_per_block,
      key_positions: KeyPositionCollection::new(),
    }
  }

  pub fn write(&mut self, kb: &KeyBuffer) -> io::Result<()> {
    let key_offset = self.write_dbuffer(&kb.buffer)?;
    self.key_positions.push(kb.key, key_offset);
    Ok(())
  }

  pub fn commit(mut self) -> io::Result<KeyPositionCollection> {
    self.flush_current_block()?;
    self.owner_store.end_write(self.page_idx);
    self.key_positions.set_position_range(0, self.page_idx * self.owner_store.state.cfg.page_size);
    Ok(self.key_positions)
  }

  fn write_dbuffer(&mut self, dbuffer: &[u8]) -> io::Result<PositionT> {
    let key_offset = self.page_idx * self.owner_store.state.cfg.page_size;
    let mut flag = FlagT::try_from(dbuffer.len()).ok().unwrap();
    for kv_chunk in dbuffer.chunks(self.chunk_size) {
      // write this chunk to current page
      let page_buffer = self.page_to_write()?;
      write_page(page_buffer, flag, kv_chunk);

      // next pages are continuation
      flag = CONT_FLAG;
    }
    Ok(key_offset)
  }

  fn page_to_write(&mut self) -> io::Result<&mut [u8]> {
    let page_size = self.owner_store.state.cfg.page_size;

    // get the buffer
    let page_buffer = if self.page_idx < (self.block_idx + 1) * self.pages_per_block {
      // continue writing in current block
      let page_offset = (self.page_idx % self.pages_per_block) * page_size;
      &mut self.block_buffer[page_offset .. page_offset+page_size]
    } else {
      // next page is in the new block, flush first
      self.flush_current_block()?;
      &mut self.block_buffer[0..page_size]
    };

    // forward page_idx
    self.page_idx += 1;

    Ok(page_buffer)
    
    // return the next page slice
  }

  fn flush_current_block(&mut self) -> io::Result<()> {
    // write up to written page
    let written_buffer = if self.page_idx < (self.block_idx + 1) * self.pages_per_block {
      let written_length = (self.page_idx % self.pages_per_block) * self.owner_store.state.cfg.page_size;
      &self.block_buffer[0 .. written_length]
    } else {
      &self.block_buffer
    };

    // write to storage and step block forward
    self.owner_store.write_block(self.block_idx, written_buffer)?;
    self.block_idx += 1;
    Ok(())
  }
}

// impl<'s, 'a> Drop for BlockStoreWriter<'s, 'a> {
//   fn drop(&mut self) {
//     self.owner_store.end_write(0)
//   }
// }


/* Reader */

pub struct BlockStoreReader {
  chunk_flags: Vec<FlagT>,
  chunks_buffer: Vec<u8>,
  chunk_idx_first: usize,
  chunk_size: usize,
}

pub struct BlockStoreReaderIter<'a> {
  r: &'a BlockStoreReader,
  chunk_idx: usize,
}

impl BlockStoreReader {
  fn new(chunk_flags: Vec<FlagT>, chunks_buffer: Vec<u8>, chunk_size: usize) -> BlockStoreReader {
    // seek first valid page
    let mut chunk_idx = 0;
    while chunk_idx < chunk_flags.len() && chunk_flags[chunk_idx] == CONT_FLAG {
      chunk_idx += 1;
    }

    BlockStoreReader {
      chunk_flags,
      chunks_buffer,
      chunk_idx_first: chunk_idx,
      chunk_size,
    }
  }

  pub fn iter(&self) -> BlockStoreReaderIter {
    BlockStoreReaderIter{ r: self, chunk_idx: self.chunk_idx_first }
  }
}

impl<'a> Iterator for BlockStoreReaderIter<'a> {
  type Item = &'a [u8];

  fn next(&mut self) -> Option<Self::Item> {
    if self.chunk_idx < self.r.chunk_flags.len() {
      // calculate boundary
      let dbuffer_offset = self.chunk_idx * self.r.chunk_size;
      let dbuffer_length = usize::try_from(self.r.chunk_flags[self.chunk_idx]).ok().unwrap();
      assert_ne!(dbuffer_length, 0);
      if dbuffer_offset + dbuffer_length < self.r.chunks_buffer.len() {
        // move chunk index
        self.chunk_idx += dbuffer_length / self.r.chunk_size + (dbuffer_length % self.r.chunk_size != 0) as usize;

        // return the kp buffer slice
        Some(&self.r.chunks_buffer[dbuffer_offset .. dbuffer_offset + dbuffer_length])
      } else {
        // didn't read the whole buffer
        None
      }
    } else {
      None
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use tempfile::TempDir;
  use crate::db::key_buffer::KeyT;
  use crate::io::storage::FileSystemAdaptor;

  fn generate_simple_kv() -> ([KeyT; 14], [Box<[u8]>; 14]) {
    let test_keys: [KeyT; 14] = [
      -50,
      100,
      200,
      1,
      2,
      4,
      8,
      16,
      32,
      64,
      128,
      256,
      512,
      1024,
    ];
    let test_buffers: [Box<[u8]>; 14] = [
      Box::new([255u8]),
      Box::new([1u8, 1u8, 2u8, 3u8, 5u8, 8u8, 13u8, 21u8]),
      Box::new([0u8; 256]),
      Box::new([0u8; 1]),
      Box::new([0u8; 2]),
      Box::new([0u8; 4]),
      Box::new([0u8; 8]),
      Box::new([0u8; 16]),
      Box::new([0u8; 32]),
      Box::new([0u8; 64]),
      Box::new([0u8; 128]),
      Box::new([0u8; 256]),
      Box::new([0u8; 512]),
      Box::new([0u8; 1024]),
    ];
    (test_keys, test_buffers)
  }

  #[test]
  fn read_write_full_test() -> Result<(), Box<dyn Error>> {
    let (test_keys, test_buffers) = generate_simple_kv();

    // setup a block store
    let temp_dir = TempDir::new()?;
    let fsa = FileSystemAdaptor::new(&temp_dir);
    let es = Rc::new(ExternalStorage::new(Box::new(fsa)));
    let mut bstore = BlockStore::builder("test_bstore").build(Rc::clone(&es));

    // write but never commit
    let _kps = {
      let mut bwriter = bstore.begin_write()?;
      assert_eq!(bwriter.owner_store.state.total_pages, 0, "Total pages should be cleared");
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer{ key: *key, buffer: value.to_vec()})?;
      }
    };
    assert_eq!(bstore.state.total_pages, 0, "Total pages should be zero without commit");

    // write some data
    let kps = {
      let mut bwriter = bstore.begin_write()?;
      assert_eq!(bwriter.owner_store.state.total_pages, 0, "Total pages should be cleared");
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer{ key: *key, buffer: value.to_vec()})?;
      }
      bwriter.commit()?
    };
    assert!(bstore.state.total_pages > 0, "Total pages should be updated after writing");

    // check monotonicity of the key-position pairs
    let mut prev_position = 0;  // position must be at least zero
    for (key, kp) in test_keys.iter().zip(kps.iter()) {
      assert_eq!(*key, kp.key, "Key must be written in order of insertions");
      assert!(prev_position <= kp.position, "Positions must be non-decreasing");
      prev_position = kp.position;
    }

    // check rereading from position
    for idx in 0..kps.len() {
      let kr = kps.range_at(idx)?;
      let cur_offset = kr.offset;
      let cur_length = kr.length;
      let reader = bstore.read_within(cur_offset, cur_length)?;
      let mut reader_iter = reader.iter();

      // check correctness
      let dbuffer = reader_iter.next().expect("Expect more data buffer");
      // assert_eq!(kb.key, cur_key, "Read key does not match with the given map");
      // assert_eq!(kb.key, test_keys[idx], "Read key does not match");
      assert_eq!(dbuffer, test_buffers[idx].to_vec(), "Read buffer does not match");

      // check completeness
      assert!(reader_iter.next().is_none(), "Expected no more data buffer")
    }

    // check reading partially, unaligned
    {
      // read in from between(1, 2) and between(7, 8)... should ignore 1 + 7
      let pos_1 = kps[1].position;
      let pos_2 = kps[2].position;
      let pos_1half = (pos_1 + pos_2) / 2;
      let pos_7 = kps[7].position;
      let pos_8 = kps[8].position;
      let pos_7half = (pos_7 + pos_8) / 2;
      let reader = bstore.read_within(pos_1half, pos_7half - pos_1half)?;
      let mut reader_iter = reader.iter();

      // should read 2, 3, 4, 5, 6 pairs
      for idx in 2..7 {  
        let dbuffer = reader_iter.next().expect("Expect more data buffer");
        // assert_eq!(kb.key, test_keys[idx], "Read key does not match (partial)");
        assert_eq!(dbuffer, test_buffers[idx].to_vec(), "Read buffer does not match (partial)");
      }
      assert!(reader_iter.next().is_none(), "Expected no more data buffer (partial)")
    }

    // check reading all
    {
      let reader = bstore.read_all()?;
      let mut reader_iter = reader.iter();
      for cur_value in test_buffers.iter() {
        // get next and check correctness
        let dbuffer = reader_iter.next().expect("Expect more data buffer");
        // assert_eq!(kb.key, *cur_key, "Read key does not match");
        assert_eq!(dbuffer, cur_value.to_vec(), "Read buffer does not match");
      } 
      assert!(reader_iter.next().is_none(), "Expected no more data buffer (read all)")
    }

    Ok(())
  }
}
