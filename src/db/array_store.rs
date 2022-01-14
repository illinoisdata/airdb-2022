use byteorder::{LittleEndian, ByteOrder};
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
use crate::io::storage::Range;
use crate::meta::context::Context;


#[derive(Serialize, Deserialize)]
pub struct ArrayStoreState {
  array_path: PathBuf,
  data_size: usize,
  offset: usize,  // in bytes, array file might contain some header
  length: usize,  // number of elements
}


pub struct ArrayStore {
  storage: Rc<ExternalStorage>,
  state: ArrayStoreState,
}

impl ArrayStore {
  pub fn new_sized(storage: Rc<ExternalStorage>, array_path: PathBuf, data_size: usize) -> ArrayStore {
    ArrayStore{
      storage,
      state: ArrayStoreState {
        array_path,
        data_size,
        offset: 0,
        length: 0,
      },
    }
  }

  // TODO: move to script level
  pub fn sosd_uint32(storage: Rc<ExternalStorage>, array_path: PathBuf, length: usize) -> ArrayStore {
    ArrayStore{
      storage,
      state: ArrayStoreState {
        array_path,
        data_size: 4,
        offset: 8,  // SOSD array leads with 8-byte encoding of the length
        length,
      },
    }
  }

  // TODO: move to script level
  pub fn sosd_uint64(storage: Rc<ExternalStorage>, array_path: PathBuf, length: usize) -> ArrayStore {
    ArrayStore{
      storage,
      state: ArrayStoreState {
        array_path,
        data_size: 8,
        offset: 8,  // SOSD array leads with 8-byte encoding of the length
        length,
      },
    }
  }

  pub fn load(ctx: &Context, state: ArrayStoreState) -> ArrayStore {
    ArrayStore{ storage: Rc::clone(&ctx.storage), state }
  }

  pub fn begin_write(&mut self) -> Result<ArrayStoreWriter, Box<dyn Error>> {
    // since we require mutable borrow, there will only be one writer in a code block.
    // this would disallow readers while the writer's lifetime as well

    // prepare the prefix directory
    self.storage.create(&PathBuf::from(""))?;

    // make the writer
    self.state.length = 0;  // TODO: append write?
    Ok(ArrayStoreWriter::new(self))
  }

  fn end_write(&mut self, written_elements: usize) {
    self.state.length += written_elements;
  }

  pub fn read_all(&self) -> Result<ArrayStoreReader, Box<dyn Error>> {
    self.read_within(0, self.state.length * self.state.data_size)
  }

  pub fn read_within(&self, offset: PositionT, length: PositionT) -> Result<ArrayStoreReader, Box<dyn Error>> {
    // read and extract dbuffer than completely fits in the range 
    let array_buffer = self.read_page_range(offset, length)?;
    Ok(ArrayStoreReader::new(array_buffer, self.state.data_size))
  }

  pub fn reconstruct_key_positions(&self) -> Result<KeyPositionCollection, Box<dyn Error>> {
    // HACK: should be at semantic level above, given the deserialization
    let mut kps = KeyPositionCollection::new();
    let reader = self.read_all()?;
    let mut current_offset = 0;
    for dbuffer in reader.iter() {
      let current_key = LittleEndian::read_uint(dbuffer, self.state.data_size);
      kps.push(current_key.try_into().unwrap(), current_offset);  // TODO: overflow?
      current_offset += self.state.data_size;
    }
    Ok(kps)
  }

  fn write_array(&self, array_buffer: &[u8]) -> io::Result<()> {
      self.storage.write_all(&self.state.array_path, array_buffer)
  }

  fn read_page_range(&self, offset: PositionT, length: PositionT) -> io::Result<Vec<u8>> {
    // calculate first and last "page" indexes
    let offset = offset + self.state.offset;
    let end_offset = offset + length;
    let start_page_idx = offset / self.state.data_size + (offset % self.state.data_size != 0) as usize;
    let end_page_idx = end_offset / self.state.data_size;

    // make read requests
    let array_buffer = self.storage.read_range(
      &self.state.array_path,
      &Range{
        offset: start_page_idx * self.state.data_size,
        length: (end_page_idx - start_page_idx) * self.state.data_size
      },
    )?;
    Ok(array_buffer)
  }
}


/* Writer */

pub struct ArrayStoreWriter<'a> {
  owner_store: &'a mut ArrayStore,

  // writing state
  array_buffer: Vec<u8>,

  // temporary full index
  key_positions: KeyPositionCollection,
}

impl<'a> ArrayStoreWriter<'a> {
  fn new(owner_store: &mut ArrayStore) -> ArrayStoreWriter {
    ArrayStoreWriter{
      owner_store,
      array_buffer: Vec::new(),
      key_positions: KeyPositionCollection::new(),
    }
  }

  pub fn write(&mut self, kb: &KeyBuffer) -> io::Result<()> {
    let key_offset = self.write_dbuffer(&kb.buffer)?;
    self.key_positions.push(kb.key, key_offset);
    Ok(())
  }

  pub fn commit(mut self) -> io::Result<KeyPositionCollection> {
    let length = self.key_positions.len();
    self.flush_array_buffer()?;
    self.owner_store.end_write(length);
    self.key_positions.set_position_range(0, length * self.owner_store.state.data_size);
    Ok(self.key_positions)
  }

  fn write_dbuffer(&mut self, dbuffer: &[u8]) -> io::Result<PositionT> {
    assert_eq!(dbuffer.len(), self.owner_store.state.data_size);
    let cur_position = self.array_buffer.len();
    self.array_buffer.extend_from_slice(dbuffer);
    Ok(cur_position)
  }

  fn flush_array_buffer(&mut self) -> io::Result<()> {
    // write to storage and step block forward
    self.owner_store.write_array(&self.array_buffer)
  }
}


/* Reader */

pub struct ArrayStoreReader {
  array_buffer: Vec<u8>,
  data_size: usize,
}

pub struct ArrayStoreReaderIter<'a> {
  r: &'a ArrayStoreReader,
  current_offset: usize,
}

impl ArrayStoreReader {
  fn new(array_buffer: Vec<u8>, data_size: usize) -> ArrayStoreReader {
    ArrayStoreReader {
      array_buffer,
      data_size,
    }
  }

  pub fn iter(&self) -> ArrayStoreReaderIter {
    ArrayStoreReaderIter{ r: self, current_offset: 0 }
  }
}

impl<'a> Iterator for ArrayStoreReaderIter<'a> {
  type Item = &'a [u8];

  fn next(&mut self) -> Option<Self::Item> {
    if self.current_offset < self.r.array_buffer.len() {
      let dbuffer = &self.r.array_buffer[self.current_offset..self.current_offset+self.r.data_size];
      self.current_offset += self.r.data_size;
      Some(dbuffer)
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

  fn generate_simple_kv() -> ([KeyT; 10], [Vec<u8>; 10]) {
    let test_keys: [KeyT; 10] = [0, 2, 8, 21, 24, 666, 667, 669, 672, 679];
    let test_buffers: [Vec<u8>; 10] = [
      vec![0u8, 0u8, 0u8, 0u8],
      vec![2u8, 0u8, 0u8, 0u8],
      vec![8u8, 0u8, 0u8, 0u8],
      vec![21u8, 0u8, 0u8, 0u8],
      vec![24u8, 0u8, 0u8, 0u8],
      vec![154u8, 2u8, 0u8, 0u8],
      vec![155u8, 2u8, 0u8, 0u8],
      vec![157u8, 2u8, 0u8, 0u8],
      vec![160u8, 2u8, 0u8, 0u8],
      vec![167u8, 2u8, 0u8, 0u8],
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
    let mut arrstore = ArrayStore::new_sized(Rc::clone(&es), PathBuf::from("test_arrstore"), 4);

    // write but never commit
    let _kps = {
      let mut bwriter = arrstore.begin_write()?;
      assert_eq!(bwriter.owner_store.state.length, 0, "Total pages should be cleared");
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer{ key: *key, buffer: value.to_vec()})?;
      }
    };
    assert_eq!(arrstore.state.length, 0, "Total pages should be zero without commit");

    // write some data
    let kps = {
      let mut bwriter = arrstore.begin_write()?;
      assert_eq!(bwriter.owner_store.state.length, 0, "Total pages should be cleared");
      for (key, value) in test_keys.iter().zip(test_buffers.iter()) {
        bwriter.write(&KeyBuffer{ key: *key, buffer: value.to_vec()})?;
      }
      bwriter.commit()?
    };
    assert!(arrstore.state.length > 0, "Total pages should be updated after writing");

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
      let reader = arrstore.read_within(cur_offset, cur_length)?;
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
      let reader = arrstore.read_within(pos_1half, pos_7half - pos_1half)?;
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
      let reader = arrstore.read_all()?;
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
