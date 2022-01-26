use byteorder::ByteOrder;
use byteorder::LittleEndian;
use rand::Rng;
use rand::SeedableRng;
use rand_pcg::Pcg64;
use serde::{Serialize, Deserialize};
use sscanf::scanf;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;

use crate::common::error::GResult;
use crate::index::Index;
use crate::index::IndexBuilder;
use crate::index::IndexMeta;
use crate::meta::Context;
use crate::store::array_store::ArrayStore;
use crate::store::array_store::ArrayStoreState;
use crate::store::DataStore;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyT;


#[derive(Debug)]
pub struct KeyRank {
  pub key: KeyT,
  pub rank: usize,  // from 0 to n-1
}


/* DB that manages key and compute their ranks */

pub struct SOSDRankDB {
  array_store: ArrayStore,
  index: Option<Box<dyn Index>>,
}

impl SOSDRankDB {

  pub fn new(array_store: ArrayStore) -> SOSDRankDB {
    SOSDRankDB { array_store, index: None }
  }

  pub fn build_index(&mut self, index_builder: Box<dyn IndexBuilder>) -> GResult<()> {
    let kps = self.reconstruct_key_positions()?;
    self.attach_index(index_builder.build_index(&kps)?);
    Ok(())
  }

  pub fn attach_index(&mut self, index: Box<dyn Index>) {
    self.index = Some(index)
  }

  pub fn rank_of(&self, key: KeyT) -> GResult<Option<KeyRank>> {
    let kpr = self.index
      .as_ref()
      .expect("Index missing, trying to accessing empty data store")
      .predict(&key)?;
    let reader = self.array_store.read_array_within(kpr.offset, kpr.length)?;
    for (dbuffer, rank) in reader.iter_with_rank() {
      let current_key = self.deserialize_key(dbuffer);
      if current_key == key {
        return Ok(Some(KeyRank{ key: current_key, rank }));
      }
    }
    Ok(None)  // no entry with matching key
  }

  pub fn reconstruct_key_positions(&self) -> GResult<KeyPositionCollection> {
    // SOSD blob contains uint32/uint64s written next to each other
    // We can reconstruct the kps by multiplying the rank with data size
    let mut kps = KeyPositionCollection::new();
    let reader = self.array_store.read_all()?;
    let mut current_offset = 0;
    let mut last_key = 0;
    for dbuffer in reader.iter() {
      let current_key = self.deserialize_key(dbuffer);
      if last_key == current_key {
        continue
      } else {
        assert!(last_key < current_key);
        kps.push(current_key, current_offset);  // TODO: overflow?
        current_offset += self.array_store.data_size();
        last_key = current_key
      }
    }
    kps.set_position_range(0, current_offset);
    Ok(kps)
  }

  fn deserialize_key(&self, dbuffer: &[u8]) -> KeyT {
    LittleEndian::read_uint(dbuffer, self.array_store.data_size())
  }
}


#[derive(Serialize, Deserialize)]
pub struct SOSDRankDBMeta {
  array_store_state: ArrayStoreState,
  index: Option<IndexMeta>,
}

impl SOSDRankDB {  // for Metaserde
  pub fn to_meta(self, ctx: &mut Context) -> GResult<SOSDRankDBMeta> {
    Ok(SOSDRankDBMeta {
      array_store_state: self.array_store.to_meta_state(ctx)?,
      index: match self.index {
        Some(index) => Some(index.to_meta(ctx)?),
        None => None,
      }
    })
  }

  pub fn from_meta(meta: SOSDRankDBMeta, ctx: &Context) -> GResult<SOSDRankDB> {
    Ok(SOSDRankDB {
      array_store: ArrayStore::from_meta(meta.array_store_state, ctx)?,
      index: match meta.index {
        Some(index_meta) => Some(IndexMeta::from_meta(index_meta, ctx)?),
        None => None,
      },
    })
  }
}

pub fn generate_keyset(kps: &KeyPositionCollection, keyset_path: String, num_keyset: usize) -> GResult<()> {
  let mut keyset_file = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(keyset_path.as_str())?;
  let mut rng = Pcg64::seed_from_u64(54613789);  // "random" seed via cat typing asdasd

  for _ in 0..num_keyset {
    let rank = rng.gen_range(0..kps.len());
    let kp = &kps[rank];  // assume key-position is sorted by key
    writeln!(&mut keyset_file, "{} {}", kp.key, rank)?;
  }
  Ok(())
}

pub fn read_keyset(keyset_path: String) -> GResult<Vec<KeyRank>> {
  let keyset_file = OpenOptions::new()
    .read(true)
    .open(keyset_path.as_str())?;
  let reader = BufReader::new(keyset_file);
  Ok(reader.lines().map(|line| {
    let (key, rank) = scanf!(line.unwrap(), "{} {}", KeyT, usize).unwrap();
    KeyRank { key, rank }
  }).collect())
}