use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::time::Duration;

use crate::common::error::GResult;
use crate::db::block_store::BlockStore;
use crate::db::block_store::BlockStoreReader;
use crate::db::key_buffer::KeyT;
use crate::db::key_position::KeyPositionCollection;
use crate::db::key_position::KeyPositionRange;
use crate::index::Index;
use crate::index::PartialIndex;
use crate::io::profile::StorageProfile;
use crate::model::ModelBuilder;
use crate::model::ModelRecon;


#[derive(Debug, Clone)]
struct OutofCoverageError;

impl fmt::Display for OutofCoverageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "this index does not cover the requested key")
    }
}

impl Error for OutofCoverageError {}

pub struct PiecewiseIndex {
  bstore: BlockStore,
  model_serde: Box<dyn ModelRecon>,
}

impl PiecewiseIndex {
  fn predict_from_reader(&self, reader: BlockStoreReader, key: &KeyT) -> Result<KeyPositionRange, Box<dyn Error>> {
    for model_buffer in reader.iter() {
      let model = self.model_serde.reconstruct(model_buffer)?;
      let coverage = model.coverage();
      if coverage.cover(key) {
        return Ok(model.predict(key));
      } else if coverage.greater_than(key) {
        // already read pass the coverage for this key
        return Err(Box::new(OutofCoverageError));
      }
    }
    Err(Box::new(OutofCoverageError))
  }
}

impl Index for PiecewiseIndex {
  fn predict(&self, key: &KeyT) -> Result<KeyPositionRange, Box<dyn Error>> {
    let reader = self.bstore.read_all()?;
    self.predict_from_reader(reader, key)
  }

  fn estimate_cost(&self, _profile: &dyn StorageProfile) -> Duration {
    panic!("TODO")
  }
}

impl PartialIndex for PiecewiseIndex {
  fn predict_within(&self, kr: &KeyPositionRange) -> Result<KeyPositionRange, Box<dyn Error>> {
    let reader = self.bstore.read_within(kr.offset, kr.length)?;
    self.predict_from_reader(reader, &kr.key)
  }
}

impl PiecewiseIndex {
  pub fn build(
    mut model_builder: Box<dyn ModelBuilder>,
    mut bstore: BlockStore,
    kps: &KeyPositionCollection
  ) -> GResult<(PiecewiseIndex, KeyPositionCollection)> {
    let mut bwriter = bstore.begin_write()?;
    for kp in kps.iter() {
      if let Some(model_kp) = model_builder.consume(kp)? {
        bwriter.write(&model_kp)?;
      }
    }

    // finalize last bits of model
    let (maybe_model_kp, model_serde) = model_builder.finalize()?;
    if let Some(model_kp) = maybe_model_kp {
        bwriter.write(&model_kp)?;
    }

    // commit write to get resulting key-position collection and return
    let new_kps = bwriter.commit()?;
    Ok((PiecewiseIndex{ bstore, model_serde }, new_kps))
  }
}
