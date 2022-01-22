use serde::{Serialize, Deserialize};
use std::error::Error;
use std::fmt;
use std::fmt::Debug;

use crate::common::error::GResult;
use crate::index::Index;
use crate::index::IndexMeta;
use crate::index::IndexMetaserde;
use crate::index::PartialIndex;
use crate::index::PartialIndexMeta;
use crate::index::PartialIndexMetaserde;
use crate::meta::Context;
use crate::model::BuilderFinalReport;
use crate::model::ModelBuilder;
use crate::model::ModelDraft;
use crate::model::ModelRecon;
use crate::model::ModelReconMeta;
use crate::store::DataStore;
use crate::store::DataStoreMeta;
use crate::store::DataStoreReader;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;


#[derive(Debug, Clone)]
struct OutofCoverageError;

impl fmt::Display for OutofCoverageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "this index does not cover the requested key")
    }
}

impl Error for OutofCoverageError {}

#[derive(Debug)]
pub struct PiecewiseIndex {
  data_store: Box<dyn DataStore>,
  model_serde: Box<dyn ModelRecon>,
}

impl PiecewiseIndex {
  fn predict_from_reader(&self, reader: Box<dyn DataStoreReader>, key: &KeyT) -> GResult<KeyPositionRange> {
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
  fn predict(&self, key: &KeyT) -> GResult<KeyPositionRange> {
    let reader = self.data_store.read_all()?;
    self.predict_from_reader(reader, key)
  }
}

impl PartialIndex for PiecewiseIndex {
  fn predict_within(&self, kr: &KeyPositionRange) -> GResult<KeyPositionRange> {
    let reader = self.data_store.read_within(kr.offset, kr.length)?;
    self.predict_from_reader(reader, &kr.key)
  }
}

impl PiecewiseIndex {
  pub fn build(
    mut model_builder: Box<dyn ModelBuilder>,
    mut data_store: Box<dyn DataStore>,
    kps: &KeyPositionCollection
  ) -> GResult<(PiecewiseIndex, KeyPositionCollection)> {  // maybe remove?
    let mut data_writer = data_store.begin_write()?;
    for kpr in kps.range_iter() {
      if let Some(model_kb) = model_builder.consume(&kpr)? {
        data_writer.write(&model_kb)?;
      }
    }

    // finalize last bits of model
    let BuilderFinalReport {
      maybe_model_kb,
      serde: model_serde,
      model_loads: _,
    } = model_builder.finalize()?;
    if let Some(model_kb) = maybe_model_kb {
        data_writer.write(&model_kb)?;
    }

    // commit write to get resulting key-position collection and return
    let new_kps = data_writer.commit()?;
    log::info!(
      "{:?}: built {} buffers, total {} bytes",
      data_store,
      new_kps.len(),
      new_kps.total_bytes(),
    );
    Ok((PiecewiseIndex{ data_store, model_serde }, new_kps))
  }

  pub fn craft(
    model_draft: ModelDraft,
    mut data_store: Box<dyn DataStore>,
  ) -> GResult<(PiecewiseIndex, KeyPositionCollection)> {
    let mut data_writer = data_store.begin_write()?;

    // write the model key buffers
    let (model_kbs, model_serde) = (model_draft.key_buffers, model_draft.serde);
    for model_kb in model_kbs {
      data_writer.write(&model_kb)?;
    }

    // commit write to get resulting key-position collection and return
    let new_kps = data_writer.commit()?;
    log::info!(
      "{:?}: crafted {} buffers, total {} bytes",
      data_store,
      new_kps.len(),
      new_kps.total_bytes(),
    );
    Ok((PiecewiseIndex{ data_store, model_serde }, new_kps))
  }
}


#[derive(Serialize, Deserialize)]
pub struct PiecewiseIndexMeta {
  data_store: DataStoreMeta,
  model_serde: ModelReconMeta,
}

impl IndexMetaserde for PiecewiseIndex {  // for Metaserde
  fn to_meta(&self, ctx: &mut Context) -> GResult<IndexMeta> {
    Ok(IndexMeta::Piecewise {
      meta: PiecewiseIndexMeta {
        data_store: self.data_store.to_meta(ctx)?,
        model_serde: self.model_serde.to_meta(ctx)?,
      }
    })
  }
}

impl PiecewiseIndex {  // for Metaserde
  pub fn from_meta(meta: PiecewiseIndexMeta, ctx: &Context) -> GResult<PiecewiseIndex> {
    Ok(PiecewiseIndex {
      data_store: DataStoreMeta::from_meta(meta.data_store, ctx)?,
      model_serde: ModelReconMeta::from_meta(meta.model_serde, ctx)?,
    })
  }
}

impl PartialIndexMetaserde for PiecewiseIndex {  // for Metaserde
  fn to_meta_partial(&self, ctx: &mut Context) -> GResult<PartialIndexMeta> {
    Ok(PartialIndexMeta::Piecewise {
      meta: PiecewiseIndexMeta {
        data_store: self.data_store.to_meta(ctx)?,
        model_serde: self.model_serde.to_meta(ctx)?,
      }
    })
  }
}

impl PiecewiseIndex {  // for Metaserde
  pub fn from_meta_partial(meta: PiecewiseIndexMeta, ctx: &Context) -> GResult<PiecewiseIndex> {
    Ok(PiecewiseIndex {
      data_store: DataStoreMeta::from_meta(meta.data_store, ctx)?,
      model_serde: ModelReconMeta::from_meta(meta.model_serde, ctx)?,
    })
  }
}