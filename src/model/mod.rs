use serde::{Serialize, Deserialize};
use std::fmt::Debug;
use std::time::Duration;

use crate::common::error::GResult;
use crate::io::profile::StorageProfile;
use crate::meta::Context;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;

type MaybeKeyBuffer = Option<KeyBuffer>;


/* Models */

pub trait Model {
  // predict position(s) for the key
  fn predict(&self, key: &KeyT) -> KeyPositionRange;
}


/* Model Deserializer */

pub trait ModelRecon: ModelReconMetaserde + Debug {
  fn reconstruct(&self, buffer: &[u8]) -> GResult<Box<dyn Model>>;
}

#[derive(Serialize, Deserialize)]
pub enum ModelReconMeta {
  DoubleLinear { meta: linear::DoubleLinearModelReconMeta },
}

pub trait ModelReconMetaserde {
  fn to_meta(&self, ctx: &mut Context) -> GResult<ModelReconMeta>;
}

impl ModelReconMeta {
  pub fn from_meta(meta: ModelReconMeta, ctx: &Context) -> GResult<Box<dyn ModelRecon>> {
    let store = match meta {
      ModelReconMeta::DoubleLinear { meta } => Box::new(linear::DoubleLinearModelRecon::from_meta(meta, ctx)?) as Box<dyn ModelRecon>,
    };
    Ok(store)
  }
}


/* Model (Incremental) Builders */

pub struct BuilderFinalReport {
  pub maybe_model_kb: MaybeKeyBuffer,  // last buffer if any
  pub serde: Box<dyn ModelRecon>,  // for future deserialization
  pub model_loads: Vec<usize>,  // search load(s) in bytes assuming having the whole model
}

pub trait ModelBuilder: Sync {
  fn consume(&mut self, kpr: &KeyPositionRange) -> GResult<MaybeKeyBuffer>;
  fn finalize(self: Box<Self>) -> GResult<BuilderFinalReport>;
}


/* Model Drafter */
// prefer this if the resulting model is not large

#[derive(Debug)]
pub struct ModelDraft {
  pub key_buffers: Vec<KeyBuffer>,
  pub serde: Box<dyn ModelRecon>,
  pub loads: Vec<usize>,
  pub cost: Duration,
}

unsafe impl Send for ModelDraft {}

pub trait ModelDrafter: Sync + Debug {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft>;
}



pub mod toolkit;
pub mod linear;
