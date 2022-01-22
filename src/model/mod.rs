use serde::{Serialize, Deserialize};
use std::fmt::Debug;
use std::time::Duration;

use crate::common::error::GResult;
use crate::io::profile::StorageProfile;
use crate::meta::Context;
use crate::store::complexity::StepComplexity;
use crate::store::key_buffer::KeyBuffer;
use crate::store::key_position::KeyInterval;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;

type MaybeKeyBuffer = Option<KeyBuffer>;


/* Models */

pub trait Model {
  // coverage within this model
  fn coverage(&self) -> KeyInterval;  
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

pub trait ModelBuilder {
  fn consume(&mut self, kpr: &KeyPositionRange) -> GResult<MaybeKeyBuffer>;
  fn finalize(self: Box<Self>) -> GResult<BuilderFinalReport>;
}


/* Model Drafter */
// prefer this if the resulting model is not large

pub struct ModelDraft {
  pub key_buffers: Vec<KeyBuffer>,
  pub serde: Box<dyn ModelRecon>,
  pub loads: Vec<usize>,
  pub cost: Duration,
}

pub trait ModelDrafter {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft>;
}


/* Accumulating mulitple drafters into one that tries and picks the best one */

pub struct MultipleDrafter {
  drafters: Vec<Box<dyn ModelDrafter>>,
}

impl MultipleDrafter {
  pub fn from(drafters: Vec<Box<dyn ModelDrafter>>) -> MultipleDrafter {
    MultipleDrafter{ drafters }
  }

  pub fn push(&mut self, drafter: Box<dyn ModelDrafter>) {
    self.drafters.push(drafter)
  }
}

impl ModelDrafter for MultipleDrafter {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft> {
    let mut best_draft: Option<ModelDraft> = None;
    for drafter in &self.drafters {  // TODO: parallelize this?
      let draft = drafter.draft(kps, profile)?;
      best_draft = match best_draft {
        Some(the_best_draft) => if the_best_draft.cost < draft.cost {
          Some(the_best_draft)
        } else {
          Some(draft)
        },
        None => Some(draft),
      };
    }
    let best_draft = best_draft.expect("No draft produced (possibly drafters list is empty?)");
    log::info!(
      "Best drafted model: {:?}, {} submodels, loads= {:?}, cost= {:?}",
      best_draft.serde,
      best_draft.key_buffers.len(),
      best_draft.loads,
      best_draft.cost,
    );
    Ok(best_draft)
  }
}


/* Builder --> Drafter adaptor */

pub type BuilerProducer = dyn Fn() -> Box<dyn ModelBuilder>;

pub struct BuilderAsDrafter {
  builder_producer: Box<BuilerProducer>,
}

impl BuilderAsDrafter {
  fn wrap(builder_producer: Box<BuilerProducer>) -> BuilderAsDrafter {
    BuilderAsDrafter { builder_producer }
  }
}

impl ModelDrafter for BuilderAsDrafter {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft> {
    let mut model_builder = (*self.builder_producer)();
    let mut total_size = 0;
    let mut key_buffers = Vec::new();
    for kpr in kps.range_iter() {
      if let Some(model_kb) = model_builder.consume(&kpr)? {
        total_size += model_kb.buffer.len();
        key_buffers.push(model_kb);
      }
    }

    // finalize last bits of model
    let BuilderFinalReport { maybe_model_kb, serde, model_loads } = model_builder.finalize()?;
    if let Some(model_kb) = maybe_model_kb {
        total_size += model_kb.buffer.len();
        key_buffers.push(model_kb);
    }

    // estimate cost
    let (est_complexity_loads, _) = StepComplexity::measure(profile, total_size);
    let complexity_cost = profile.sequential_cost(&est_complexity_loads);
    let model_cost = profile.sequential_cost(&model_loads);
    let total_loads = [est_complexity_loads, model_loads].concat();
    let cost = profile.sequential_cost(&total_loads);
    log::info!(
      "Drafted model: {} submodels, loads= {:?}, cost= {:?} (c/m: {:?}/{:?})",
      key_buffers.len(),
      total_loads,
      cost,
      complexity_cost,
      model_cost,
    );
    Ok(ModelDraft{ key_buffers, serde, loads: total_loads, cost })
  }
}

pub mod linear;
