use rayon::prelude::*;

use crate::model::BuilderFinalReport;
use crate::model::GResult;
use crate::model::KeyPositionCollection;
use crate::model::ModelBuilder;
use crate::model::ModelDraft;
use crate::model::ModelDrafter;
use crate::model::StorageProfile;
use crate::store::complexity::StepComplexity;


/* Accumulating mulitple drafters into one that tries and picks the best one */

#[derive(Debug)]
pub struct MultipleDrafter {
  drafters: Vec<Box<dyn ModelDrafter>>,
  use_parallel: bool,
}

impl MultipleDrafter {
  pub fn from(drafters: Vec<Box<dyn ModelDrafter>>) -> MultipleDrafter {
    MultipleDrafter{ drafters, use_parallel: true }
  }

  pub fn push(&mut self, drafter: Box<dyn ModelDrafter>) {
    self.drafters.push(drafter)
  }

  pub fn to_serial(mut self) -> MultipleDrafter {
    self.use_parallel = false;
    self
  }

  pub fn to_parallel(mut self) -> MultipleDrafter {
    self.use_parallel = true;
    self
  }

  fn draft_par(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> Option<ModelDraft> {
    self.drafters.par_iter()
      .map(|drafter| drafter.draft(kps, profile)
          .unwrap_or_else(|_| panic!("Drafting failed at {:?}", drafter)))
      .min_by_key(|draft| draft.cost)
  }

  fn draft_ser(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> Option<ModelDraft> {
    self.drafters.iter()
      .map(|drafter| drafter.draft(kps, profile)
          .unwrap_or_else(|_| panic!("Drafting failed at {:?}", drafter)))
      .min_by_key(|draft| draft.cost)
  }
}

impl ModelDrafter for MultipleDrafter {
  fn draft(&self, kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<ModelDraft> {
    let best_draft = match self.use_parallel {
      true => self.draft_par(kps, profile),
      false => self.draft_ser(kps, profile),
    }.expect("No draft produced (possibly drafters list is empty?)");
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

pub type BuilerProducer = dyn Fn() -> Box<dyn ModelBuilder> + Sync;

pub struct BuilderAsDrafter {
  builder_producer: Box<BuilerProducer>,
}

impl std::fmt::Debug for BuilderAsDrafter {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BuilderAsDrafter").finish()
  }
}

unsafe impl Sync for ModelDraft {}

impl BuilderAsDrafter {
  pub fn wrap(builder_producer: Box<BuilerProducer>) -> BuilderAsDrafter {
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