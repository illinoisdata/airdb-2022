use serde::{Serialize, Deserialize};
use std::error::Error;
use std::rc::Rc;

use crate::common::error::GResult;
use crate::index::Index;
use crate::index::IndexBuilder;
use crate::index::IndexMeta;
use crate::index::IndexMetaserde;
use crate::index::naive::NaiveIndex;
use crate::index::PartialIndex;
use crate::index::PartialIndexMeta;
use crate::index::piecewise::PiecewiseIndex;
use crate::io::profile::StorageProfile;
use crate::io::storage::ExternalStorage;
use crate::meta::Context;
use crate::model::ModelDrafter;
use crate::store::key_position::KeyPositionCollection;
use crate::store::key_position::KeyPositionRange;
use crate::store::key_position::KeyT;
use crate::store::store_designer::StoreDesigner;


/* Stack index */

#[derive(Debug)]
pub struct StackIndex {
  upper_index: Box<dyn Index>,
  lower_index: Box<dyn PartialIndex>,
}

impl Index for StackIndex {
  fn predict(&self, key: &KeyT) -> Result<KeyPositionRange, Box<dyn Error>> {
    let kr = self.upper_index.predict(key)?;
    self.lower_index.predict_within(&kr)
  }
}

pub struct BalanceStackIndexBuilder<'a> {
  storage: Rc<ExternalStorage>,
  drafter: Box<dyn ModelDrafter>,
  profile: &'a dyn StorageProfile,
  prefix_name: String,
}

impl<'a> BalanceStackIndexBuilder<'a> {
  pub fn new(storage: &Rc<ExternalStorage>, drafter: Box<dyn ModelDrafter>, profile: &'a dyn StorageProfile, prefix_name: String) -> BalanceStackIndexBuilder<'a> {
    BalanceStackIndexBuilder {
      storage: Rc::clone(storage),
      drafter,
      profile,
      prefix_name,
    }
  }
}

impl<'a> BalanceStackIndexBuilder<'a> {
  pub fn bns_at_layer(  // balance & stack, at layer
    &self,
    kps: &KeyPositionCollection,
    layer_idx: usize,
  ) -> GResult<Box<dyn Index>> {
    // if no index is built
    let no_index_cost = self.profile.cost(kps.total_bytes());

    // if index is built
    let model_draft = self.drafter.draft(kps, self.profile)?;

    // if this layer is profitable, stack and try next layer
    if model_draft.cost < no_index_cost {
      // persist
      let data_store = StoreDesigner::new(&self.storage)
        .design_for_kbs(&model_draft.key_buffers, &self.layer_name(layer_idx));
      let (piecewise_index, lower_index_kps) = PiecewiseIndex::craft(model_draft, data_store)?;

      // try next
      let upper_index = self.bns_at_layer(&lower_index_kps, layer_idx + 1)?;
      let lower_index = Box::new(piecewise_index) as Box<dyn PartialIndex>;
      let stack_index = Box::new(StackIndex { upper_index, lower_index });
      Ok(stack_index)
    } else {
      // fetching whole data layer is faster than building index
      Ok(Box::new(NaiveIndex::build(kps)))
    }
  }

  fn layer_name(&self, layer_idx: usize) -> String {
    format!("{}_{}", self.prefix_name, layer_idx)
  }
}

impl<'a> IndexBuilder for BalanceStackIndexBuilder<'a> {
  fn build_index(&self, kps: &KeyPositionCollection) -> GResult<Box<dyn Index>> {
    self.bns_at_layer(kps, 1)
  }
}


#[derive(Serialize, Deserialize)]
pub struct StackIndexMeta {
  upper_index: IndexMeta,
  lower_index: PartialIndexMeta,
}

impl IndexMetaserde for StackIndex {  // for Metaserde
  fn to_meta(&self, ctx: &mut Context) -> GResult<IndexMeta> {
    Ok(IndexMeta::Stack {
      meta: Box::new(StackIndexMeta {
        upper_index: self.upper_index.to_meta(ctx)?,
        lower_index: self.lower_index.to_meta_partial(ctx)?,
      })
    })
  }
}

impl StackIndex {  // for Metaserde
  pub fn from_meta(meta: StackIndexMeta, ctx: &Context) -> GResult<StackIndex> {
    Ok(StackIndex{
      upper_index: IndexMeta::from_meta(meta.upper_index, ctx)?,
      lower_index: PartialIndexMeta::from_meta_partial(meta.lower_index, ctx)?,
    })
  }
}

// pub struct StackPartialIndex {
//   upper_index: Box<dyn PartialIndex>,
//   lower_index: Box<dyn PartialIndex>,
// }

// impl Index for StackPartialIndex {
//   fn predict(&self, key: &KeyT) -> Result<KeyPositionRange, Box<dyn Error>> {
//     let intermediate_kr = self.upper_index.predict(key)?;
//     self.lower_index.predict_within(&intermediate_kr)
//   }
// }

// impl PartialIndex for StackPartialIndex {
//   fn predict_within(&self, kr: &KeyPositionRange) -> Result<KeyPositionRange, Box<dyn Error>> {
//     let intermediate_kr = self.upper_index.predict_within(kr)?;
//     self.lower_index.predict_within(&intermediate_kr)
//   }
// }