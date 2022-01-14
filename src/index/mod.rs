use std::error::Error;
use std::time::Duration;

use crate::db::key_buffer::KeyT;
use crate::db::key_position::KeyPositionRange;
use crate::io::profile::StorageProfile;


/* Index traits */

pub trait Index {
  fn predict(&self, key: &KeyT) -> Result<KeyPositionRange, Box<dyn Error>>;
  fn estimate_cost(&self, profile: &dyn StorageProfile) -> Duration;
}

pub trait PartialIndex: Index {
  fn predict_within(&self, kr: &KeyPositionRange) -> Result<KeyPositionRange, Box<dyn Error>>;
}

pub mod piecewise;
pub mod hierarchical;
