use crate::common::error::GResult;
use crate::db::key_buffer::KeyT;
use crate::db::key_buffer::KeyBuffer;
use crate::db::key_position::KeyInterval;
use crate::db::key_position::KeyPosition;
use crate::db::key_position::KeyPositionRange;

type MaybeKeyBuffer = Option<KeyBuffer>;


/* Models */

pub trait Model {
  // coverage within this model
  fn coverage(&self) -> KeyInterval;  
  // predict position(s) for the key
  fn predict(&self, key: &KeyT) -> KeyPositionRange;
}


/* Model Deserializer */

pub trait ModelRecon {
  fn reconstruct(&self, buffer: &[u8]) -> GResult<Box<dyn Model>>;
}


/* Model Builders */

pub trait ModelBuilder {
  fn consume(&mut self, kp: &KeyPosition) -> GResult<MaybeKeyBuffer>;
  fn finalize(self: Box<Self>) -> GResult<(MaybeKeyBuffer, Box<dyn ModelRecon>)>;
}

pub mod linear;