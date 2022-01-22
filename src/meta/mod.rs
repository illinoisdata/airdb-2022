use serde::{Serialize, Deserialize};
use std::rc::Rc;

use crate::common::error::GResult;
use crate::io::storage::ExternalStorage;


pub struct Context {
  pub storage: Option<Rc<ExternalStorage>>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
  pub fn new() -> Context {
    Context {
      storage: None,
    }
  }

  pub fn put_storage(&mut self, storage: &Rc<ExternalStorage>) {
    if let Some(storage) = &self.storage {
      // if exists, check same object
      assert!(Rc::ptr_eq(storage, storage));
    } else {
      // if not, update
      self.storage = Some(Rc::clone(storage));
    }
  }
}


// default serializer, for convenience (JSON)
pub fn serialize<T: Serialize>(meta: &T) -> GResult<Vec<u8>> {
  Ok(serde_json::to_vec(meta)?)
}

pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> GResult<T> {
  Ok(serde_json::from_slice(bytes)?)
}


// // default serializer, for convenience (BSON)
// pub fn serialize<T: Serialize>(meta: &T) -> GResult<Vec<u8>> {
//   Ok(bson::to_vec(meta)?)
// }

// pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> GResult<T> {
//   Ok(bson::from_slice(bytes)?)
// }


/* Serializable to Metadata */

// TODO: make proper trait?
// pub trait Metaserde {
//   fn to_meta(&self: Self, ctx: &mut Context) -> GResult<Deserializable>;
//   fn from_meta(meta: &Deserializable, ctx: &Context) -> GResult<Self>;
// }