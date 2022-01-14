use std::error::Error;
use std::rc::Rc;

use crate::io::storage::ExternalStorage;

pub struct Context {
  pub storage: Rc<ExternalStorage>,
}

/* Serializable to Metadata */

pub trait Metaserde {
  // TODO: trait unstable
  fn to_meta(&self, ctx: &mut Context) -> Result<Vec<u8>, Box<dyn Error>>;
  fn from_meta(ctx: &Context, meta: &[u8]) -> Result<Self, Box<dyn Error>> where Self: Sized;
}