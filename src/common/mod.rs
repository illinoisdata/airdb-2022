// use bytes::Bytes;
// pub type SharedBytes = Bytes;

use std::rc::Rc;
pub type SharedBytes = Rc<Vec<u8>>;

pub mod error;
