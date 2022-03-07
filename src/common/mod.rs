// use bytes::Bytes;
// pub type ArcBytes = Bytes;

use std::sync::Arc;
pub type ArcBytes = Arc<Vec<u8>>;

pub mod error;
