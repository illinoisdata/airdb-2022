
use super::{error::GResult, bytebuffer::ByteBuffer};

pub trait Serde<T> {
    fn serialize(&self, buff:&mut ByteBuffer) -> GResult<()>;  

    fn deserialize(buff:&mut ByteBuffer) -> T;
} 
