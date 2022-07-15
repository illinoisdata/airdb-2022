use crate::{
    common::{error::GResult, serde::Serde, bytebuffer::ByteBuffer},
    db::rw_db::{Key, Value},
    storage::segment::Entry,
};
pub trait Transaction {
    fn put(&mut self, key: Key, value: Value) -> GResult<()>;

    fn get(&mut self, key: &Key) -> GResult<Option<Entry>>;

    fn delete(&mut self, key: Key) -> GResult<()>;

    fn commit(&mut self) -> GResult<()>;

    fn serialize(&self, buff:&mut ByteBuffer) -> GResult<()>;  

    // fn deserialize(buff:&mut ByteBuffer) -> T;
}
