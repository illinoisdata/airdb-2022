use crate::store::key_position::KEY_LENGTH;
use crate::store::key_position::KeyT;


/* key-value struct */

// const KEY_LENGTH: usize = std::mem::size_of::<KeyT>();
#[derive(Debug)]
pub struct KeyBuffer {
  pub key: KeyT,  // TODO: generic Num + PartialOrd type
  pub buffer: Vec<u8>,  // TODO: copy-on-write?
}

impl KeyBuffer {  // maybe implement in Serializer, Deserializer instead?
  pub fn serialize(&self) -> Vec<u8> {
    // TODO: return reference by concat slices
    let mut serialized_buffer = vec![0u8; KEY_LENGTH + self.buffer.len()];
    serialized_buffer[..KEY_LENGTH].clone_from_slice(&self.key.to_be_bytes());
    serialized_buffer[KEY_LENGTH..].clone_from_slice(&self.buffer);
    serialized_buffer
  }

  pub fn deserialize(serialized_buffer: &[u8]) -> KeyBuffer {
    let mut key_bytes = [0u8; KEY_LENGTH];
    key_bytes[..KEY_LENGTH].clone_from_slice(&serialized_buffer[..KEY_LENGTH]);
    KeyBuffer {
      key: KeyT::from_be_bytes(key_bytes),
      buffer: serialized_buffer[KEY_LENGTH..].to_vec(),
    }
  }
}
