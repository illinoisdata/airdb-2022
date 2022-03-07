use std::fmt;

use crate::store::key_position::KEY_LENGTH;
use crate::store::key_position::KeyT;


/* key-value struct */

// const KEY_LENGTH: usize = std::mem::size_of::<KeyT>();
pub struct KeyBuffer {
  pub key: KeyT,  // TODO: generic Num + PartialOrd type
  pub buffer: Vec<u8>,  // TODO: copy-on-write?
}

impl fmt::Debug for KeyBuffer {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("KeyBuffer")
      .field("key", &self.key)
      .field("buffer_bytes", &self.buffer.len())
      .finish()
  }
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
    KeyBuffer {
      key: KeyT::from_be_bytes(serialized_buffer[..KEY_LENGTH].try_into().unwrap()),
      buffer: serialized_buffer[KEY_LENGTH..].to_vec(),
    }
  }

  pub fn deserialize_key(serialized_buffer: &[u8]) -> KeyT {
    KeyT::from_be_bytes(serialized_buffer[..KEY_LENGTH].try_into().unwrap())
  }

  pub fn serialized_size(&self) -> usize {
    KEY_LENGTH + self.buffer.len()
  }
}
