
use byteorder::{BigEndian, ByteOrder};

use super::{dataslice::DataSlice, readbuffer::ReadBuffer};

pub struct ReversedByteBuffer {
    data: DataSlice,
    rpos: usize,
}

impl ReversedByteBuffer {
    // Construct a new ByteBuffer by wrapping an data array
    // It is supposed to be used in the read-only scenario.
    // The backend data array will be read from back to front
    //
    pub fn wrap(data_new: DataSlice) -> ReversedByteBuffer {
        let len = data_new.len();
        ReversedByteBuffer {
            data: data_new,
            rpos: len,
        }
    }
}

impl ReadBuffer for ReversedByteBuffer {
    fn has_remaining(&self) -> bool {
        self.rpos > 0
    }

    /// Read a defined amount of raw bytes. The program crash if not enough bytes are available
    fn read_bytes(&mut self, size: usize) -> Vec<u8> {
        assert!(self.rpos >= size);
        self.rpos -= size;
        self.data.copy_range(self.rpos..self.rpos + size)
    }

    // Read one byte. The program crash if not enough bytes are available
    fn read_u8(&mut self) -> u8 {
        assert!(self.rpos >= 1);
        self.rpos -= 1;
        self.data.get(self.rpos)
    }

    // Read a 2-bytes long value. The program crash if not enough bytes are available
    fn read_u16(&mut self) -> u16 {
        assert!(self.rpos >= 2);
        self.rpos -= 2;
        let range = self.rpos..self.rpos + 2;
        self.data.get_data(range, |x| BigEndian::read_u16(x))
    }

    // Read a four-bytes long value. The program crash if not enough bytes are available
    fn read_u32(&mut self) -> u32 {
        assert!(self.rpos >= 4);
        self.rpos -= 4;
        let range = self.rpos..self.rpos + 4;
        self.data.get_data(range, |x| BigEndian::read_u32(x))
    }

    // Read an eight bytes long value. The program crash if not enough bytes are available
    fn read_u64(&mut self) -> u64 {
        assert!(self.rpos >= 8);
        self.rpos -= 8;
        let range = self.rpos..self.rpos + 8;
        self.data.get_data(range, |x| BigEndian::read_u64(x))
    }

    fn read_u128(&mut self) -> u128 {
        assert!(self.rpos >= 16);
        self.rpos -= 16;
        let range = self.rpos..self.rpos + 16;
        self.data.get_data(range, |x| BigEndian::read_u128(x))
    }

    fn read_i64(&mut self) -> i64 {
        self.read_u64() as i64
    }
}


#[cfg(test)]
mod tests {
    use crate::common::error::GResult;

    #[test]
    fn reverse_bytebuffer_test() -> GResult<()> {
        // 


        Ok(())
    }
}