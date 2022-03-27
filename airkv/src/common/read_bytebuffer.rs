use byteorder::{BigEndian, ByteOrder};

use super::{readbuffer::ReadBuffer, dataslice::DataSlice};

pub struct ReadByteBuffer {
    data: DataSlice,
    rpos: usize,
}

impl ReadByteBuffer {
    /// Construct a new ByteBuffer by wrapping an data array
    /// It is supposed to be used in the read-only scenario.
    ///
    pub fn wrap(data_new: DataSlice) -> Self {
        Self {
            data: data_new,
            rpos: 0,
        }
    }

}

impl ReadBuffer for ReadByteBuffer {
    /// whether has remaining bytes to read
    fn has_remaining(&self) -> bool {
        self.rpos < self.data.len()
    }

    /// Read a defined amount of raw bytes. The program crash if not enough bytes are available
    fn read_bytes(&mut self, size: usize) -> Vec<u8> {
        assert!(self.rpos + size <= self.data.len());
        let range = self.rpos..self.rpos + size;
        let res = self.data.copy_range(range);
        self.rpos += size;
        res
    }

    /// Read one byte. The program crash if not enough bytes are available
    fn read_u8(&mut self) -> u8 {
        assert!(self.rpos < self.data.len());
        let pos = self.rpos;
        self.rpos += 1;
        self.data.get(pos)
    }

    /// Read a 2-bytes long value. The program crash if not enough bytes are available
    ///
    fn read_u16(&mut self) -> u16 {
        assert!(self.rpos + 2 <= self.data.len());
        let range = self.rpos..self.rpos + 2;
        self.rpos += 2;
        self.data.get_data(range, |x| BigEndian::read_u16(x))
    }

    /// Read a four-bytes long value. The program crash if not enough bytes are available
    fn read_u32(&mut self) -> u32 {
        assert!(self.rpos + 4 <= self.data.len());
        let range = self.rpos..self.rpos + 4;
        self.rpos += 4;
        self.data.get_data(range, |x| BigEndian::read_u32(x))
    }

    /// Read an eight bytes long value. The program crash if not enough bytes are available
    fn read_u64(&mut self) -> u64 {
        assert!(self.rpos + 8 <= self.data.len());
        let range = self.rpos..self.rpos + 8;
        self.rpos += 8;
        self.data.get_data(range, |x| BigEndian::read_u64(x))
    }

    /// Same as `read_u64()` but for signed values
    fn read_i64(&mut self) -> i64 {
        self.read_u64() as i64
    }

    fn read_u128(&mut self) -> u128 {
        assert!(self.rpos + 16 <= self.data.len());
        let range = self.rpos..self.rpos + 16;
        self.rpos += 16;
        self.data.get_data(range, |x| BigEndian::read_u128(x))
    }
}
