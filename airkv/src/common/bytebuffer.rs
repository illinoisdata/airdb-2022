use byteorder::{BigEndian, ByteOrder};
use std::{fmt::Display, io::Write};

use super::readbuffer::ReadBuffer;

//ByteBuffer is designed to read/write binary data easily and efficiently.
pub struct ByteBuffer {
    data: Vec<u8>,
    wpos: usize,
    rpos: usize,
}

impl Display for ByteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Dump the byte buffer to a string.
        let mut str = String::new();
        for b in &self.data {
            str = str + &format!("0x{:01$x} ", b, 2);
        }
        str.pop();
        write!(f, "{}", str)
    }
}

impl ByteBuffer {
    pub fn new() -> ByteBuffer {
        ByteBuffer {
            data: vec![],
            wpos: 0,
            rpos: 0,
        }
    }

    /// Construct a new ByteBuffer by wrapping an data array
    /// It is supposed to be used in the read-only scenario.
    ///
    pub fn wrap(data_new: Vec<u8>) -> ByteBuffer {
        ByteBuffer {
            data: data_new,
            wpos: 0,
            rpos: 0,
        }
    }

    /// Construct a new ByteBuffer filled with the data array.
    pub fn from_bytes(bytes: &[u8]) -> ByteBuffer {
        let mut buffer = ByteBuffer::new();
        buffer.write_bytes(bytes);
        buffer
    }

    /// Return the buffer size
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the buffer and reinitialize the reading and writing cursor
    pub fn clear(&mut self) {
        self.data.clear();
        self.wpos = 0;
        self.rpos = 0;
    }

    /// Change the buffer size to size.
    ///
    /// _Note_: You cannot shrink a buffer with this method
    pub fn resize(&mut self, size: usize) {
        let diff = size - self.data.len();
        if diff > 0 {
            self.data.extend(std::iter::repeat(0).take(diff))
        }
    }

    // Write operations

    /// Append a byte array to the buffer. The buffer is automatically extended if needed
    pub fn write_bytes(&mut self, bytes: &[u8]) {

        let size = bytes.len() + self.wpos;

        if size > self.data.len() {
            self.resize(size);
        }

        for v in bytes {
            self.data[self.wpos] = *v;
            self.wpos += 1;
        }
    }

    /// Append a byte (8 bits value) to the buffer
    pub fn write_u8(&mut self, val: u8) {
        self.write_bytes(&[val]);
    }

    /// Same as `write_u8()` but for signed values
    pub fn write_i8(&mut self, val: i8) {
        self.write_u8(val as u8);
    }

    /// Append a word (16 bits value) to the buffer
    pub fn write_u16(&mut self, val: u16) {
        let mut buf = [0; 2];
        BigEndian::write_u16(&mut buf, val);
        self.write_bytes(&buf);
    }

    /// Same as `write_u16()` but for signed values
    pub fn write_i16(&mut self, val: i16) {
        self.write_u16(val as u16);
    }

    /// Append a double word (32 bits value) to the buffer
    pub fn write_u32(&mut self, val: u32) {
        let mut buf = [0; 4];
        BigEndian::write_u32(&mut buf, val);
        self.write_bytes(&buf);
    }

    /// Same as `write_u32()` but for signed values
    pub fn write_i32(&mut self, val: i32) {
        self.write_u32(val as u32);
    }

    /// Append a quaddruple word (64 bits value) to the buffer
    pub fn write_u64(&mut self, val: u64) {
        let mut buf = [0; 8];
        BigEndian::write_u64(&mut buf, val);
        self.write_bytes(&buf);
    }

    pub fn write_u128(&mut self, val: u128) {
        let mut buf = [0; 16];
        BigEndian::write_u128(&mut buf, val);
        self.write_bytes(&buf);
    }

    /// Same as `write_u64()` but for signed values
    pub fn write_i64(&mut self, val: i64) {
        self.write_u64(val as u64);
    }

    /// Return the position of the reading cursor
    pub fn get_rpos(&self) -> usize {
        self.rpos
    }

    /// Set the reading cursor position.
    /// *Note* : Set the reading cursor to `min(newPosition, self.len())` to prevent overflow
    pub fn set_rpos(&mut self, rpos: usize) {
        self.rpos = std::cmp::min(rpos, self.data.len());
    }

    /// Return the writing cursor position
    pub fn get_wpos(&self) -> usize {
        self.wpos
    }

    /// Set the writing cursor position.
    /// *Note* : Set the writing cursor to `min(newPosition, self.len())` to prevent overflow
    pub fn set_wpos(&mut self, wpos: usize) {
        self.wpos = std::cmp::min(wpos, self.data.len());
    }

    /// Return the raw byte buffer.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.to_vec()
    }

    pub fn to_view(&self) -> &[u8] {
        &self.data[self.rpos..self.data.len()]
    }

    pub fn write_bool(&mut self, value: bool) {
        self.write_u8(if value { 1u8 } else { 0u8 });
    }

    pub fn read_bool(&mut self) -> bool {
        let res = self.read_u8();
        res == 1u8
    }
   
}

impl ReadBuffer for ByteBuffer {
    /// whether has remaining bytes to read
    fn has_remaining(&self) -> bool {
        self.rpos < self.data.len()
    }

    /// Read a defined amount of raw bytes. The program crash if not enough bytes are available
    fn read_bytes(&mut self, size: usize) -> Vec<u8> {
        assert!(self.rpos + size <= self.data.len());
        let range = self.rpos..self.rpos + size;
        let mut res = Vec::<u8>::new();
        //TODO: check the correctness
        res.write_all(&self.data[range]).unwrap();
        self.rpos += size;
        res
    }

    /// Read one byte. The program crash if not enough bytes are available
    fn read_u8(&mut self) -> u8 {
        assert!(self.rpos < self.data.len());
        let pos = self.rpos;
        self.rpos += 1;
        self.data[pos]
    }

    /// Read a 2-bytes long value. The program crash if not enough bytes are available
    fn read_u16(&mut self) -> u16 {
        assert!(self.rpos + 2 <= self.data.len());
        let range = self.rpos..self.rpos + 2;
        self.rpos += 2;
        BigEndian::read_u16(&self.data[range])
    }

    /// Read a four-bytes long value. The program crash if not enough bytes are available
    fn read_u32(&mut self) -> u32 {
        assert!(self.rpos + 4 <= self.data.len());
        let range = self.rpos..self.rpos + 4;
        self.rpos += 4;
        BigEndian::read_u32(&self.data[range])
    }

    /// Read an eight bytes long value. The program crash if not enough bytes are available
    fn read_u64(&mut self) -> u64 {
        assert!(self.rpos + 8 <= self.data.len());
        let range = self.rpos..self.rpos + 8;
        self.rpos += 8;
        BigEndian::read_u64(&self.data[range])
    }

    /// Same as `read_u64()` but for signed values
    fn read_i64(&mut self) -> i64 {
        self.read_u64() as i64
    }

    fn read_u128(&mut self) -> u128 {
        assert!(self.rpos + 16 <= self.data.len());
        let range = self.rpos..self.rpos + 16;
        self.rpos += 16;
        BigEndian::read_u128(&self.data[range])
    }
}

impl Default for ByteBuffer {
    fn default() -> Self {
        Self::new()
    }
}
