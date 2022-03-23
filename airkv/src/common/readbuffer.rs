pub trait ReadBuffer {

    // fn wrap(data_new: Vec<u8>) -> Self;

    fn has_remaining(&self) -> bool;

    fn read_bytes(&mut self, size: usize) -> Vec<u8>;

    fn read_u8(&mut self) -> u8;

    fn read_u16(&mut self) -> u16;

    fn read_u32(&mut self) -> u32; 

    fn read_u64(&mut self) -> u64;

    fn read_i64(&mut self) -> i64;

    fn read_u128(&mut self) -> u128; 

}
