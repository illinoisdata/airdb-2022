use std::{cell::RefCell, io::Write, ops::Range, rc::Rc};

pub type SharedCacheData = Rc<RefCell<Vec<u8>>>;

// DataSlice describe a slice backed by a shared data vector
// data => the shared data vector
// range => the range of the shared data vector occupied by the current slice 
pub struct DataSlice {
    data: SharedCacheData,
    range: Range<usize>,
}

impl DataSlice {
    pub fn new(data_new: SharedCacheData, range_new: Range<usize>) -> Self {
        // check the range lay within the min/max of data
        assert!(!range_new.is_empty());
        assert!(range_new.end <= data_new.borrow().len());
        Self {
            data: data_new,
            range: range_new,
        }
    }

    pub fn wrap(data_new: SharedCacheData) -> Self {
        let len = data_new.borrow().len();
        Self {
            data: data_new,
            range: 0..len,
        }
    }

    pub fn wrap_vec(data: Vec<u8>) -> Self {
        let data_new = Rc::new(RefCell::new(data));
        let len = data_new.borrow().len();
        Self {
            data: data_new,
            range: 0..len,
        }
    }


    pub fn get(&self, idx: usize) -> u8 {
        self.data.borrow()[idx + self.range.start]
    }

    pub fn get_data<T>(&self, index: Range<usize>, data_retrieval: impl Fn(&[u8]) -> T) -> T {
        data_retrieval(
            &self.data.borrow()[index.start + self.range.start..index.end + self.range.start],
        )
    }

    pub fn copy_range(&self, range_idx: Range<usize>) -> Vec<u8> {
        let mut res = Vec::<u8>::new();
        //TODO: check the correctness
        res.write_all(
            &self.data.borrow()
                [range_idx.start + self.range.start..range_idx.end + self.range.start],
        )
        .unwrap();
        res
    }

    pub fn len(&self) -> usize {
        if self.range.is_empty() {
            0
        } else {
            self.range.end - self.range.start
        }
    }

    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, cmp, rc::Rc};

    use rand::{Rng};

    use crate::common::error::GResult;

    use super::DataSlice;

    #[test]
    fn dataslice_all_test() -> GResult<()> {
        // test DataSlice::wrap()
        let mut test_data = [0u8; 256];
        rand::thread_rng().fill(&mut test_data[..]);
        let data_rc = Rc::new(RefCell::new(test_data.to_vec()));
        let data_slice_all = DataSlice::wrap(data_rc);
        assert_eq!(test_data.len(), data_slice_all.len());
        // test DataSlice.get()
        (0..test_data.len()).for_each(|i| {
            assert_eq!(test_data[i], data_slice_all.get(i));
        });

        // test DataSlice.get_data
        (0..10).for_each(|_x| {
            let bound_1 = rand::thread_rng().gen_range(0..test_data.len());
            let bound_2 = rand::thread_rng().gen_range(0..test_data.len());
            let random_range = cmp::min(bound_1, bound_2)..cmp::max(bound_1, bound_2);
            data_slice_all.get_data(random_range.clone(), |x| {
                assert_eq!(test_data[random_range.clone()], *x);
            });
        });

        // test DataSlice.copy_range
        (0..10).for_each(|_x| {
            let bound_1 = rand::thread_rng().gen_range(0..test_data.len());
            let bound_2 = rand::thread_rng().gen_range(0..test_data.len());
            let random_range = cmp::min(bound_1, bound_2)..cmp::max(bound_1, bound_2);
            assert_eq!(
                test_data[random_range.clone()],
                data_slice_all.copy_range(random_range)
            );
        });
        Ok(())
    }

    #[test]
    fn dataslice_range_test() -> GResult<()> {
        // test DataSlice::wrap()
        let mut origin_data = [0u8; 256];
        rand::thread_rng().fill(&mut origin_data[..]);
        let data_rc = Rc::new(RefCell::new(origin_data.to_vec()));

        // get a range slice of the original data arary 
        let slice_range = 101..220;
        let data_slice_range = DataSlice::new(data_rc,slice_range.clone());
        assert_eq!(slice_range.len(), data_slice_range.len());
        let test_data = &origin_data[slice_range];

        // test DataSlice.get()
        (0..test_data.len()).for_each(|i| {
            assert_eq!(test_data[i], data_slice_range.get(i));
        });

        // test DataSlice.get_data
        (0..10).for_each(|_x| {
            let bound_1 = rand::thread_rng().gen_range(0..test_data.len());
            let bound_2 = rand::thread_rng().gen_range(0..test_data.len());
            let random_range = cmp::min(bound_1, bound_2)..cmp::max(bound_1, bound_2);
            data_slice_range.get_data(random_range.clone(), |x| {
                assert_eq!(test_data[random_range.clone()], *x);
            });
        });

        // test DataSlice.copy_range
        (0..10).for_each(|_x| {
            let bound_1 = rand::thread_rng().gen_range(0..test_data.len());
            let bound_2 = rand::thread_rng().gen_range(0..test_data.len());
            let random_range = cmp::min(bound_1, bound_2)..cmp::max(bound_1, bound_2);
            assert_eq!(
                test_data[random_range.clone()],
                data_slice_range.copy_range(random_range)
            );
        });
        Ok(())
    }
}
