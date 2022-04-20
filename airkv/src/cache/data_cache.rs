use std::{
    cell::{Ref, RefCell, RefMut},
    cmp,
    ops::Range,
    rc::Rc,
};

use crate::common::{dataslice::{DataSlice, SharedCacheData}, error::GResult};

/// DataRange describe a part of a data segment
/// range => the min/max position of the part in the segment
/// data =>  the data for a part of the segment 
///          use SharedCacheData(Rc<RefCell<Vec<u8>>>) to denote the data/cache in order to avoid memory copy
/// 
pub struct DataRange {
    range: Range<u64>,
    data: SharedCacheData, 
}

impl DataRange {
    pub fn new(range_new: Range<u64>, data_new: Vec<u8>) -> Self {
        Self {
            range: range_new,
            data: Rc::new(RefCell::new(data_new)),
        }
    }

    pub fn get_range(&self) -> &Range<u64> {
        &self.range
    }

    pub fn get_mut_data(&mut self) -> RefMut<Vec<u8>> {
        self.data.borrow_mut()
    }

    pub fn get_data(&self) -> Ref<Vec<u8>> {
        self.data.borrow()
    }

    pub fn get_data_rc(&self) -> SharedCacheData {
        Rc::clone(&self.data)
    }

    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// whether the other range is a consecutive range
    pub fn is_consecutive_range(&self, other: &DataRange) -> bool {
        self.range.end == other.get_range().start
    }

    pub fn contains_range(&self, range: &Range<u64>) -> bool {
        self.range.start <= range.start && self.range.end >= range.end
    }

    /// return the difference set for other_range - self.range (assume other_range is larger than self.range)
    pub fn range_difference(&self, other_range: &Range<u64>) -> Range<u64> {
        // TODO: remove this check
        assert!(other_range.end > self.range.end && other_range.start >= self.range.start);
        self.range.end..other_range.end
    }

    // pub fn is_overlapping(&self, other: &DataRange) -> bool {
    //     let ends_min = cmp::min(self.get_range().end, other.get_range().end);
    //     let starts_max = cmp::max(self.get_range().start, other.get_range().start);
    //     ends_min < starts_max
    // }

    pub fn is_overlapping_range(&self, other: &Range<u64>) -> bool {
        let ends_min = cmp::min(self.get_range().end, other.end);
        let starts_max = cmp::max(self.get_range().start, other.start);
        ends_min > starts_max
    }

    pub fn append(&mut self, other: &mut DataRange) -> GResult<()> {
        assert!(self.is_consecutive_range(other));
        self.data.borrow_mut().append(&mut other.get_mut_data());
        self.range.end = other.get_range().end;
        Ok(())
    }
}

impl Default for DataRange {
    fn default() -> Self {
        Self {
            range: 0..0,
            data: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

pub enum CacheHitStatus {
    Hit { data: DataSlice },
    HitPartial { miss_range: Range<u64> },
    Miss { miss_range: Range<u64> },
}

/// binary data cache for each data segment
#[derive(Default)]
pub struct DataCache {
    is_full: bool,
    // DataCache has to start from address 0
    cached_data: DataRange,
}

impl DataCache {
    pub fn new(is_full_new: bool, range_new: Range<u64>, data_new: Vec<u8>) -> Self {
        //TODO: remove this check
        // DataCache has to start from address 0
        assert!(range_new.start == 0);
        Self {
            is_full: is_full_new,
            cached_data: DataRange::new(range_new, data_new),
        }
    }

    pub fn is_full(&self) -> bool {
        self.is_full
    }

    pub fn is_empty(&self) -> bool {
        self.cached_data.is_empty()
    }

    pub fn get_full(&self) -> GResult<CacheHitStatus> {
        if self.is_full {
            Ok(CacheHitStatus::Hit {data: DataSlice::wrap(self.cached_data.get_data_rc())})
        } else if self.is_empty() {
             // 0 for miss_range.end stands for reading to the end of the segment
             Ok(CacheHitStatus::Miss {miss_range: 0..0})
        } else {
             // 0 for miss_range.end stands for reading to the end of the segment
             Ok(CacheHitStatus::HitPartial {miss_range: self.cached_data.get_range().end..0})
        }
    }

    pub fn get(&self, target_range: &Range<u64>) -> GResult<CacheHitStatus> {
        if self.cached_data.is_empty() {
            Ok(CacheHitStatus::Miss {
                miss_range: target_range.clone(),
            })
        } else if self.cached_data.contains_range(target_range) {
            Ok(CacheHitStatus::Hit {
                data: DataSlice::new(self.cached_data.get_data_rc(), target_range.start as usize..target_range.end as usize),
            })
        } else if self.cached_data.is_overlapping_range(target_range) {
            let diff = self.cached_data.range_difference(target_range);
            Ok(CacheHitStatus::HitPartial { miss_range: diff })
        } else {
            Ok(CacheHitStatus::Miss {
                miss_range: target_range.clone(),
            })
        }
    }

    pub fn update(&mut self, is_full: bool, new_range: &mut DataRange) -> GResult<()> {
        self.cached_data.append(new_range)?;
        self.is_full = is_full;
        Ok(())
    }
}

