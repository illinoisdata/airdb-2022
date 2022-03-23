use std::{collections::HashMap, ops::Range};

use crate::common::error::GResult;

use super::airlock::{AirLockRequest, LockHolder, ResourceID, Timestamp};

#[allow(dead_code)]
pub struct LockHolderDesc {
    holder: LockHolder,
    window: Range<Timestamp>,
}

#[allow(dead_code)]
#[derive(Default)]
pub struct AirLockTracker {
    res_lock_map: HashMap<ResourceID, LockHolderDesc>,
}

#[allow(unused_variables)]
impl AirLockTracker {
    pub fn append_lock_reqs(&self, reqs: &[AirLockRequest]) -> GResult<()> {
        //TODO(L0): implement it
        Ok(())
    }
}
