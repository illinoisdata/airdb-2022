use core::time;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::{
    common::{bytebuffer::ByteBuffer, error::GResult, readbuffer::ReadBuffer, serde::Serde},
    db::rw_db::ClientID,
    io::storage_connector::StorageConnector,
    lsmt::tree_delta::TreeDelta,
    storage::{meta::Meta, seg_util::SegIDUtil, segment::SegID, segment_manager::SegmentManager},
};

pub static LOCK_TIMEOUT_IN_SEC: u16 = 60;
pub static TIMEOUT: std::time::Duration = time::Duration::from_secs(LOCK_TIMEOUT_IN_SEC as u64);

pub static CLIENT_CLOCK_SKEW_IN_SEC: u8 = 10;

// TODO: find efficient type and serialization way for clientid/timestamp
// TODO: replace timestamp with a incremental number
pub type ResourceID = SegID;
pub type Timestamp = DateTime<Utc>;

#[derive(Clone)]
pub struct AirLockID {
    client_id: ClientID,
    start_timestamp: Timestamp,
}

impl AirLockID {
    pub fn new(client_id_new: ClientID, timestamp: Timestamp) -> Self {
        Self {
            client_id: client_id_new,
            start_timestamp: timestamp,
        }
    }

    pub fn get_client_id(&self) -> &ClientID {
        &self.client_id
    }
}

impl PartialEq for AirLockID {
    fn eq(&self, other: &Self) -> bool {
        self.client_id == other.client_id
            && self.start_timestamp.timestamp_millis() == other.start_timestamp.timestamp_millis()
    }
}

impl Eq for AirLockID {}

impl Display for AirLockID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AirLockID(clientID: {}, start_timestamp: {})",
            self.client_id, self.start_timestamp
        )
    }
}

impl Serde<AirLockID> for AirLockID {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        buff.write_u16(self.client_id);
        buff.write_i64(self.start_timestamp.timestamp_millis());
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> AirLockID {
        let client_id = buff.read_u16();
        let timestamp = Utc.timestamp_millis(buff.read_i64());
        AirLockID::new(client_id, timestamp)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::common::{bytebuffer::ByteBuffer, error::GResult, serde::Serde};

    use super::AirLockID;

    #[test]
    fn airlockid_serde_test() -> GResult<()> {
        let lock_id = AirLockID::new(3u16, Utc::now());
        let mut buffer = ByteBuffer::new();
        lock_id.serialize(&mut buffer)?;
        let deserialized_lock = AirLockID::deserialize(&mut buffer);
        assert!(lock_id == deserialized_lock);
        Ok(())
    }
}

pub struct AirLockRequest {
    /// assume the length of resource_ids vec is no larger than 256
    resource_ids: Vec<ResourceID>,
    client_id: ClientID,
    timestamp: Timestamp,
}

impl AirLockRequest {
    pub fn new_with_timestamp(
        resource_ids_new: Vec<ResourceID>,
        client_id_new: ClientID,
        timestamp_new: Timestamp,
    ) -> Self {
        //TODO: remove this check
        assert!(resource_ids_new.len() <= u8::MAX as usize);
        Self {
            resource_ids: resource_ids_new,
            client_id: client_id_new,
            timestamp: timestamp_new,
        }
    }

    pub fn new(resource_ids_new: Vec<ResourceID>, client_id_new: ClientID) -> Self {
        //TODO: remove this check
        assert!(resource_ids_new.len() <= u8::MAX as usize);
        Self {
            resource_ids: resource_ids_new,
            client_id: client_id_new,
            timestamp: Utc::now(),
        }
    }

    pub fn get_lock_id(&self) -> AirLockID {
        AirLockID::new(self.client_id, self.timestamp)
    }

    pub fn get_client(&self) -> &ClientID {
        &self.client_id
    }

    pub fn get_request_time(&self) -> &Timestamp {
        &self.timestamp
    }

    pub fn get_res_ids(&self) -> &Vec<ResourceID> {
        &self.resource_ids
    }
}

impl Serde<AirLockRequest> for AirLockRequest {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        let res_number = self.resource_ids.len();
        buff.write_u8(res_number as u8);
        for i in 0..res_number {
            buff.write_u32(SegIDUtil::get_non_optimistic_segid(self.resource_ids[i]));
        }
        buff.write_u16(self.client_id);
        buff.write_i64(self.timestamp.timestamp_millis());
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> AirLockRequest {
        let res_number = buff.read_u8() as usize;
        let mut res_ids_read = Vec::with_capacity(res_number);
        for _i in 0..res_number {
            res_ids_read.push(SegIDUtil::from_non_optimistic_segid(buff.read_u32()));
        }

        let client_id_read = buff.read_u16();
        let timestamp_read = Utc.timestamp_millis(buff.read_i64());
        AirLockRequest::new_with_timestamp(res_ids_read, client_id_read, timestamp_read)
    }
}

pub struct AirLockCheck {
    lock_id: AirLockID,
    check_time: Timestamp,
}

impl AirLockCheck {
    pub fn new(lock_id_new: AirLockID) -> Self {
        Self {
            lock_id: lock_id_new,
            check_time: Utc::now(),
        }
    }

    pub fn get_lock_id(&self) -> &AirLockID {
        &self.lock_id
    }

    pub fn get_client(&self) -> &ClientID {
        self.lock_id.get_client_id()
    }

    pub fn get_check_time(&self) -> &Timestamp {
        &self.check_time
    }
}

pub enum AirLockStatus<AirLockID> {
    Acquired(AirLockID),
    Renewed(AirLockID),
    Failed,
    InvalidCheck,
}

impl AirLockStatus<AirLockID> {
    pub fn is_success(&self) -> bool {
        matches!(self, AirLockStatus::Acquired(_) | AirLockStatus::Renewed(_))
    }
}

#[derive(Clone)]
pub struct LockHolder {
    lock_id: AirLockID,
    last_renew_timestamp: Timestamp,
    res_ids: Vec<SegID>,
    is_renewed: bool,
}

impl LockHolder {
    pub fn new(
        client_id_new: ClientID,
        timestamp_new: Timestamp,
        res_ids_new: Vec<SegID>,
        is_renewed_new: bool,
    ) -> Self {
        Self {
            lock_id: AirLockID::new(client_id_new, timestamp_new),
            last_renew_timestamp: timestamp_new,
            res_ids: res_ids_new,
            is_renewed: is_renewed_new,
        }
    }

    pub fn get_lock_id(&self) -> &AirLockID {
        &self.lock_id
    }

    pub fn get_res_ids(&self) -> &Vec<SegID> {
        &self.res_ids
    }

    pub fn get_client_id(&self) -> &ClientID {
        self.lock_id.get_client_id()
    }

    pub fn is_renewed_req(&self, lock_req: &AirLockRequest) -> bool {
        //TODO: remove assert and test is_renewed_req
        assert!(lock_req.get_request_time() > self.get_start_time());
        self.res_ids == *lock_req.get_res_ids()
            && lock_req.get_client().eq(&self.lock_id.client_id)
            && self.valid_access_time(lock_req.get_request_time())
    }

    pub fn renew_lease(&mut self, renew_timestamp: Timestamp) {
        self.last_renew_timestamp = renew_timestamp;
        self.is_renewed = true;
    }

    pub fn get_start_time(&self) -> &Timestamp {
        &self.lock_id.start_timestamp
    }

    pub fn get_expired_time(&self) -> Timestamp {
        self.last_renew_timestamp
            + Duration::seconds(LOCK_TIMEOUT_IN_SEC as i64)
            + Duration::seconds(CLIENT_CLOCK_SKEW_IN_SEC as i64)
    }

    fn valid_access_time(&self, access_time: &Timestamp) -> bool {
        access_time <= &self.get_expired_time()
    }

    // check whether the target lock request is working/holding the resource
    pub fn valid_check(&self, lock_check: &AirLockCheck) -> AirLockStatus<AirLockID> {
        if (&self.lock_id == lock_check.get_lock_id())
            && self.valid_access_time(lock_check.get_check_time())
        {
            if self.is_renewed {
                AirLockStatus::Renewed(self.lock_id.clone())
            } else {
                AirLockStatus::Acquired(self.lock_id.clone())
            }
        } else {
            AirLockStatus::Failed
        }
    }

    // check whether the lock in the lock request can replace the current lock holder
    pub fn valid_acquire(&self, lock_req: &AirLockRequest) -> AirLockStatus<AirLockID> {
        if !self.valid_access_time(lock_req.get_request_time()) {
            AirLockStatus::Acquired(AirLockID::new(
                *lock_req.get_client(),
                *lock_req.get_request_time(),
            ))
        } else if lock_req.get_client() == self.get_client_id() {
            AirLockStatus::Renewed(self.lock_id.clone())
        } else {
            AirLockStatus::Failed
        }
    }
}

pub trait CriticalOperation {
    fn check_uninit(&self, conn: &dyn StorageConnector, seg_manager: &mut SegmentManager) -> bool;

    fn run(
        &self,
        conn: &dyn StorageConnector,
        seg_manager: &mut SegmentManager,
        lock_id: AirLockID,
    ) -> GResult<()>;

    fn get_res(&self) -> Vec<ResourceID>;

    fn get_client(&self) -> ClientID;
}

// define the specific CriticalOperation for tail segment update
pub struct TailUpdateCO {
    res_ids: Vec<SegID>,
    client_id: ClientID,
    old_tail: SegID,
}

impl TailUpdateCO {
    pub fn new(res_ids_new: Vec<SegID>, client_id_new: ClientID, old_tail_new: SegID) -> TailUpdateCO {
        assert!(res_ids_new.len() == 1);
        Self {
            res_ids: res_ids_new,
            client_id: client_id_new,
            old_tail: old_tail_new,
        }
    }

    pub fn get_old_tail(&self) -> SegID {
        self.old_tail
    }
}

// implement CriticalOperation methods
impl CriticalOperation for TailUpdateCO {
    fn run(
        &self,
        conn: &dyn StorageConnector,
        seg_manager: &mut SegmentManager,
        lock_id: AirLockID,
    ) -> GResult<()> {
        let new_tail = self.res_ids[0];

        // create new tail
        seg_manager.create_new_tail_seg(conn, new_tail)?;
        // update tree desc: add old tail to level 0 and add a new tail
        let delta: TreeDelta = TreeDelta::update_tail_delta_from_segid(new_tail, self.get_old_tail());
        // self.seg_manager.append_tree_delta(&delta)
        seg_manager
            .get_mut_meta_seg()
            .append_commit_info(conn, CommitInfo::new(lock_id, delta))
    }

    fn get_res(&self) -> Vec<ResourceID> {
        self.res_ids.clone()
    }

    fn get_client(&self) -> ClientID {
        self.client_id
    }

    fn check_uninit(&self, conn: &dyn StorageConnector, seg_manager: &mut SegmentManager) -> bool {
        let new_tail = self.res_ids[0];
        // update meta
        match seg_manager.refresh_meta(conn) {
            Ok(_) => SegIDUtil::is_new_tail(new_tail, seg_manager.get_cached_tail_id()),
            Err(_) => {
                println!("WARN: refresh meta failed when check uninit");
                // just return true to let the client try to acquire the lock
                // TODO: find a better way to cope with this case(such as retry refreshing meta )
                true
            }
        }
    }
}

pub type CommitContent = TreeDelta;
pub struct CommitInfo {
    lock_id: AirLockID,
    content: CommitContent,
}

impl CommitInfo {
    pub fn new(lock_id_new: AirLockID, commit_content_new: CommitContent) -> Self {
        Self {
            lock_id: lock_id_new,
            content: commit_content_new,
        }
    }

    pub fn get_content(&self) -> &CommitContent {
        &self.content
    }

    pub fn get_lock_id(&self) -> &AirLockID {
        &self.lock_id
    }

    pub fn get_client(&self) -> &ClientID {
        self.lock_id.get_client_id()
    }
}

impl Serde<CommitInfo> for CommitInfo {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        self.lock_id.serialize(buff)?;
        self.content.serialize(buff)?;
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> CommitInfo {
        let lock_id = AirLockID::deserialize(buff);
        let content = CommitContent::deserialize(buff);
        CommitInfo::new(lock_id, content)
    }
}
