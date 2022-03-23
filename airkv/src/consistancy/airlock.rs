use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

use crate::{
    common::{bytebuffer::ByteBuffer, error::GResult, readbuffer::ReadBuffer, serde::Serde},
    storage::segment::SegID,
};

// TODO: find efficient type and serialization way for clientid/timestamp
// uuid is too long for a lock request
pub type ClientID = Uuid;
pub type ResourceID = SegID;
pub type Timestamp = DateTime<Utc>;
pub struct AirLockRequest {
    /// assume the length of resource_ids vec is no larger than 256
    resource_ids: Vec<ResourceID>,
    client_id: ClientID,
    timestamp: Timestamp,
}

impl AirLockRequest {
    pub fn new(
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
}

impl Serde<AirLockRequest> for AirLockRequest {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        let res_number = self.resource_ids.len();
        buff.write_u8(res_number as u8);
        for i in 0..res_number {
            buff.write_u32(self.resource_ids[i]);
        }
        buff.write_u128(self.client_id.as_u128());
        buff.write_i64(self.timestamp.timestamp_millis());
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> AirLockRequest {
        let res_number = buff.read_u8() as usize;
        let mut res_ids_read = Vec::with_capacity(res_number);
        for _i in 0..res_number {
            res_ids_read.push(buff.read_u32());
        }

        let client_id_read = Uuid::from_u128(buff.read_u128());
        let timestamp_read = Utc.timestamp_millis(buff.read_i64());
        AirLockRequest::new(res_ids_read, client_id_read, timestamp_read)
    }
}

// pub type
pub enum AirLockStatus<LockHolder> {
    Acquired,
    Renewed,
    Failed(LockHolder),
    InvalidCheck,
}

#[allow(dead_code)]
pub struct LockHolder {
    client_id: ClientID,
    hold_timestamp: Timestamp,
}

#[allow(dead_code)]

impl LockHolder {
    fn new(client_id_new: ClientID, timestamp_new: Timestamp) -> Self {
        Self {
            client_id: client_id_new,
            hold_timestamp: timestamp_new,
        }
    }
}
