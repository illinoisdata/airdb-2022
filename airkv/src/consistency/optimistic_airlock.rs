use std::fmt::{Display, Formatter};

use crate::{common::{bytebuffer::ByteBuffer, error::GResult, serde::Serde, readbuffer::ReadBuffer}, db::rw_db::ClientID};

use super::airlock::{CommitContent, ResourceID};

pub struct OptimisticCommitInfo {
    lock_id: OptimisticAirLockID,
    content: CommitContent,
}

#[derive(Clone)]
pub struct OptimisticAirLockID {
    critical_res_ids: Vec<ResourceID>,
    client_id: ClientID,
}

impl OptimisticAirLockID {
    pub fn new(client_id_new: ClientID, critical_res_ids_new: Vec<ResourceID>) -> Self {
        Self {
            critical_res_ids: critical_res_ids_new,
            client_id: client_id_new,
        }
    }

    pub fn get_client_id(&self) -> ClientID {
        self.client_id
    }

    pub fn get_res_ids(&self) -> &Vec<ResourceID> {
        &self.critical_res_ids
    }
}

// impl PartialEq for AirLockID {
//     fn eq(&self, other: &Self) -> bool {
//         self.client_id == other.client_id
//             && self.start_timestamp.timestamp_millis() == other.start_timestamp.timestamp_millis()
//     }
// }

// impl Eq for AirLockID {}

impl Display for OptimisticAirLockID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AirLockID(clientID: {}, critical res: {:?})",
            self.client_id, self.critical_res_ids
        )
    }
}

impl Serde<OptimisticAirLockID> for OptimisticAirLockID {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        buff.write_u16(self.client_id);
        let res_number = self.critical_res_ids.len();
        buff.write_u8(res_number as u8);
        for i in 0..res_number {
            //TODO: optimize to write down 32 bits only
            buff.write_u64(self.critical_res_ids[i]);
        }
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> OptimisticAirLockID {
        let client_id = buff.read_u16();
        let res_number = buff.read_u8() as usize;
        let mut res_ids_read = Vec::with_capacity(res_number);
        for _i in 0..res_number {
            res_ids_read.push(buff.read_u64());
        }
        OptimisticAirLockID::new(client_id, res_ids_read)
    }
}

impl OptimisticCommitInfo {
    pub fn new(lock_id_new: OptimisticAirLockID, commit_content_new: CommitContent) -> Self {
        Self {
            lock_id: lock_id_new,
            content: commit_content_new,
        }
    }

    pub fn get_content(&self) -> &CommitContent {
        &self.content
    }

    pub fn get_lock_id(&self) -> &OptimisticAirLockID {
        &self.lock_id
    }

    pub fn get_client(&self) -> ClientID {
        self.lock_id.get_client_id()
    }
}

impl Serde<OptimisticCommitInfo> for OptimisticCommitInfo {
    fn serialize(&self, buff: &mut ByteBuffer) -> GResult<()> {
        self.lock_id.serialize(buff)?;
        self.content.serialize(buff)?;
        Ok(())
    }

    fn deserialize(buff: &mut ByteBuffer) -> OptimisticCommitInfo {
        let lock_id = OptimisticAirLockID::deserialize(buff);
        let content = CommitContent::deserialize(buff);
        OptimisticCommitInfo::new(lock_id, content)
    }
}
