use std::collections::{HashMap, HashSet};

use crate::{
    common::{error::GResult, serde::Serde, bytebuffer::ByteBuffer},
    consistency::snapshot::Snapshot,
    db::rw_db::{Key, RWDBImpl, Value, RWDB},
    io::storage_connector::StorageConnector,
    storage::segment::Entry,
};

use super::transaction::Transaction;

pub struct OptimisticTransaction<'a, T: StorageConnector> {
    writeBuffer: HashMap<Key, Entry>,
    db: &'a mut RWDBImpl<T>,
    snapshot: Snapshot,
    readset: HashSet<Key>,
    writeset: HashSet<Key>,
    
}

impl<'a, T: StorageConnector> OptimisticTransaction<'a, T> {
    pub fn new(db_new: &'a mut RWDBImpl<T>, snapshot_new: Snapshot) -> Self {
        Self {
            writeBuffer: HashMap::with_capacity(3),
            db: db_new,
            snapshot: snapshot_new,
            readset: HashSet::with_capacity(3),
            writeset: HashSet::with_capacity(3),
        }
    }
}

impl<'a, T: StorageConnector> Transaction for OptimisticTransaction<'a, T> {
    fn put(&mut self, key: Key, value: Value) -> GResult<()> {
        self.writeset.insert(key.clone());
        self.writeBuffer.insert(key.clone(), Entry::new(key, value));
        Ok(())
    }

    fn get(&mut self, key: &Key) -> GResult<Option<Entry>> {
        self.readset.insert(key.clone());
        // first, try to get from writebuffer of the current transaction
        let res = self.writeBuffer.get(key);
        if let Some(entry) = res {
            Ok(Some(entry.clone()))
        } else {
            // if the key is not found in the writebuffer, try to search from db snapshot for the current transaction
            todo!();
            self.db.get_from_snapshot(&self.snapshot, key)
        }
    }

    fn delete(&mut self, key: Key) -> GResult<()> {
        self.writeset.insert(key.clone());
        todo!()
    }

    fn commit(&mut self) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        self.serialize(&mut buffer)?;
        self.db.put_bytes(buffer.to_view())?;
        //varify commit status
        todo!();
    }

    fn serialize(&self, buffer:&mut ByteBuffer) -> GResult<()> {
        let read_version = self.snapshot.get_version();
        buffer.write_u32(read_version);
        let mut txn_buff = ByteBuffer::new();
        self.readset.iter().for_each(|item| {
            txn_buff.write_bytes(item)
        });
        self.writeset.iter().for_each(|item| {
            txn_buff.write_bytes(item)
        });
        // self.writeBuffer.into_iter().for_each(|(k, entry)|{

        // });
        let txn_len = txn_buff.len();
        assert!(txn_len <= u16::MAX.into());
        buffer.write_u16(txn_len as u16);
        buffer.write_bytes(txn_buff.to_view());
        todo!()


    }
}
