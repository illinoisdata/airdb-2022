use std::{collections::HashMap, thread};

use url::Url;
use uuid::Uuid;

use crate::{
    common::error::{AppendError, GResult},
    consistency::{
        airlock::{ClientID, CriticalOperation, TailUpdateCO, TIMEOUT},
        airlock_manager::AirLockManager,
        snapshot::Snapshot,
    },
    io::{
        fake_store_service_conn::FakeStoreServiceConnector,
        local_storage_conn::LocalStorageConnector,
        storage_connector::{StorageConnector, StorageType},
    },
    storage::{
        data_entry::{AppendRes, EntryAccess},
        meta::Meta,
        seg_util::SegIDUtil,
        segment::{Entry, SegID, Segment, SegmentProps, SEG_BLOCK_NUM_LIMIT},
        segment_manager::SegmentManager,
    },
};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub struct DBFactory {}

impl DBFactory {
    pub fn new_rwdb(home_dir_new: Url, store_type: StorageType) -> Box<dyn RWDB> {
        match store_type {
            StorageType::LocalFakeStore => {
                Box::new(RWDBImpl::<LocalStorageConnector>::new_from_connector(
                    home_dir_new,
                    LocalStorageConnector::default(),
                ))
            }
            StorageType::RemoteFakeStore => {
                Box::new(RWDBImpl::<FakeStoreServiceConnector>::new_from_connector(
                    home_dir_new,
                    FakeStoreServiceConnector::default(),
                ))
            }
            StorageType::AzureStore => {
                //TODO
                Box::new(RWDBImpl::<LocalStorageConnector>::new_from_connector(
                    home_dir_new,
                    LocalStorageConnector::default(),
                ))
            }
        }
    }

    pub fn gen_client_id() -> ClientID {
        Uuid::new_v4()
    }
}

pub trait RWDB {
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()>;

    fn put(&mut self, key: Key, value: Value) -> GResult<()>;

    fn put_entries(&mut self, entries: Vec<Entry>) -> GResult<()>;

    fn get(&mut self, key: Key) -> GResult<Option<Entry>>;

    fn get_from_snapshot(&mut self, snapshot: Snapshot, key: Key) -> GResult<Option<Entry>>;

    fn delete(&mut self, key: Key) -> GResult<()>;

    fn delete_entries(&mut self, entries: Vec<Key>) -> GResult<()>;

    fn get_client_id(&self) -> ClientID;

    fn close(&mut self) -> GResult<()>;
}

pub struct RWDBImpl<T: StorageConnector> {
    store_connector: T,
    seg_manager: SegmentManager,
    client_id: ClientID,
    // options:
}

impl<T: StorageConnector> RWDBImpl<T> {
    fn new_from_connector(home_dir_new: Url, connector_new: T) -> RWDBImpl<T> {
        let client_id_new = DBFactory::gen_client_id();
        Self {
            store_connector: connector_new,
            seg_manager: SegmentManager::new(client_id_new, home_dir_new),
            client_id: client_id_new,
        }
    }
}

impl<T: StorageConnector> RWDB for RWDBImpl<T> {
    // impl<'b> RWDB for RWDBImpl<'b> {
    // fn open(&'b mut self, props: &HashMap<String, String>) -> GResult<()> {
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()> {
        self.store_connector.open(props)?;
        // TODO: find a way to create the meta segment and the first tail segment
        // for now, just assume we have already created the meta and tail segment before launching the client
        // refresh meta
        self.seg_manager.refresh_meta(&self.store_connector)?;
        //TODO: finish other initial work
        Ok(())
    }

    // fn put(&'b mut self, key: Key, value: Value) -> GResult<()> {
    fn put(&mut self, key: Key, value: Value) -> GResult<()> {
        let entries = vec![Entry::new(key, value)];
        self.put_entries(entries)
    }

    fn put_entries(&mut self, entries: Vec<Entry>) -> GResult<()> {
        let tail_seg = self.seg_manager.get_cached_tail_seg();
        let tail_id = tail_seg.get_segid();
        let entries_slice: &[Entry] = &entries;
        match tail_seg.append_entries(&self.store_connector, entries_slice.iter()) {
            // match self.seg_manager.append_to_tail(tail_seg, entries) {
            AppendRes::Success(size) => {
                if size < SEG_BLOCK_NUM_LIMIT {
                    Ok(())
                } else {
                    // seal the old tail
                    tail_seg.seal(&self.store_connector)?;
                    // try to update tail
                    self.create_or_get_updated_tail(tail_id)
                }
            }
            AppendRes::BlockCountExceedFailure => {
                // seal the old tail
                tail_seg.seal(&self.store_connector)?;
                //try to update tail
                self.create_or_get_updated_tail(tail_id)?;
                // append to the new tail
                // TODO: check how to converge
                self.put_entries(entries)
            }
            AppendRes::AppendToSealedFailure => {
                let updated_tail = self
                    .seg_manager
                    .get_mut_meta_seg()
                    .get_refreshed_tail(&self.store_connector)?;
                if updated_tail != tail_id {
                    // the tail has been updated, append to the new tail
                    self.put_entries(entries)
                } else {
                    // rarely go to this branch(it happens only when a client sealed the old tail but failed to update a new tail)
                    // the tail remains the same, try to update tail
                    self.create_or_get_updated_tail(tail_id)?;
                    // append to the new tail
                    self.put_entries(entries)
                }
            }
            other => Err(Box::new(AppendError::new(other.to_string()))),
        }
    }

    fn get(&mut self, key: Key) -> GResult<Option<Entry>> {
        let mut tail_props = self.try_get_tail_props_from_cache()?;
        while !tail_props.is_active_tail() {
            tail_props = self.try_get_tail_props_updated()?;
        }
        self.get_from_snapshot(self.gen_snapshot_from_cache(tail_props), key)
    }

    fn get_from_snapshot(&mut self, snapshot: Snapshot, key: Key) -> GResult<Option<Entry>> {
        snapshot.get_entry(&self.store_connector, &mut self.seg_manager, &key)
    }

    fn close(&mut self) -> GResult<()> {
        //TODO: finish other release work
        self.store_connector.close()?;
        Ok(())
    }

    fn get_client_id(&self) -> ClientID {
        self.client_id
    }

    #[allow(unused)]
    fn delete(&mut self, key: Key) -> GResult<()> {
        todo!()
    }

    #[allow(unused)]
    fn delete_entries(&mut self, entries: Vec<Key>) -> GResult<()> {
        todo!()
    }
}

impl<T: StorageConnector> RWDBImpl<T> {
    fn try_get_tail_props_from_cache(&mut self) -> GResult<SegmentProps> {
        self.seg_manager
            .get_cached_tail_seg()
            .get_props(&self.store_connector)
    }

    fn try_get_tail_props_updated(&mut self) -> GResult<SegmentProps> {
        let conn = &self.store_connector;
        self.seg_manager.get_updated_tail_seg(conn).get_props(conn)
    }

    fn gen_snapshot_from_cache(&self, tail_props: SegmentProps) -> Snapshot {
        Snapshot::new(
            tail_props.get_seg_len(),
            self.seg_manager.get_meta_seg().get_tree_desc_from_cache(),
        )
    }

    fn create_or_get_updated_tail(&mut self, old_tail_id: SegID) -> GResult<()> {
        let tail_update_co =
            TailUpdateCO::new(vec![SegIDUtil::gen_next_tail(old_tail_id)], self.client_id);

        while tail_update_co.check_uninit(&self.store_connector, &mut self.seg_manager)
            && AirLockManager::run_with_single_lock(
                &self.store_connector,
                &mut self.seg_manager,
                &tail_update_co,
            )
        {
            thread::sleep(TIMEOUT);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;
    use url::Url;

    use crate::{
        common::error::GResult,
        io::{file_utils::UrlUtil, storage_connector::StorageType},
    };

    use super::DBFactory;

    #[test]
    fn db_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        //TODO: gen true home url
        let home_url: Url =
            UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        // create meta segment and the first tail segment

        let fake_props: &HashMap<String, String> = &HashMap::new();
        let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
        db.open(fake_props)?;
        db.close()?;
        Ok(())
    }
}
