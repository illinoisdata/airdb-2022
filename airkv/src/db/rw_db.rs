use std::{collections::HashMap, fmt::Debug};

use url::Url;

use crate::{
    common::{
        bytebuffer::ByteBuffer,
        error::{AppendError, GResult},
        serde::Serde,
    },
    consistency::{
        airlock::{CriticalOperation, TailUpdateCO},
        airlock_manager::AirLockManager,
        optimistic_airlock::{OptimisticAirLockID, OptimisticCommitInfo},
        snapshot::Snapshot,
    },
    io::{
        azure_conn::AzureConnector,
        fake_store_service_conn::FakeStoreServiceConnector,
        local_storage_conn::LocalStorageConnector,
        storage_connector::{StorageConnector, StorageType},
    },
    lsmt::tree_delta::TreeDelta,
    storage::{
        data_entry::{AppendRes},
        meta::Meta,
        seg_util::SegIDUtil,
        segment::{Entry, SegID, Segment, SegmentProps, SEG_BLOCK_NUM_LIMIT},
        segment_manager::SegmentManager,
    },
    transaction::{optimistic_transaction::OptimisticTransaction, transaction::Transaction},
};

use super::compact_db::{CompactionDB, CompactionDBImpl};

// the first bit denotes rw_client or compact client: 0 is rw_client; 1 is compact_client
// the following bits denotes the real client id
pub type ClientID = u16;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub static FAKE_CLIENT_ID: ClientID = 0;

pub struct DBFactory {}

impl DBFactory {
    pub fn new_rwdb(home_dir_new: Url, store_type: StorageType) -> Box<dyn RWDB> {
        match store_type {
            StorageType::AzureStore => Box::new(RWDBImpl::<AzureConnector>::new_from_connector(
                home_dir_new,
                AzureConnector::default(),
            )),
            StorageType::RemoteFakeStore => {
                Box::new(RWDBImpl::<FakeStoreServiceConnector>::new_from_connector(
                    home_dir_new,
                    FakeStoreServiceConnector::default(),
                ))
            }
            StorageType::LocalFakeStore => {
                Box::new(RWDBImpl::<LocalStorageConnector>::new_from_connector(
                    home_dir_new,
                    LocalStorageConnector::default(),
                ))
            }
        }
    }

    pub fn new_rwdb_from_str(home_dir: String, store_type: String) -> Box<dyn RWDB> {
        let home_dir_new = Url::parse(&home_dir)
            .unwrap_or_else(|_| panic!("parse url error: the home dir is {}", home_dir));
        match store_type.as_str() {
            "AzureStore" => Box::new(RWDBImpl::<AzureConnector>::new_from_connector(
                home_dir_new,
                AzureConnector::default(),
            )),
            "RemoteFakeStore" => {
                Box::new(RWDBImpl::<FakeStoreServiceConnector>::new_from_connector(
                    home_dir_new,
                    FakeStoreServiceConnector::default(),
                ))
            }
            "LocalFakeStore" => Box::new(RWDBImpl::<LocalStorageConnector>::new_from_connector(
                home_dir_new,
                LocalStorageConnector::default(),
            )),
            store_type => {
                panic!("wrong storage type {}", store_type)
            }
        }
    }

    pub fn new_compactiondb(home_dir_new: Url, store_type: StorageType) -> Box<dyn CompactionDB> {
        match store_type {
            StorageType::AzureStore => {
                Box::new(CompactionDBImpl::<AzureConnector>::new_from_connector(
                    home_dir_new,
                    AzureConnector::default(),
                ))
            }
            StorageType::RemoteFakeStore => {
                todo!()
            }
            StorageType::LocalFakeStore => {
                todo!()
            }
        }
    }

    pub fn gen_client_id(home_dir: &Url, conn: &dyn StorageConnector, is_rw: bool) -> ClientID {
        // if is_rw {
        // read/write client
        let client_tracker_dir = home_dir
            .join("rw_client_tracker")
            .unwrap_or_else(|_| panic!("Cannot generate a path for rw_client_tracker"));

        // append a fake block and get the current block number of client_tracker_seg as the client id
        let client_id = match conn.append(&client_tracker_dir, &[0u8]) {
            AppendRes::Success(block_num) => block_num,
            res => {
                println!("ERROR: failed to append to client_tracker_seg");
                panic!("failed to get client id: {}", res)
            }
        };
        client_id
        // } else {
        //     // compact client
        //     // read/write client
        //     let client_tracker_dir = home_dir
        //         .join("compact_client_tracker")
        //         .unwrap_or_else(|_| panic!("Cannot generate a path for compact_client_tracker"));

        //     // append a fake block and get the current block number of client_tracker_seg as the client id
        //     let client_id = match conn.append(&client_tracker_dir, &[0u8]) {
        //         AppendRes::Success(block_num) => block_num,
        //         res => panic!("failed to get client id: {}", res),
        //     };
        //     assert!(client_id < (1u16 << 15));
        //     client_id | (1u16 << 15)
        // }
    }
}

pub trait RWDB {
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()>;

    fn begin_transaction<'a>(
        &'a mut self,
        txn_options: &HashMap<String, String>,
    ) -> Box<dyn Transaction + 'a>;

    fn put_txn(&mut self, txn: &dyn Transaction) -> GResult<()>;

    fn put(&mut self, key: Key, value: Value) -> GResult<()>;

    fn put_entries(&mut self, entries: Vec<Entry>) -> GResult<()>;

    fn put_bytes(&mut self, data: &[u8]) -> GResult<()>;

    fn get(&mut self, key: &Key) -> GResult<Option<Entry>>;

    fn get_from_snapshot(&mut self, snapshot: &Snapshot, key: &Key) -> GResult<Option<Entry>>;

    fn delete(&mut self, key: Key) -> GResult<()>;

    fn delete_entries(&mut self, entries: Vec<Key>) -> GResult<()>;

    fn get_client_id(&self) -> ClientID;

    fn get_props(&self) -> &DBProps;

    fn close(&mut self) -> GResult<()>;
}

#[derive(Debug)]
pub struct DBProps {
    seg_block_num_limit: u16,
}

impl Default for DBProps {
    fn default() -> Self {
        Self {
            seg_block_num_limit: 50000,
        }
    }
}

impl DBProps {
    pub fn set_seg_block_num_limit(&mut self, limit: u16) {
        self.seg_block_num_limit = limit
    }

    pub fn get_seg_block_num_limit(&self) -> u16 {
        self.seg_block_num_limit
    }
}

pub struct RWDBImpl<T: StorageConnector> {
    store_connector: T,
    seg_manager: SegmentManager,
    client_id: ClientID,
    props: DBProps,
}

impl<T: StorageConnector> Debug for RWDBImpl<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RWDBImpl")
            .field("client_id", &self.client_id)
            .field("props", &self.props)
            .finish()
    }
}

impl<T: StorageConnector> Drop for RWDBImpl<T> {
    fn drop(&mut self) {
        println!("destructor has been called");
    }
}

impl<T: StorageConnector> RWDBImpl<T> {
    pub fn new_from_connector(home_dir_new: Url, connector_new: T) -> RWDBImpl<T> {
        // get client id
        let client_id_new = DBFactory::gen_client_id(&home_dir_new, &connector_new, true);
        println!("INFO: create a new rw client with id {}", client_id_new);
        Self {
            store_connector: connector_new,
            seg_manager: SegmentManager::new(client_id_new, home_dir_new),
            client_id: client_id_new,
            props: DBProps::default(),
        }
    }
}

impl<T: StorageConnector> RWDB for RWDBImpl<T> {
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()> {
        match props.get("SEG_BLOCK_NUM_LIMIT") {
            Some(block_num) => {
                let block_limit = block_num.parse()?;
                self.props.set_seg_block_num_limit(block_limit);
                unsafe {
                    SEG_BLOCK_NUM_LIMIT = block_limit;
                }
            }
            None => {}
        }
        self.store_connector.open(props)?;
        // TODO: find a way to create the meta segment
        // for now, just assume we have already created the meta before launching the client
        // refresh meta
        self.seg_manager.refresh_meta(&self.store_connector)?;
        if !self.seg_manager.has_valid_tail() {
            // create the tail segment if necessary
            self.create_or_get_updated_tail(self.seg_manager.get_cached_tail_id())?;
        }
        //TODO: finish other initial work
        Ok(())
    }

    fn put(&mut self, key: Key, value: Value) -> GResult<()> {
        let entries = vec![Entry::new(key, value)];
        self.put_entries(entries)?;
        Ok(())
    }

    fn put_bytes(&mut self, data: &[u8]) -> GResult<()> {
        let tail_seg = self.seg_manager.get_cached_tail_seg();
        let tail_id = tail_seg.get_segid();
        match tail_seg.append_all(&self.store_connector, data) {
            AppendRes::Success(size) => {
                if size < self.props.get_seg_block_num_limit() {
                    Ok(())
                } else {
                    // seal the old tail
                    tail_seg.seal(&self.store_connector)?;
                    // try to update tail
                    self.create_or_get_updated_tail(tail_id)?;
                    Ok(())
                }
            }
            AppendRes::BlockCountExceedFailure => {
                // seal the old tail
                tail_seg.seal(&self.store_connector)?;
                //try to update tail
                self.create_or_get_updated_tail(tail_id)?;
                // append to the new tail
                // TODO: check how to converge
                self.put_bytes(data)?;
                Ok(())
            }
            AppendRes::AppendToSealedFailure => {
                let updated_tail = self
                    .seg_manager
                    .get_mut_meta_seg()
                    .get_refreshed_tail(&self.store_connector)?;
                if updated_tail != tail_id {
                    // the tail has been updated, append to the new tail
                    self.put_bytes(data)?;
                    Ok(())
                } else {
                    // rarely go to this branch(it happens only when a client sealed the old tail but failed to update a new tail)
                    // the tail remains the same, try to update tail
                    self.create_or_get_updated_tail(tail_id)?;
                    // append to the new tail
                    self.put_bytes(data)?;
                    Ok(())
                }
            }
            other => Err(Box::new(AppendError::new(other.to_string()))),
        }
    }

    fn put_txn(&mut self, txn: &dyn Transaction) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        txn.serialize(&mut buffer)?;
        self.put_bytes(buffer.to_view())
    }

    fn put_entries(&mut self, entries: Vec<Entry>) -> GResult<()> {
        let mut buffer = ByteBuffer::new();
        for entry in entries {
            // //in order to support backward read for tail/L0 segment
            // // write in this order: value -> value length -> key -> key length
            // let value = entry.get_value_slice();
            // let key = entry.get_key_slice();
            // buffer.write_bytes(value);
            // buffer.write_u16(value.len() as u16);
            // buffer.write_bytes(key);
            // buffer.write_u16(key.len() as u16);
            entry.serialize(&mut buffer)?;
        }
        // let start = Instant::now();
        self.put_bytes(buffer.to_view())
    }

    fn get(&mut self, key: &Key) -> GResult<Option<Entry>> {
        let snapshot = self.get_latest_valid_snapshot();
        self.get_from_snapshot(&snapshot, key)
    }

    fn get_from_snapshot(&mut self, snapshot: &Snapshot, key: &Key) -> GResult<Option<Entry>> {
        snapshot.get_entry(&self.store_connector, &mut self.seg_manager, key)
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

    fn get_props(&self) -> &DBProps {
        &self.props
    }

    fn begin_transaction<'a>(
        &'a mut self,
        txn_options: &HashMap<String, String>,
    ) -> Box<dyn Transaction + 'a> {
        match txn_options.get("TRANSACTION_STRATEGY") {
            Some(strategy) => {
                if strategy == "optimistic" {
                    let snapshot = self.get_latest_valid_snapshot();
                    Box::new(OptimisticTransaction::new(self, snapshot))
                } else {
                    panic!("ERROR: unsupported transaction strategy {}", strategy);
                }
            }
            None => {
                // in default cases, use optimistic transaction strategy
                let snapshot = self.get_latest_valid_snapshot();
                Box::new(OptimisticTransaction::new(self, snapshot))
            }
        }
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

    fn get_latest_valid_snapshot(&mut self) -> Snapshot {
        //TODO: find a better way to get the snapshot
        let mut tail_props = self
            .try_get_tail_props_from_cache()
            .expect("failed to get tail_props_from_cache");

        while !tail_props.is_active_tail() {
            tail_props = self
                .try_get_tail_props_updated()
                .expect("failed to get tail props from updated meta");
        }
        self.gen_snapshot_from_cache(tail_props)
    }

    fn gen_snapshot_from_cache(&self, tail_props: SegmentProps) -> Snapshot {
        let tree = self.seg_manager.get_meta_seg().get_tree_desc_from_cache();
        Snapshot::new(
            tail_props.get_seg_len(),
            tail_props.get_block_num(),
            self.props.get_seg_block_num_limit(),
            tree,
        )
    }

    // if there has been a new tail, just refresh the meta and get the new tail
    // otherwise create a new tail
    // return => whether the new tail has been created by the current client
    fn create_or_get_updated_tail(&mut self, old_tail_id: SegID) -> GResult<bool> {
        // TODO: use parameters to trigger optimistic strategy
        if true {
            // create a tail and append the new tail info to the meta segment
            let new_tail_leading = SegIDUtil::gen_next_tail(old_tail_id);
            let new_tail = new_tail_leading | (self.client_id as SegID);

            // create new tail
            // let start = Instant::now();
            self.seg_manager
                .create_new_tail_seg(&self.store_connector, new_tail)?;
            // unsafe {
            //     TAIL_CREATE_TIME += start.elapsed().as_millis();
            // }

            // update tree desc: add old tail to level 0 and add a new tail
            let delta: TreeDelta = TreeDelta::update_tail_delta_from_segid(new_tail, old_tail_id);
            let lock_id = OptimisticAirLockID::new(
                self.client_id,
                vec![SegIDUtil::get_resid_from_segid(new_tail)],
            );

            // let start_commit = Instant::now();
            self.seg_manager
                .get_mut_meta_seg()
                .append_optimistic_commit_info(
                    &self.store_connector,
                    OptimisticCommitInfo::new(lock_id.clone(), delta),
                )?;

            // unsafe {
            //     TAIL_LOCK_COMMIT_TIME += start_commit.elapsed().as_millis();
            // }

            //refresh meta to get the valid newest tail info
            // let start_check = Instant::now();
            let commit_res = self
                .seg_manager
                .get_mut_meta_seg()
                .check_optimistic_commit(&self.store_connector, &lock_id);

            // unsafe {
            //     TAIL_LOCK_CHECK_TIME += start_check.elapsed().as_millis();
            // }

            // TODO: remove the tail when the commit failed .
            // if commit_res {
            //     let tail_url = &SegIDUtil::get_seg_dir(new_tail, self.seg_manager.get_home_dir());
            //     println!("INFO: successfully commit a new tail {}", tail_url);
            // } else {
            //     let tail_url = &SegIDUtil::get_seg_dir(new_tail, self.seg_manager.get_home_dir());
            //     self.store_connector.remove(tail_url)?;
            //     println!("INFO: remove uncommitted tail {}", tail_url);
            // }
            Ok(commit_res)
            // Ok(true)
        } else {
            let new_tail = SegIDUtil::gen_next_tail(old_tail_id);
            // simulate singleton pattern
            let tail_update_co = TailUpdateCO::new(vec![new_tail], self.client_id, old_tail_id);
            let mut run_success: bool = false;
            loop {
                let is_uninit =
                    tail_update_co.check_uninit(&self.store_connector, &mut self.seg_manager);

                if !is_uninit {
                    break;
                }
                run_success = AirLockManager::run_with_single_lock(
                    &self.store_connector,
                    &mut self.seg_manager,
                    &tail_update_co,
                );

                if run_success {
                    break;
                }
            }

            Ok(run_success)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread::{self, JoinHandle},
    };

    use serial_test::serial;
    use tempfile::TempDir;
    use url::Url;

    use crate::{
        common::error::GResult,
        db::rw_db::{DBFactory, RWDBImpl, RWDB},
        io::{
            fake_store_service_conn::FakeStoreServiceConnector,
            file_utils::{FileUtil, UrlUtil},
            storage_connector::{StorageConnector, StorageType},
        },
        storage::{
            seg_util::{DATA_SEG_ID_MIN, META_SEG_ID, PLACEHOLDER_DATASEG_ID},
            segment::SegmentInfo,
        },
    };

    #[test]
    #[serial]
    fn db_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home_url: Url =
            UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        println!("home directory: {}", home_url.path());
        // create meta segment and the first tail segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        // meta segment
        let meta_url = SegmentInfo::generate_dir(&home_url, 0);
        first_conn.create(&meta_url)?;
        println!("meta directory: {}", meta_url.path());

        // create client tracker segment
        let client_tracker_dir = home_url
            .join("rw_client_tracker")
            .unwrap_or_else(|_| panic!("Cannot generate a path for rw_client_tracker"));
        first_conn.create(&client_tracker_dir)?;
        println!("client_tracker directory: {}", client_tracker_dir.path());

        // first tail
        let tail_url = SegmentInfo::generate_dir(&home_url, DATA_SEG_ID_MIN);

        first_conn.create(&tail_url)?;

        let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
        db.open(fake_props)?;
        db.close()?;
        Ok(())
    }

    #[test]
    #[serial]
    fn tail_update_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home_url: Url =
            UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;

        println!("home directory: {}", home_url.path());
        // create meta segment and client_tracker segment in advance
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let meta_url = SegmentInfo::generate_dir(&home_url, META_SEG_ID);
        first_conn.create(&meta_url)?;
        println!("meta directory: {}", meta_url.path());

        // create client tracker segment
        let client_tracker_dir = home_url
            .join("rw_client_tracker")
            .unwrap_or_else(|_| panic!("Cannot generate a path for rw_client_tracker"));
        first_conn.create(&client_tracker_dir)?;
        println!("client_tracker directory: {}", client_tracker_dir.path());

        // create db_impl
        let mut db_impl = RWDBImpl::<FakeStoreServiceConnector>::new_from_connector(
            home_url.clone(),
            FakeStoreServiceConnector::default(),
        );
        db_impl.open(fake_props)?;
        let _create_res = db_impl.create_or_get_updated_tail(PLACEHOLDER_DATASEG_ID)?;
        // TODO: assert creation failure because the new tail has been created in db_impl.open()
        // assert!(!create_res);
        // check whether the tail segment exists
        let client_id = db_impl.get_client_id();
        let tail_path = SegmentInfo::generate_dir(&home_url, DATA_SEG_ID_MIN | (client_id as u64));
        assert!(FileUtil::exist(&tail_path));
        db_impl.close()?;
        Ok(())
    }

    #[test]
    #[serial]
    fn multi_thread_tail_update_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home_url: Url =
            UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        // create meta segment in advance
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let meta_url = SegmentInfo::generate_dir(&home_url, META_SEG_ID);
        first_conn.create(&meta_url)?;
        println!("meta directory: {}", meta_url.path());
        // create client tracker segment
        let client_tracker_dir = home_url
            .join("rw_client_tracker")
            .unwrap_or_else(|_| panic!("Cannot generate a path for rw_client_tracker"));
        first_conn.create(&client_tracker_dir)?;
        println!("client_tracker directory: {}", client_tracker_dir.path());

        let global_tail_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        // create tail in multiple threads
        let handles: Vec<JoinHandle<()>> = (1..10)
            .map(|_x| {
                let cur_home = home_url.clone();
                let tail_count_clone = global_tail_count.clone();

                thread::spawn(move || {
                    let mut db_impl = RWDBImpl::<FakeStoreServiceConnector>::new_from_connector(
                        cur_home,
                        FakeStoreServiceConnector::default(),
                    );
                    let props: &HashMap<String, String> = &HashMap::new();
                    db_impl
                        .store_connector
                        .open(props)
                        .expect("open storage connector failed");

                    match db_impl.create_or_get_updated_tail(PLACEHOLDER_DATASEG_ID) {
                        Ok(create_res) => {
                            if create_res {
                                tail_count_clone.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        Err(err) => {
                            println!("ERROR: create_or_get_updated_tail failed: {:?}", err);
                        }
                    };
                })
            })
            .collect::<_>();

        handles
            .into_iter()
            .for_each(|handle| handle.join().expect("join failure"));

        // assure only one thread has created the tail
        assert_eq!(global_tail_count.load(Ordering::SeqCst), 1);

        Ok(())
    }
}
