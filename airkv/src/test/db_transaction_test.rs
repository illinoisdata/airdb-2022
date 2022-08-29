#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        panic,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    };

    use arrayvec::ArrayVec;
    use rand::{prelude::ThreadRng, Rng};
    use tempfile::TempDir;
    use url::Url;

    use crate::{
        common::error::GResult,
        db::rw_db::DBFactory,
        io::{
            azure_conn::AzureConnector,
            fake_store_service_conn::FakeStoreServiceConnector,
            file_utils::UrlUtil,
            local_storage_conn::LocalStorageConnector,
            storage_connector::{StorageConnector, StorageType},
        },
        storage::{
            data_segment::{
                APPEND_TIME, TAIL_CREATE_TIME, TAIL_LOCK_CHECK_TIME, TAIL_LOCK_COMMIT_TIME,
            },
            seg_util::META_SEG_ID,
            segment::{Entry, SegmentInfo},
        },
    };

    // add set up and tear down for each test
    fn run_test<T>(store_type: StorageType, test: T) -> GResult<()>
    where
        T: FnOnce(StorageType, Url) + panic::UnwindSafe,
    {
        // setup
        // create container and the meta segment
        let tmp_dir = TempDir::new()?;
        let (home_url, util_conn) = setup(store_type, &tmp_dir)?;

        let result = {
            let home_cloned = home_url.clone();
            panic::catch_unwind(move || {
                test(store_type, home_cloned);
            })
        };

        teardown(store_type, home_url, util_conn)?;
        assert!(result.is_ok());
        Ok(())
    }

    fn setup(
        store_type: StorageType,
        temp_dir: &TempDir,
    ) -> GResult<(Url, Box<dyn StorageConnector>)> {
        println!("setting up...");
        let home_url: Url = match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/", "integration");
                Url::parse(&test_path)?
            }
        };
        let mut util_conn = create_conn(store_type);
        let fake_props: &HashMap<String, String> = &HashMap::new();
        util_conn.open(fake_props)?;
        //create container
        util_conn.create(&home_url)?;
        // create the meta segment in advance
        create_meta_segment(&home_url, util_conn.as_ref()).expect("Failed to create meta segment");
        // create wr client tracker segment in advance
        create_client_tracker_segment(&home_url, util_conn.as_ref())
            .expect("Failed to create client_tracker segment");
        // TODO: create compact client tracker segment in adavance
        Ok((home_url, util_conn))
    }

    fn teardown(
        store_type: StorageType,
        home_url: Url,
        conn: Box<dyn StorageConnector>,
    ) -> GResult<()> {
        println!("tearing down...");
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                // do nothing, no need to delete container because LocalFakeStore/RemoteFakeStore uses temp directory
            }
            StorageType::AzureStore => {
                println!("remove home_dir");
                conn.remove(&home_url)?;
                thread::sleep(Duration::from_secs(30))
            }
        }
        Ok(())
    }

    fn create_conn(store_type: StorageType) -> Box<dyn StorageConnector> {
        match store_type {
            StorageType::LocalFakeStore => Box::new(LocalStorageConnector::default()),
            StorageType::RemoteFakeStore => Box::new(FakeStoreServiceConnector::default()),
            StorageType::AzureStore => Box::new(AzureConnector::default()),
        }
    }

    // create meta segment in advance and return home directory
    fn create_meta_segment(home_url: &Url, conn: &dyn StorageConnector) -> GResult<()> {
        // create meta segment
        println!("home directory: {}", home_url.path());
        // meta segment
        let meta_url = SegmentInfo::generate_dir(home_url, META_SEG_ID);
        conn.create(&meta_url)?;
        println!("meta directory: {}", meta_url.path());
        Ok(())
    }

    // create client tracker segment in advance and return home directory
    fn create_client_tracker_segment(home_url: &Url, conn: &dyn StorageConnector) -> GResult<()> {
        // create client tracker segment
        let client_tracker_dir = home_url
            .join("rw_client_tracker")
            .unwrap_or_else(|_| panic!("Cannot generate a path for rw_client_tracker"));
        conn.create(&client_tracker_dir)?;
        println!(
            "client_tracker seg directory: {}",
            client_tracker_dir.path()
        );
        Ok(())
    }

    // generate random bytes
    fn gen_random_bytes(rng: &mut ThreadRng, max: usize) -> Vec<u8> {
        (0..rng.gen_range(10..=max))
            .map(|_| rand::random::<u8>())
            .collect()
    }

    // #[test]
    // #[serial]
    // fn fake_store_single_client_txn_test() -> GResult<()> {
    //     run_test(
    //         StorageType::RemoteFakeStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_single_client_txn_test(store_type, home_url)
    //                 .expect("db_single_client_txn_test for fake connector");
    //         },
    //     )?;
    //     Ok(())
    // }

    fn db_single_client_txn_test(db_type: StorageType, home_url: Url) -> GResult<()> {
        // create db
        let mut db = DBFactory::new_rwdb(home_url, db_type);
        let mut fake_props: HashMap<String, String> = HashMap::new();
        let seg_block_num_limit: u32 = match db_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => 500,
            StorageType::AzureStore => 5,
        };
        let row_number: usize =
            seg_block_num_limit as usize * 2 + (seg_block_num_limit - 3) as usize;

        fake_props.insert(
            "SEG_BLOCK_NUM_LIMIT".to_string(),
            seg_block_num_limit.to_string(),
        );
        db.open(&fake_props)?;

        let mut rng = rand::thread_rng();
        const SAMPLE_SIZE: usize = 2000;
        let mut sample_entries = ArrayVec::<Entry, SAMPLE_SIZE>::new();

        let mut query_time: u128 = 0;
        (0..row_number).for_each(|i| {
            let mut random_part = gen_random_bytes(&mut rng, 10);
            random_part.extend(i.to_be_bytes());
            let key = random_part;
            let value = gen_random_bytes(&mut rng, 1024);

            // get samples using reservoir sampling
            if i < SAMPLE_SIZE {
                let entry = Entry::new(key.clone(), value.clone());
                sample_entries.insert(i, entry);
            } else {
                let random_v = rng.gen_range(0..i);
                if random_v < SAMPLE_SIZE {
                    let entry = Entry::new(key.clone(), value.clone());
                    sample_entries[random_v] = entry;
                }
            }
            let current = Instant::now();
            let fake_txn_options: HashMap<String, String> = HashMap::new();
            let mut txn = db.begin_transaction(&fake_txn_options);
            txn.put(key, value)
                .unwrap_or_else(|_| panic!("put entry failure for row {}", i));
            txn.commit().expect("failed to commit write transaction");
            query_time += current.elapsed().as_millis();
        });

        println!(
            "avg query latency for put is {} ms",
            query_time as f64 / row_number as f64
        );

        query_time = 0;
        // check the correctness of data by comparing with the sample
        let fake_txn_options: HashMap<String, String> = HashMap::new();
        sample_entries.iter().for_each(|entry| {
            let key = entry.get_key();
            let target_value = entry.get_value_slice();
            let current = Instant::now();
            
            let mut txn = db.begin_transaction(&fake_txn_options);
            let search_value = txn.get(key).expect("error found during searching the value");
            txn.commit().expect("failed to commit read transaction");
            query_time += current.elapsed().as_millis();
            assert!(search_value.is_some());
            assert_eq!(target_value, search_value.unwrap().get_value_slice());
        });
        println!(
            "avg query latency for get is {} ms",
            query_time as f64 / SAMPLE_SIZE as f64
        );

        db.close()?;
        Ok(())
    }




}
