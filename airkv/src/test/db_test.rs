#[cfg(test)]
mod tests {
    use serial_test::serial;

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

    #[test]
    #[serial]
    fn fake_store_single_client_test() -> GResult<()> {
        run_test(
            StorageType::RemoteFakeStore,
            |store_type: StorageType, home_url: Url| {
                db_single_client_test(store_type, home_url)
                    .expect("db_single_client_test for fake connector");
            },
        )?;
        Ok(())
    }

    // #[test]
    // #[serial]
    // fn fake_store_multi_client_test() -> GResult<()> {
    //     run_test(
    //         StorageType::RemoteFakeStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_multi_client_test(store_type, home_url)
    //                 .expect("db_single_client_test for fake connector");
    //         },
    //     )?;
    //     Ok(())
    // }

    // #[test]
    // #[serial]
    // fn fake_store_read_committed_multi_client_test_1() -> GResult<()> {
    //     run_test(
    //         StorageType::RemoteFakeStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_read_committed_multi_client_test_1(store_type, home_url)
    //                 .expect("db_read_committed_multi_client_test_1 for fake connector");
    //         },
    //     )?;
    //     Ok(())
    // }

    // #[test]
    // #[serial]
    // fn fake_store_read_committed_multi_client_test_2() -> GResult<()> {
    //     run_test(
    //         StorageType::RemoteFakeStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_read_committed_multi_client_test_2(store_type, home_url)
    //                 .expect("db_read_committed_multi_client_test_1 for fake connector");
    //         },
    //     )?;
    //     Ok(())
    // }

    #[test]
    #[serial]
    fn azure_single_client_test() -> GResult<()> {
        run_test(
            StorageType::AzureStore,
            |store_type: StorageType, home_url: Url| {
                db_single_client_test(store_type, home_url)
                    .expect("db_single_client_test for azure connector");
            },
        )?;
        Ok(())
    }

    // #[test]
    // #[serial]
    // fn azure_single_client_compaction_test() -> GResult<()> {
    //     run_test(
    //         StorageType::AzureStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_single_compaction_client_test(store_type, home_url)
    //                 .expect("db_single_compaction_client_test for azure connector");
    //         },
    //     )?;
    //     Ok(())
    // }

    #[test]
    #[serial]
    fn azure_multi_client_test() -> GResult<()> {
        run_test(
            StorageType::AzureStore,
            |store_type: StorageType, home_url: Url| {
                db_multi_client_test(store_type, home_url)
                    .expect("db_multi_client_test for azure connector");
            },
        )?;
        Ok(())
    }

    // #[test]
    // #[serial]
    // fn azure_read_committed_multi_client_test_1() -> GResult<()> {
    //     run_test(
    //         StorageType::AzureStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_read_committed_multi_client_test_1(store_type, home_url)
    //                 .expect("db_read_committed_multi_client_test_1 for azure connector");
    //         },
    //     )?;
    //     Ok(())
    // }

    // #[test]
    // #[serial]
    // fn azure_read_committed_multi_client_test_2() -> GResult<()> {
    //     run_test(
    //         StorageType::AzureStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_read_committed_multi_client_test_2(store_type, home_url)
    //                 .expect("db_read_committed_multi_client_test_1 for azure connector");
    //         },
    //     )?;
    //     Ok(())
    // }

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

    fn db_single_client_test(db_type: StorageType, home_url: Url) -> GResult<()> {
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
            db.put(key, value)
                .unwrap_or_else(|_| panic!("put entry failure for row {}", i));
            query_time += current.elapsed().as_millis();
        });

        println!(
            "avg query latency for put is {} ms",
            query_time as f64 / row_number as f64
        );

        query_time = 0;
        // check the correctness of data by comparing with the sample

        sample_entries.iter().enumerate().for_each(|(idx, entry)| {
            let key = entry.get_key();
            let target_value = entry.get_value_slice();
            let current = Instant::now();
            let search_value = db.get(key)
            .unwrap_or_else(|_| panic!("error found during searching the value with idx {}", idx));

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

    // multiple clients
    // first writes some rows and then read the sample values to verify the correctness
    fn db_multi_client_test(store_type: StorageType, home: Url) -> GResult<()> {
        // multiple clients write and read
        let handles: Vec<JoinHandle<()>> = (0..3)
            .map(|_x| {
                let home_url = home.clone();
                thread::spawn(move || {
                    println!("client {:?}: start ...", thread::current().id());
                    // create db
                    let mut db = DBFactory::new_rwdb(home_url, store_type);
                    let seg_block_num_limit: u32 = match store_type {
                        StorageType::LocalFakeStore | StorageType::RemoteFakeStore => 500,
                        StorageType::AzureStore => 5,
                    };
                    let row_number: usize =
                        seg_block_num_limit as usize * 2 + (seg_block_num_limit - 3) as usize;

                    let mut fake_props: HashMap<String, String> = HashMap::new();
                    fake_props.insert(
                        "SEG_BLOCK_NUM_LIMIT".to_string(),
                        seg_block_num_limit.to_string(),
                    );
                    db.open(&fake_props).expect("db.open() failed");

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
                        db.put(key, value).expect("put entry failure");
                        query_time += current.elapsed().as_millis();
                    });

                    println!(
                        "Thread {:?}: avg query latency for put is {} ms",
                        thread::current().id(),
                        query_time as f64 / row_number as f64
                    );

                    let mut row_c = 0;
                    query_time = 0;
                    // check the correctness of data by comparing with the sample
                    sample_entries.iter().for_each(|entry| {
                        let key = entry.get_key();
                        let target_value = entry.get_value_slice();
                        let current = Instant::now();
                        // let search_value =
                        // db.get(key).expect("error found during searching the value");

                        let search_value = match db.get(key) {
                            Ok(value) => value,
                            Err(err) => {
                                panic!("get err: {:?}", err);
                            }
                        };

                        query_time += current.elapsed().as_millis();
                        row_c += 1;
                        assert!(search_value.is_some());
                        assert_eq!(target_value, search_value.unwrap().get_value_slice());
                    });
                    println!(
                        "Thread {:?}: avg query latency for get is {} ms",
                        thread::current().id(),
                        query_time as f64 / SAMPLE_SIZE as f64
                    );

                    db.close().expect("db close failed");
                })
            })
            .collect::<_>();

        handles
            .into_iter()
            .for_each(|handle| handle.join().expect("join failure"));

        Ok(())
    }

    // #[test]
    // #[serial]
    // fn azure_read_committed_multi_client_test_1() -> GResult<()> {
    //     run_test(
    //         StorageType::AzureStore,
    //         |store_type: StorageType, home_url: Url| {
    //             db_read_committed_multi_client_test_1(store_type, home_url)
    //                 .expect("db_read_committed_multi_client_test_1 for azure connector");
    //         },
    //     )?;
    //     Ok(())
    // }

    fn db_read_committed_multi_client_test_1(store_type: StorageType, home: Url) -> GResult<()> {
        // multiple clients write and read
        let block_num_limit = match store_type {
            StorageType::RemoteFakeStore => 500,
            StorageType::AzureStore => 50,
            StorageType::LocalFakeStore => todo!(),
        };
        let thread_num = match store_type {
            StorageType::RemoteFakeStore => 10,
            StorageType::AzureStore => 10,
            // StorageType::AzureStore => 5,
            StorageType::LocalFakeStore => todo!(),
        };
        let handles: Vec<JoinHandle<()>> = (0..thread_num)
            .map(|_x| {
                let home_url = home.clone();
                thread::spawn(move || {
                    println!("client {:?}: start ...", thread::current().id());
                    // create db
                    let mut db = DBFactory::new_rwdb(home_url, store_type);
                    let mut fake_props: HashMap<String, String> = HashMap::new();
                    fake_props.insert(
                        "SEG_BLOCK_NUM_LIMIT".to_string(),
                        block_num_limit.to_string(),
                    );
                    db.open(&fake_props).expect("db.open() failed");

                    let mut rng = rand::thread_rng();
                    let row_number: usize = (block_num_limit + (block_num_limit / 5)) as usize;

                    let mut put_time: u128 = 0;
                    let mut get_time: u128 = 0;
                    (0..row_number).for_each(|i| {
                        let mut random_part = vec![];
                        random_part.extend(db.get_client_id().to_be_bytes());
                        random_part.extend(gen_random_bytes(&mut rng, 10));
                        let key = random_part;
                        let value = gen_random_bytes(&mut rng, 1024);
                        let key_cp = key.clone();
                        let value_cp = value.clone();

                        let current = Instant::now();
                        db.put(key, value).expect("put entry failure");
                        put_time += current.elapsed().as_millis();

                        let now = Instant::now();
                        let search_value = db.get(&key_cp).unwrap_or_else(|_| {
                            panic!(
                                "error found during searching the value for row number {}",
                                i
                            )
                        });

                        get_time += now.elapsed().as_millis();

                        assert!(
                            search_value.is_some(),
                            "error found during searching the value for row number {}",
                            i
                        );
                        assert_eq!(value_cp, search_value.unwrap().get_value_slice());
                    });

                    println!(
                        "Thread {:?}: avg query latency for put is {} ms",
                        thread::current().id(),
                        put_time as f64 / row_number as f64
                    );

                    println!(
                        "Thread {:?}: avg query latency for get is {} ms",
                        thread::current().id(),
                        get_time as f64 / row_number as f64
                    );
                    db.close().expect("db close failed");
                })
            })
            .collect::<_>();

        handles
            .into_iter()
            .for_each(|handle| handle.join().expect("join failure"));
        Ok(())
    }

    // multiple write clients, multiple read clients
    // read-all-committed: the read client should be able to read all committed records
    fn db_read_committed_multi_client_test_2(store_type: StorageType, home: Url) -> GResult<()> {
        let latest_commit: Arc<RwLock<Vec<u8>>> = Arc::new(RwLock::new(vec![]));
        // multiple clients write and read
        let block_num_limit = match store_type {
            StorageType::RemoteFakeStore => 500,
            StorageType::AzureStore => 50,
            StorageType::LocalFakeStore => todo!(),
        };
        let thread_num = match store_type {
            StorageType::RemoteFakeStore => 10,
            StorageType::AzureStore => 10,
            StorageType::LocalFakeStore => todo!(),
        };

        // multiple clients write and read
        let handles: Vec<JoinHandle<()>> = (0..thread_num)
            .map(|idx| {
                let home_url = home.clone();
                let latest_commit_clone = latest_commit.clone();
                thread::spawn(move || {
                    // when idx = 4 or 7, the threads will launch read clients
                    // otherwise, launch write clients
                    if idx != 4 && idx != 7 {
                        // write client
                        println!("write client {:?}: start ...", thread::current().id());
                        // create db
                        let mut db = DBFactory::new_rwdb(home_url, store_type);
                        let mut fake_props: HashMap<String, String> = HashMap::new();
                        fake_props.insert(
                            "SEG_BLOCK_NUM_LIMIT".to_string(),
                            block_num_limit.to_string(),
                        );
                        db.open(&fake_props).expect("db.open() failed");

                        let mut rng = rand::thread_rng();

                        let row_number: usize = (block_num_limit + (block_num_limit / 5)) as usize;

                        let mut put_time: u128 = 0;

                        (0..row_number).for_each(|_i| {
                            let mut random_part = vec![];
                            random_part.extend(db.get_client_id().to_be_bytes());
                            random_part.extend(gen_random_bytes(&mut rng, 10));
                            let key = random_part;
                            let key_clone = key.clone();
                            let value = key.clone();

                            let current = Instant::now();
                            db.put(key, value).expect("put entry failure");
                            put_time += current.elapsed().as_millis();
                            let mut w = latest_commit_clone.write().unwrap();
                            *w = key_clone;
                        });

                        println!(
                            "Write client {:?}: avg query latency for put is {} ms",
                            thread::current().id(),
                            put_time as f64 / row_number as f64
                        );
                        db.close().expect("db close failed");
                    } else {
                        // read client
                        println!("read client {:?}: start ...", thread::current().id());
                        // create db
                        let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
                        let mut fake_props: HashMap<String, String> = HashMap::new();
                        fake_props.insert(
                            "SEG_BLOCK_NUM_LIMIT".to_string(),
                            block_num_limit.to_string(),
                        );

                        db.open(&fake_props).expect("db.open() failed");

                        let sample_size = block_num_limit;
                        let mut get_time: u128 = 0;

                        (0..sample_size).for_each(|_x| {
                            let mut cur_key: Vec<u8> = vec![];
                            while cur_key.is_empty() {
                                let key = latest_commit_clone.read().unwrap();
                                cur_key = (*key).clone();
                            }
                            let now = Instant::now();
                            let search_value = db
                                .get(&cur_key)
                                .expect("error found during searching the value");
                            get_time += now.elapsed().as_millis();

                            assert!(search_value.is_some());
                            assert_eq!(cur_key, search_value.unwrap().get_value_slice());
                        });

                        println!(
                            "Read client {:?}: avg query latency for get is {} ms",
                            thread::current().id(),
                            get_time as f64 / sample_size as f64
                        );

                        db.close().expect("db close failed");
                    }
                })
            })
            .collect::<_>();

        handles
            .into_iter()
            .for_each(|handle| handle.join().expect("join failure"));
        Ok(())
    }

    fn db_single_compaction_client_test(db_type: StorageType, home_url: Url) -> GResult<()> {
        // launch a compactioin client in advance
        let home_url_compact = home_url.clone();
        // let mut need_stop = false;
        let need_stop = Arc::new(AtomicBool::new(false));

        let need_stop_clone = need_stop.clone();

        let t = thread::spawn(move || {
            println!("create compaction_db");
            let mut c_db = DBFactory::new_compactiondb(home_url_compact, StorageType::AzureStore);

            c_db.open(&HashMap::new())
                .expect("failed to call compactionDB.open()");
            while !need_stop_clone.load(Ordering::Relaxed) {
                let task = c_db.get_task();
                if let Some(task_desc) = task {
                    let res = c_db.execute(&task_desc).unwrap();
                    if res {
                        println!("INFO: run compaction successfully for task: {}", task_desc);
                    } else {
                        println!(
                            "WARN: finished executing compaction but failed to commit for task: {}",
                            task_desc
                        );
                    }
                } else {
                    println!("INFO: wait 20s to check compaction opportunity");
                    thread::sleep(Duration::from_secs(20));
                }
            }
        });

        // create db
        let mut db = DBFactory::new_rwdb(home_url, db_type);
        let mut fake_props: HashMap<String, String> = HashMap::new();
        // let seg_block_num_limit: u32 = match db_type {
        //     StorageType::LocalFakeStore | StorageType::RemoteFakeStore => 500,
        //     StorageType::AzureStore => 5,
        // };
        let seg_block_num_limit: u32 = match db_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => 500,
            StorageType::AzureStore => 2000,
        };
        // let row_number: usize =
        //     seg_block_num_limit as usize * 40 + (seg_block_num_limit - 3) as usize;
        let row_number: usize =
            seg_block_num_limit as usize * 4 + (seg_block_num_limit - 3) as usize;

        fake_props.insert(
            "SEG_BLOCK_NUM_LIMIT".to_string(),
            seg_block_num_limit.to_string(),
        );
        db.open(&fake_props)?;

        println!("open rw_db");
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
            db.put(key, value)
                .unwrap_or_else(|_| panic!("put entry failure for row {}", i));
            query_time += current.elapsed().as_millis();
        });

        println!("total query latency for put is {} ms", query_time);
        println!(
            "avg query latency for put is {} ms",
            query_time as f64 / row_number as f64
        );

        // unsafe {
        //     println!("total append file latency for put is {} ms", APPEND_TIME);
        //     println!(
        //         "avg query latency to append tail is {} ms",
        //         APPEND_TIME as f64 / row_number as f64
        //     );

        //     println!("total create tail latency for put is {} ms", TAIL_CREATE_TIME);
        //     println!(
        //         "avg query latency to create tail latency is {} ms",
        //         TAIL_CREATE_TIME as f64 / row_number as f64
        //     );

        //     println!("total tail lock commit latency for put is {} ms", TAIL_LOCK_COMMIT_TIME);
        //     println!(
        //         "avg query latency for tail lock commit is {} ms",
        //         TAIL_LOCK_COMMIT_TIME as f64 / row_number as f64
        //     );

        //     println!("total tail lock check latency for put is {} ms", TAIL_LOCK_CHECK_TIME);
        //     println!(
        //         "avg query latency for tail lock check is {} ms",
        //         TAIL_LOCK_CHECK_TIME as f64 / row_number as f64
        //     );
        // }

        query_time = 0;
        // check the correctness of data by comparing with the sample
        sample_entries.iter().for_each(|entry| {
            let key = entry.get_key();
            let target_value = entry.get_value_slice();
            let current = Instant::now();
            let search_value = db.get(key).expect("error found during searching the value");

            query_time += current.elapsed().as_millis();
            assert!(search_value.is_some());
            println!("INFO: successfully searched a key");
            assert_eq!(target_value, search_value.unwrap().get_value_slice());
        });
        println!(
            "avg query latency for get is {} ms",
            query_time as f64 / SAMPLE_SIZE as f64
        );
        need_stop.store(true, Ordering::Relaxed);
        t.join().expect("join failure");
        db.close()?;
        Ok(())
    }
}
