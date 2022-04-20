#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        thread::{self, JoinHandle},
        time::Instant,
    };

    use arrayvec::ArrayVec;
    use rand::{prelude::ThreadRng, Rng};
    use tempfile::TempDir;
    use url::Url;

    use crate::{
        common::error::GResult,
        db::rw_db::DBFactory,
        io::{
            fake_store_service_conn::FakeStoreServiceConnector,
            file_utils::UrlUtil,
            storage_connector::{StorageConnector, StorageType},
        },
        storage::{
            seg_util::META_SEG_ID,
            segment::{Entry, SegmentInfo},
        },
    };

    // create meta segment in advance and return home directory
    fn create_meta_segment(home_url: &Url) -> GResult<()> {
        println!("home directory: {}", home_url.path());
        // create meta segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        // meta segment
        let meta_url = SegmentInfo::generate_dir(home_url, META_SEG_ID, 0);
        first_conn.create(&meta_url)?;
        println!("meta directory: {}", meta_url.path());
        Ok(())
    }

    // generate random bytes
    fn gen_random_bytes(rng: &mut ThreadRng, max: usize) -> Vec<u8> {
        (0..rng.gen_range(10..=max))
            .map(|_| rand::random::<u8>())
            .collect()
    }

    #[test]
    fn db_single_client_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home_url: Url =
            UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        create_meta_segment(&home_url)?;

        // create db
        let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
        let mut fake_props: HashMap<String, String> = HashMap::new();
        fake_props.insert("SEG_BLOCK_NUM_LIMIT".to_string(), "500".to_string());
        db.open(&fake_props)?;

        let mut rng = rand::thread_rng();
        const SAMPLE_SIZE: usize = 2000;
        let mut sample_entries = ArrayVec::<Entry, SAMPLE_SIZE>::new();

        // insert SEG_BLOCK_NUM_LIMIT * 2 + 1000 rows
        let row_number: usize = db.get_props().get_seg_block_num_limit() as usize * 2 + 1000;
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
            "avg query latency for put is {} ms",
            query_time as f64 / row_number as f64
        );

        query_time = 0;
        // check the correctness of data by comparing with the sample
        sample_entries.iter().for_each(|entry| {
            let key = entry.get_key();
            let target_value = entry.get_value_slice();
            let current = Instant::now();
            let search_value = db.get(key).expect("error found during searching the value");

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
    #[test]
    fn db_multi_client_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home: Url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        create_meta_segment(&home)?;
        // multiple clients write and read
        let handles: Vec<JoinHandle<()>> = (0..10)
            .map(|_x| {
                let home_url = home.clone();
                thread::spawn(move || {
                    println!("client {:?}: start ...", thread::current().id());
                    // create db
                    let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
                    let mut fake_props: HashMap<String, String> = HashMap::new();
                    fake_props.insert("SEG_BLOCK_NUM_LIMIT".to_string(), "500".to_string());

                    db.open(&fake_props).expect("db.open() failed");

                    let mut rng = rand::thread_rng();
                    const SAMPLE_SIZE: usize = 2000;
                    let mut sample_entries = ArrayVec::<Entry, SAMPLE_SIZE>::new();
                    // insert SEG_BLOCK_NUM_LIMIT + 100 rows
                    let row_number: usize = db.get_props().get_seg_block_num_limit() as usize + 100;

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

                    query_time = 0;
                    // check the correctness of data by comparing with the sample
                    sample_entries.iter().for_each(|entry| {
                        let key = entry.get_key();
                        let target_value = entry.get_value_slice();
                        let current = Instant::now();
                        let search_value =
                            db.get(key).expect("error found during searching the value");

                        query_time += current.elapsed().as_millis();
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

    // multiple WR clients: each client will call put() and get() in turn.
    // read_your_write: each client should be able to read its own committed records
    #[test]
    fn read_committed_multi_client_test_1() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home: Url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        create_meta_segment(&home)?;
        // multiple clients write and read
        let handles: Vec<JoinHandle<()>> = (0..10)
            .map(|_x| {
                let home_url = home.clone();
                thread::spawn(move || {
                    println!("client {:?}: start ...", thread::current().id());
                    // create db
                    let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
                    let mut fake_props: HashMap<String, String> = HashMap::new();
                    fake_props.insert("SEG_BLOCK_NUM_LIMIT".to_string(), "500".to_string());

                    db.open(&fake_props).expect("db.open() failed");

                    let mut rng = rand::thread_rng();
                    let row_number: usize = db.get_props().get_seg_block_num_limit() as usize + 100;

                    let mut put_time: u128 = 0;
                    let mut get_time: u128 = 0;
                    (0..row_number).for_each(|_i| {
                        let mut random_part = vec![];
                        random_part.extend(db.get_client_id().as_bytes());
                        random_part.extend(gen_random_bytes(&mut rng, 10));
                        let key = random_part;
                        let value = gen_random_bytes(&mut rng, 1024);
                        let key_cp = key.clone();
                        let value_cp = value.clone();

                        let current = Instant::now();
                        db.put(key, value).expect("put entry failure");
                        put_time += current.elapsed().as_millis();

                        let now = Instant::now();
                        let search_value = db
                            .get(&key_cp)
                            .expect("error found during searching the value");
                        get_time += now.elapsed().as_millis();

                        assert!(search_value.is_some());
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
    #[test]
    fn read_committed_multi_client_test_2() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home: Url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        create_meta_segment(&home)?;
        let latest_commit: Arc<RwLock<Vec<u8>>> = Arc::new(RwLock::new(vec![]));

        // multiple clients write and read
        let handles: Vec<JoinHandle<()>> = (0..10)
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
                        let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
                        let mut fake_props: HashMap<String, String> = HashMap::new();
                        fake_props.insert("SEG_BLOCK_NUM_LIMIT".to_string(), "500".to_string());

                        db.open(&fake_props).expect("db.open() failed");

                        let mut rng = rand::thread_rng();

                        let row_number: usize =
                            db.get_props().get_seg_block_num_limit() as usize + 100;
                        let mut put_time: u128 = 0;

                        (0..row_number).for_each(|_i| {
                            let mut random_part = vec![];
                            random_part.extend(db.get_client_id().as_bytes());
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
                        fake_props.insert("SEG_BLOCK_NUM_LIMIT".to_string(), "500".to_string());

                        db.open(&fake_props).expect("db.open() failed");

                        let sample_size = 450;
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
}
