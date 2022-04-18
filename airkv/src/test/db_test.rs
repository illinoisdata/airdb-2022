#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrayvec::ArrayVec;
    use rand::{prelude::ThreadRng, Rng};
    use tempfile::TempDir;
    use url::{Url};

    use crate::{
        common::error::GResult,
        db::rw_db::DBFactory,
        io::{
            fake_store_service_conn::FakeStoreServiceConnector,
            file_utils::UrlUtil,
            storage_connector::{StorageConnector, StorageType},
        },
        storage::{
            seg_util::{DATA_SEG_ID_MIN, META_SEG_ID},
            segment::{Entry, SegmentInfo, SEG_BLOCK_NUM_LIMIT},
        },
    };

    #[test]
    fn db_single_client_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home_url: Url =
            UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
        println!("home directory: {}", home_url.path());
        // create meta segment and the first tail segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        // meta segment
        let meta_url = SegmentInfo::generate_dir(&home_url, META_SEG_ID, 0);
        first_conn.create(&meta_url)?;
        println!("meta directory: {}", meta_url.path());
        // first tail
        let tail_url = SegmentInfo::generate_dir(&home_url, DATA_SEG_ID_MIN, 0);
        first_conn.create(&tail_url)?;

        let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
        db.open(fake_props)?;
        let mut rng = rand::thread_rng();

        fn gen_random_bytes(rng: &mut ThreadRng, max: usize) -> Vec<u8> {
            (0..rng.gen_range(10..=max))
                .map(|_| rand::random::<u8>())
                .collect()
        }


        const SAMPLE_SIZE: usize = 10000;
        let mut sample_entries = ArrayVec::<Entry, SAMPLE_SIZE>::new();

        // insert SEG_BLOCK_NUM_LIMIT * 2 + 1000 rows
        (0..SEG_BLOCK_NUM_LIMIT as usize * 2 + 1000).for_each(|i| {
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
            db.put(key, value).expect("put entry failure");
        });

        // check the correctness of data by comparing with the sample
        sample_entries.iter().for_each(|entry| {
            let key = entry.get_key();
            let target_value = entry.get_value();
            let search_value = db
                .get(key.to_vec())
                .expect("error found during searching the value");
            assert!(search_value.is_some());
            assert_eq!(target_value, search_value.unwrap().get_value());
        });

        db.close()?;
        Ok(())
    }
}
