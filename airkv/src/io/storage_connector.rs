use url::Url;

use crate::{
    common::error::GResult,
    storage::{
        data_entry::AppendRes,
        segment::{SegSize, SegmentProps},
    },
};
use std::collections::HashMap;

use super::file_utils::Range;

#[derive(Copy, Clone)]
pub enum StorageType {
    LocalFakeStore,
    RemoteFakeStore,
    AzureStore,
}

pub trait StorageConnector {
    // open connection
    fn open(&mut self, props: &HashMap<String, String>) -> GResult<()>;

    // close connection and clear states
    fn close(&mut self) -> GResult<()>;

    // read whole segment specified in path
    fn read_all(&self, path: &Url) -> GResult<Vec<u8>>;

    // read range starting at offset for length bytes
    // TODO: support the case when range.length = 0, it means reading from an offset to the end of the segment
    fn read_range(&self, path: &Url, range: &Range) -> GResult<Vec<u8>>;

    // get the current length of the target segment
    fn get_size(&self, path: &Url) -> GResult<u64>;

    // get common properties of the target segment
    fn get_props(&self, path: &Url) -> GResult<SegmentProps>;

    // seal the target segment(change its access permission as read-only)
    fn seal(&self, path: &Url) -> GResult<()>;

    // create empty segment at a target path
    fn create(&self, path: &Url) -> GResult<()>;

    // append the byte array to the end of a target segment
    fn append(&self, path: &Url, buf: &[u8]) -> AppendRes<SegSize>;

    // write whole byte array to a segment
    fn write_all(&self, path: &Url, buf: &[u8]) -> GResult<()>;

    // remove the whole segment
    fn remove(&self, path: &Url) -> GResult<()>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serial_test::serial;
    use tempfile::TempDir;
    use url::Url;

    use crate::{
        common::error::GResult,
        io::{
            azure_conn::AzureConnector,
            fake_store_service_conn::FakeStoreServiceConnector,
            file_utils::{Range, UrlUtil},
            local_storage_conn::LocalStorageConnector,
            storage_connector::StorageConnector,
        },
        storage::data_entry::AppendRes,
    };
    use core::time;
    use std::thread::{self, JoinHandle};

    use rand::Rng;

    use crate::common::error::AppendError;

    use super::StorageType;

    #[test]
    #[serial]
    fn azure_basic_op_test() -> GResult<()> {
        basic_op_test(StorageType::AzureStore, &mut AzureConnector::default())?;
        Ok(())
    }

    #[test]
    #[serial]
    fn azure_write_read_all_zero_ok() -> GResult<()> {
        write_read_all_zero_ok(StorageType::AzureStore, &mut AzureConnector::default())?;
        Ok(())
    }

    #[test]
    #[serial]
    fn azure_write_read_all_random_ok() -> GResult<()> {
        write_read_all_random_ok(StorageType::AzureStore, &mut AzureConnector::default())?;
        Ok(())
    }

    #[test]
    #[serial]
    fn azure_write_twice_read_all_random_ok() -> GResult<()> {
        write_twice_read_all_random_ok(StorageType::AzureStore, &mut AzureConnector::default())?;
        Ok(())
    }

    #[test]
    #[serial]
    fn azure_write_read_range_random_ok() -> GResult<()> {
        write_read_range_random_ok(StorageType::AzureStore, &mut AzureConnector::default())?;
        Ok(())
    }

    #[test]
    #[serial]
    fn azure_single_thread_append_test() -> GResult<()> {
        single_thread_append_test(StorageType::AzureStore, &mut AzureConnector::default())?;
        Ok(())
    }

    #[test]
    #[serial]
    fn azure_multi_thread_append_test() -> GResult<()> {
        multi_thread_append_test(StorageType::AzureStore, &mut AzureConnector::default())?;
        Ok(())
    }

    #[test]
    #[serial]
    fn fake_store_basic_op_test() -> GResult<()> {
        basic_op_test(
            StorageType::RemoteFakeStore,
            &mut FakeStoreServiceConnector::default(),
        )?;
        Ok(())
    }

    #[test]
    #[serial]
    fn fake_store_write_read_all_zero_ok() -> GResult<()> {
        write_read_all_zero_ok(
            StorageType::RemoteFakeStore,
            &mut FakeStoreServiceConnector::default(),
        )?;
        Ok(())
    }

    #[test]
    #[serial]
    fn fake_store_write_read_all_random_ok() -> GResult<()> {
        write_read_all_random_ok(
            StorageType::RemoteFakeStore,
            &mut FakeStoreServiceConnector::default(),
        )?;
        Ok(())
    }

    #[test]
    #[serial]
    fn fake_store_write_twice_read_all_random_ok() -> GResult<()> {
        write_twice_read_all_random_ok(
            StorageType::RemoteFakeStore,
            &mut FakeStoreServiceConnector::default(),
        )?;
        Ok(())
    }

    #[test]
    #[serial]
    fn fake_store_write_read_range_random_ok() -> GResult<()> {
        write_read_range_random_ok(
            StorageType::RemoteFakeStore,
            &mut FakeStoreServiceConnector::default(),
        )?;
        Ok(())
    }

    #[test]
    #[serial]
    fn fake_store_single_thread_append_test() -> GResult<()> {
        single_thread_append_test(
            StorageType::RemoteFakeStore,
            &mut FakeStoreServiceConnector::default(),
        )?;
        Ok(())
    }

    #[test]
    #[serial]
    fn fake_store_multi_thread_append_test() -> GResult<()> {
        multi_thread_append_test(
            StorageType::RemoteFakeStore,
            &mut FakeStoreServiceConnector::default(),
        )?;
        Ok(())
    }

    fn basic_op_test<T: StorageConnector>(
        store_type: StorageType,
        first_conn: &mut T,
    ) -> GResult<()> {
        let temp_dir: TempDir;
        let url: Url;
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                temp_dir = TempDir::new()?;
                url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/{}", "airkv", "test_blob_1");
                url = Url::parse(&test_path)?;
            }
        }
        let test_url = &url;
        let fake_props: &HashMap<String, String> = &HashMap::new();
        println!("ready to open");
        first_conn.open(fake_props)?;
        println!("open success");

        first_conn.create(test_url)?;
        println!("create success");

        let content =
            "The first sentence in a paragraph is sometimes called the key or topic sentence \
    because it gives us the key to what the paragraph will be about.";
        let buf: &[u8] = &String::from(content).into_bytes();
        let content1 =
            "Second round: The first sentence in a paragraph is sometimes called the key or topic sentence \
    because it gives us the key to what the paragraph will be about.";
        let buf1: &[u8] = &String::from(content1).into_bytes();
        let res = first_conn.append(test_url, buf);
        match res {
            AppendRes::Success(size) => {
                println!("append success with block_size {}", size);
                assert_eq!(size, 1);
            }
            _ => {
                panic!("ERROR:append failed")
            }
        }

        let props = first_conn.get_props(test_url)?;
        assert_eq!(props.get_seg_len(), buf.len() as u64);
        assert_eq!(props.get_block_num(), 1);
        assert!(!props.is_sealed());
        let res1 = first_conn.append(test_url, buf1);
        match res1 {
            AppendRes::Success(size) => {
                println!("append success with block_size {}", 2);
                assert_eq!(size, 2);
            }
            _ => {
                panic!("ERROR:append failed")
            }
        }
        let props1 = first_conn.get_props(test_url)?;
        assert_eq!(props1.get_seg_len(), (buf.len() + buf1.len()) as u64);
        assert_eq!(props1.get_block_num(), 2);
        assert!(!props1.is_sealed());

        first_conn.seal(test_url)?;
        println!("seal success");

        let props2 = first_conn.get_props(test_url)?;
        assert_eq!(props2.get_seg_len(), (buf.len() + buf1.len()) as u64);
        assert!(props2.is_sealed());

        let res2 = first_conn.append(test_url, buf);
        assert!(matches!(res2, AppendRes::AppendToSealedFailure));

        first_conn.seal(test_url)?;
        println!("seal success");

        let read_data = first_conn.read_range(test_url, &Range::new(0, buf.len() as u64))?;
        assert_eq!(buf, read_data);

        let read_data1 = first_conn.read_range(test_url, &Range::new(buf.len() as u64, 0))?;
        assert_eq!(buf1, read_data1);

        let read_data2 = first_conn.read_all(test_url)?;
        assert_eq!([buf, buf1].concat(), read_data2);
        first_conn.remove(test_url)?;

        Ok(())
    }

    fn write_read_all_zero_ok<T: StorageConnector>(
        store_type: StorageType,
        first_conn: &mut T,
    ) -> GResult<()> {
        let temp_dir: TempDir;
        let url: Url;
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                temp_dir = TempDir::new()?;
                url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/{}", "airkv", "test_blob_1");
                url = Url::parse(&test_path)?;
            }
        }
        let test_url = &url;

        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        // write some data
        let test_data = [0u8; 256];
        first_conn.write_all(test_url, &test_data)?;

        // read and check
        let test_data_reread = first_conn.read_all(test_url)?;
        first_conn.remove(test_url)?;
        assert_eq!(
            &test_data[..],
            &test_data_reread[..],
            "Reread data not matched with original one"
        );
        Ok(())
    }

    fn write_read_all_random_ok<T: StorageConnector>(
        store_type: StorageType,
        first_conn: &mut T,
    ) -> GResult<()> {
        let temp_dir: TempDir;
        let url: Url;
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                temp_dir = TempDir::new()?;
                url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/{}", "airkv", "test_blob_1");
                url = Url::parse(&test_path)?;
            }
        }
        let test_url = &url;
        // write some data
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let mut test_data = [0u8; 256];
        rand::thread_rng().fill(&mut test_data[..]);
        first_conn.write_all(test_url, &test_data)?;

        // read and check
        let test_data_reread = first_conn.read_all(test_url)?;
        first_conn.remove(test_url)?;

        assert_eq!(
            &test_data[..],
            &test_data_reread[..],
            "Reread data not matched with original one"
        );
        Ok(())
    }

    fn write_twice_read_all_random_ok<T: StorageConnector>(
        store_type: StorageType,
        first_conn: &mut T,
    ) -> GResult<()> {
        let temp_dir: TempDir;
        let url: Url;
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                temp_dir = TempDir::new()?;
                url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/{}", "airkv", "test_blob_1");
                url = Url::parse(&test_path)?;
            }
        }
        let test_url = &url;
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let test_data_old = [1u8; 256];
        first_conn.write_all(test_url, &test_data_old)?;

        // write more, this should completely replace previous result
        let test_data_actual = [2u8; 128];
        first_conn.write_all(test_url, &test_data_actual)?;

        // read and check
        let test_data_reread = first_conn.read_all(test_url)?;
        first_conn.remove(test_url)?;

        assert_ne!(
            &test_data_old[..],
            &test_data_reread[..],
            "Old data should be removed"
        );
        assert_eq!(
            &test_data_actual[..],
            &test_data_reread[..],
            "Reread data not matched with original one, possibly containing old data"
        );
        Ok(())
    }

    ///
    /// use the same set of tests as LocalStorageConnector to varify FakeStoreServiceConnector
    ///
    fn write_read_range_random_ok<T: StorageConnector>(
        store_type: StorageType,
        first_conn: &mut T,
    ) -> GResult<()> {
        let temp_dir: TempDir;
        let url: Url;
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                temp_dir = TempDir::new()?;
                url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/{}", "airkv", "test_blob_1");
                url = Url::parse(&test_path)?;
            }
        }
        let test_url = &url;
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let mut test_data = [0u8; 256];
        rand::thread_rng().fill(&mut test_data[..]);
        first_conn.write_all(test_url, &test_data)?;

        // test 100 random ranges
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            let offset = rng.gen_range(0..test_data.len() - 1);
            let length = rng.gen_range(1..test_data.len() - offset);
            let test_data_range =
                first_conn.read_range(test_url, &Range::new_usize(offset, length))?;
            let test_data_expected = &test_data[offset..offset + length];
            assert_eq!(
                test_data_expected,
                &test_data_range[..],
                "Reread data not matched with original one"
            );
        }
        first_conn.remove(test_url)?;
        Ok(())
    }

    /* Test single-thread append */
    fn single_thread_append_test<T: StorageConnector>(
        store_type: StorageType,
        first_conn: &mut T,
    ) -> GResult<()> {
        let temp_dir: TempDir;
        let url: Url;
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                temp_dir = TempDir::new()?;
                url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/{}", "airkv", "test_blob_1");
                url = Url::parse(&test_path)?;
            }
        }
        let test_url = &url;
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        first_conn.create(test_url)?;

        let content =
            "The first sentence in a paragraph is sometimes called the key or topic sentence \
        because it gives us the key to what the paragraph will be about.";
        let buf = String::from(content).into_bytes();
        let res = first_conn.append(test_url, &buf);
        assert!(res.is_success());
        let res1 = first_conn.append(test_url, &buf);
        assert!(res1.is_success());
        let res2 = first_conn.append(test_url, &buf);
        assert!(res2.is_success());
        let res3 = first_conn.seal(test_url);
        assert!(res3.is_ok());

        // sleep to wait the concumer thread to flush data to the disk
        let sleep_period = time::Duration::from_secs(3);
        thread::sleep(sleep_period);
        // check correctness
        let res_buf = first_conn.read_all(test_url)?;
        let mut expect_buf = buf.clone();
        expect_buf.extend(&buf);
        expect_buf.extend(&buf);
        assert_eq!(expect_buf, res_buf);
        first_conn.remove(test_url)?;

        Ok(())
    }

    /* Test multi-thread append */
    fn multi_thread_append_test<T: StorageConnector>(
        store_type: StorageType,
        read_conn: &mut T,
    ) -> GResult<()> {
        let temp_dir: TempDir;
        let url: Url;
        match store_type {
            StorageType::LocalFakeStore | StorageType::RemoteFakeStore => {
                temp_dir = TempDir::new()?;
                url = UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?;
            }
            StorageType::AzureStore => {
                let test_path = format!("az:///{}/{}", "airkv", "test_blob_1");
                url = Url::parse(&test_path)?;
            }
        }
        let test_url = &url;
        let content =
            "The first sentence in a paragraph is sometimes called the key or topic sentence \
        because it gives us the key to what the paragraph will be about.";
        let buf = String::from(content).into_bytes();

        //create segment in advance
        let fake_props: &HashMap<String, String> = &HashMap::new();
        read_conn.open(fake_props)?;
        read_conn.create(test_url)?;

        let mut clients: Vec<JoinHandle<_>> = vec![];
        //launch three threads to append data simultaneously
        //each thread simulates a client
        for _i in 1..=3 {
            let p_c = test_url.clone();
            let b_c = buf.clone();
            let handle = thread::spawn(move || match store_type {
                StorageType::LocalFakeStore => {
                    let mut conn = LocalStorageConnector::default();
                    let fake_props: &HashMap<String, String> = &HashMap::new();
                    conn.open(fake_props).expect("ERROR: open error");
                    match conn.append(&p_c, &b_c) {
                        AppendRes::Success(_) => Ok(()),
                        default => Err(Box::new(AppendError::new(format!(
                            "append error: {}",
                            default
                        )))),
                    }
                }
                StorageType::RemoteFakeStore => {
                    let mut conn = FakeStoreServiceConnector::default();
                    let fake_props: &HashMap<String, String> = &HashMap::new();
                    conn.open(fake_props).expect("ERROR: open error");
                    match conn.append(&p_c, &b_c) {
                        AppendRes::Success(_) => Ok(()),
                        default => Err(Box::new(AppendError::new(format!(
                            "append error: {}",
                            default
                        )))),
                    }
                }
                StorageType::AzureStore => {
                    let mut conn = AzureConnector::default();
                    let fake_props: &HashMap<String, String> = &HashMap::new();
                    conn.open(fake_props).expect("ERROR: open error");
                    match conn.append(&p_c, &b_c) {
                        AppendRes::Success(_) => Ok(()),
                        default => Err(Box::new(AppendError::new(format!(
                            "append error: {}",
                            default
                        )))),
                    }
                }
            });
            clients.push(handle);
        }

        clients.into_iter().for_each(|h| {
            h.join()
                .expect("join failure")
                .expect("ERROR: append error");
        });

        // check correctness
        let res_buf = read_conn.read_all(test_url)?;
        let seal_res = read_conn.seal(test_url);
        assert!(seal_res.is_ok());

        read_conn.remove(test_url)?;

        let mut expect_buf = buf.clone();
        expect_buf.extend(&buf);
        expect_buf.extend(&buf);
        assert_eq!(expect_buf, res_buf);
        Ok(())
    }
}
