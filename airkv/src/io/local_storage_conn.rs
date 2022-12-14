use super::local_append_store::Message;
use super::file_utils::Range;
use super::storage_connector::StorageConnector;
use crate::common::error::GResult;
use crate::io::local_append_store::FakeAppendStore;
use crate::io::file_utils::FileUtil;
use crate::storage::data_entry::AppendRes;
use crate::storage::segment::{SegSize, SegmentProps};

use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::sync::Mutex;
use url::Url;

/*
 FAKE_STORE is the unique instance of FakeAppendStore and may be shared by multiple clients in mock tests.

 FakeAppendStore is a singleton shared by multiple clients. It controls the consumer thread to keep flushing the append data from the message-passing channel receiver side. The constructor of FakeAppendStore triggers the consumer thread.
*/

lazy_static! {
    pub static ref FAKE_STORE: Mutex<FakeAppendStore> = Mutex::new(FakeAppendStore::default());
}

/*
  Design:
    LocalStorageConnector is the connector to a fake storage based on the local file system. It is supposed to mock the cloud storage properties(especially the consistency of Azure Append Blob) using local files. Each client is supposed to create its own instance of LocalStorageConnector and call open() of its instance to get the access to the fake storage.

    Note:
    1. LocalStorageConnector.open() is expected to be called once for each client, although there is no side-effect if it is called twice or more.
    2. LocalStorageConnector.open() will get the sender side of the message-passing channel for its client. LocalStorageConnector.LocalStorageConnector.append() called by multiple clients are orchestrated by the message-passing channel which has multiple senders (multi-producer) and one receiver(single-consumer). Each client has one instance of LocalStorageConnector. Therefore,each client holds one sender and many threads(clients) can send simultaneously to one receiver.

    Usage Example(for each client):

    // the client creates its own instance of LocalStorageConnector
    let local_connector = LocalStorageConnector::default();
    local_connector.open(props_hashmap);
    // do regular access operations
    local_connector.append(path_to_append, data_to_append);
    ...

    Another example can refer to the test of multi_thread_append_test().
*/
#[derive(Default)]
pub struct LocalStorageConnector {
    writer: Option<Sender<Message>>,
}

#[allow(unused)]
impl StorageConnector for LocalStorageConnector {
    // open connection
    fn open(&mut self, _props: &HashMap<String, String>) -> GResult<()> {
        // only the first thread which calls this method can init the FakeAppendStore and therefore create the backend consumer thread. This mechanism of being called once globally is ensured by START.call_once().
        self.writer = Some(FAKE_STORE.lock().unwrap().get_sender());
        Ok(())
    }

    // close connection and clear states
    fn close(&mut self) -> GResult<()> {
        self.writer = None;
        Ok(())
    }

    // create empty segment at a target path
    fn create(&self, path: &Url) -> GResult<()> {
        FileUtil::create_file(path)
    }

    // read whole segment specified in path
    fn read_all(&self, path: &Url) -> GResult<Vec<u8>> {
        FileUtil::read_all_from_path(path)
    }

    // read range starting at offset for length bytes
    fn read_range(&self, path: &Url, range: &Range) -> GResult<Vec<u8>> {
        FileUtil::read_range_from_path(path, range)
    }

    // get the current length of the target segment
    fn get_size(&self, path: &Url) -> GResult<u64> {
        FileUtil::file_size(path)
    }

    // append the byte array to the end of a target segment
    fn append(&self, path: &Url, buf: &[u8]) -> AppendRes<SegSize> {
        // match self
        //     .writer
        //     .as_ref()
        //     .unwrap()
        //     .send(Message::new(path.clone(), buf.to_vec()))
        // {
        //     Ok(_) => todo!(),
        //     Err(_) => AppendRes::UnknownFailure,
        // }
        // Ok(())
        todo!()
    }

    // write whole byte array to a segment
    fn write_all(&self, path: &Url, buf: &[u8]) -> GResult<()> {
        FileUtil::write_all_to_path(path, buf)
    }

    // remove the whole segment
    fn remove(&self, path: &Url) -> GResult<()> {
        FileUtil::delete_file(path)
    }

    fn seal(&self, path: &Url) -> GResult<()> {
        todo!()
    }

    fn get_props(&self, path: &Url) -> GResult<SegmentProps> {
        todo!()
    }
}

//TODO: enable this test
// #[cfg(test)]
// mod tests {
//     use crate::{common::error::AppendError, io::file_utils::UrlUtil};

//     use super::*;
//     use rand;
//     use rand::Rng;
//     use std::{thread, time};
//     use tempfile::TempDir;

//     #[test]
//     fn create_remove_test() -> GResult<()> {
//         let temp_dir = TempDir::new()?;
//         let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

//         let mut first_conn = LocalStorageConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         first_conn.open(fake_props)?;
//         match first_conn.create(test_url) {
//             Ok(x) => x,
//             Err(e) => panic!("{}", e),
//         }
//         assert!(FileUtil::exist(test_url)?);
//         assert_eq!(first_conn.get_size(test_url)?, 0);
//         first_conn.remove(test_url)?;
//         assert!(!FileUtil::exist(test_url)?);
//         Ok(())
//     }

//     #[test]
//     fn write_read_all_zero_ok() -> GResult<()> {
//         let temp_dir = TempDir::new()?;
//         let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

//         let mut first_conn = LocalStorageConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         first_conn.open(fake_props)?;
//         // write some data
//         let test_data = [0u8; 256];
//         first_conn.write_all(test_url, &test_data)?;

//         // read and check
//         let test_data_reread = first_conn.read_all(test_url)?;
//         assert_eq!(
//             &test_data[..],
//             &test_data_reread[..],
//             "Reread data not matched with original one"
//         );
//         Ok(())
//     }

//     #[test]
//     fn write_read_all_random_ok() -> GResult<()> {
//         // write some data
//         let temp_dir = TempDir::new()?;
//         let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

//         let mut first_conn = LocalStorageConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         first_conn.open(fake_props)?;
//         let mut test_data = [0u8; 256];
//         rand::thread_rng().fill(&mut test_data[..]);
//         first_conn.write_all(test_url, &test_data)?;

//         // read and check
//         let test_data_reread = first_conn.read_all(test_url)?;
//         assert_eq!(
//             &test_data[..],
//             &test_data_reread[..],
//             "Reread data not matched with original one"
//         );
//         Ok(())
//     }

//     #[test]
//     fn write_twice_read_all_random_ok() -> GResult<()> {
//         // write some data
//         let temp_dir = TempDir::new()?;
//         let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

//         let mut first_conn = LocalStorageConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         first_conn.open(fake_props)?;
//         let test_data_old = [1u8; 256];
//         first_conn.write_all(test_url, &test_data_old)?;

//         // write more, this should completely replace previous result
//         let test_data_actual = [2u8; 128];
//         first_conn.write_all(test_url, &test_data_actual)?;

//         // read and check
//         let test_data_reread = first_conn.read_all(test_url)?;
//         assert_ne!(
//             &test_data_old[..],
//             &test_data_reread[..],
//             "Old data should be removed"
//         );
//         assert_eq!(
//             &test_data_actual[..],
//             &test_data_reread[..],
//             "Reread data not matched with original one, possibly containing old data"
//         );
//         Ok(())
//     }

//     #[test]
//     fn write_read_range_random_ok() -> GResult<()> {
//         // write some data
//         let temp_dir = TempDir::new()?;
//         let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

//         let mut first_conn = LocalStorageConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         first_conn.open(fake_props)?;
//         let mut test_data = [0u8; 256];
//         rand::thread_rng().fill(&mut test_data[..]);
//         first_conn.write_all(test_url, &test_data)?;

//         // test 100 random ranges
//         let mut rng = rand::thread_rng();
//         for _ in 0..100 {
//             let offset = rng.gen_range(0..test_data.len() - 1);
//             let length = rng.gen_range(1..test_data.len() - offset);
//             let test_data_range =
//                 first_conn.read_range(test_url, &Range::new_usize(offset, length))?;
//             let test_data_expected = &test_data[offset..offset + length];
//             assert_eq!(
//                 test_data_expected,
//                 &test_data_range[..],
//                 "Reread data not matched with original one"
//             );
//         }
//         Ok(())
//     }

//     #[test]
//     /* Test single-thread append */
//     fn single_thread_append_test() -> GResult<()> {
//         let mut first_conn = LocalStorageConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         first_conn.open(fake_props)?;
//         let temp_dir = TempDir::new()?;
//         let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

//         let content =
//             "The first sentence in a paragraph is sometimes called the key or topic sentence \
//         because it gives us the key to what the paragraph will be about.";
//         let buf = String::from(content).into_bytes();
//         let res = first_conn.append(test_url, &buf);
//         assert!(res.is_success());
//         let res1 = first_conn.append(test_url, &buf);
//         assert!(res1.is_success());
//         let res2 = first_conn.append(test_url, &buf);
//         assert!(res2.is_success());
//         // sleep to wait the concumer thread to flush data to the disk
//         let sleep_period = time::Duration::from_secs(3);
//         thread::sleep(sleep_period);
//         // check correctness
//         let res_buf = first_conn.read_all(test_url)?;
//         let mut expect_buf = buf.clone();
//         expect_buf.extend(&buf);
//         expect_buf.extend(&buf);
//         assert_eq!(expect_buf, res_buf);
//         Ok(())
//     }

//     #[test]
//     /* Test multi-thread append */
//     fn multi_thread_append_test() -> GResult<()> {
//         let temp_dir = TempDir::new()?;
//         let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

//         let content =
//             "The first sentence in a paragraph is sometimes called the key or topic sentence \
//         because it gives us the key to what the paragraph will be about.";
//         let buf = String::from(content).into_bytes();

//         let append_closure = &(|p: Url, b: Vec<u8>| -> GResult<()> {
//             let mut first_conn: LocalStorageConnector = LocalStorageConnector::default();
//             let fake_props: &HashMap<String, String> = &HashMap::new();
//             first_conn.open(fake_props)?;
//             match first_conn.append(&p, &b) {
//                 AppendRes::Success(_) => Ok(()),
//                 default => Err(Box::new(AppendError::new(format!(
//                     "append error: {}",
//                     default
//                 )))),
//             }
//         });

//         //launch three threads to append data simultaneously
//         //each thread simulates a client
//         for _i in 1..=3 {
//             let p_c = test_url.clone();
//             let b_c = buf.clone();
//             thread::spawn(move || append_closure(p_c, b_c));
//         }

//         // sleep to wait the concumer thread to flush data to the disk
//         let sleep_period = time::Duration::from_secs(3);
//         thread::sleep(sleep_period);

//         // check correctness
//         let mut read_conn = LocalStorageConnector::default();
//         let fake_props: &HashMap<String, String> = &HashMap::new();
//         read_conn.open(fake_props)?;
//         let res_buf = read_conn.read_all(test_url)?;

//         let mut expect_buf = buf.clone();
//         expect_buf.extend(&buf);
//         expect_buf.extend(&buf);
//         assert_eq!(expect_buf, res_buf);
//         Ok(())
//     }
// }
