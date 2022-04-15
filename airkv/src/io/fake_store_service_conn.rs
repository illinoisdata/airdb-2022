use self::fakestore_service_connector::{
    fake_store_service_client::FakeStoreServiceClient, AppendRequest, CreateRequest,
    GetPropsRequest, GetSizeRequest, ReadAllRequest, ReadRangeRequest, RemoveRequest, SealRequest,
    WriteAllRequest,
};
use super::{file_utils::Range, storage_connector::StorageConnector};
use crate::{
    common::error::{GResult, GenericError, UnknownServerError},
    storage::{
        data_entry::AppendRes,
        segment::{BlockNum, SegSize, SegmentProps},
    },
};
use std::{cell::RefCell, collections::HashMap};
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use url::Url;

pub mod fakestore_service_connector {
    tonic::include_proto!("fakestoreservice");
}

type ClientType = FakeStoreServiceClient<Channel>;

///
/// FakeStoreServiceConnector provides API for clients to connect with the remote fake store service.
/// It is an alternative implementation for LocalStorageConnector.
///
/// The major difference is:
/// FakeStoreServiceConnector connects to a store server,
/// while LocalStorageConnector use a backend consumer thread to mimic the store server.
///
///
pub struct FakeStoreServiceConnector {
    client: Option<RefCell<ClientType>>,
    rt: Runtime,
}

impl Default for FakeStoreServiceConnector {
    fn default() -> Self {
        Self {
            client: None,
            rt: Runtime::new().expect("Failed to initialize tokio runtime"),
        }
    }
}

impl FakeStoreServiceConnector {
    async fn read_all_async(client: &mut ClientType, path: &Url) -> GResult<Vec<u8>> {
        let read_all_request = tonic::Request::new(ReadAllRequest {
            path: path.to_string(),
        });
        let read_all_response = client.read_all(read_all_request).await?;
        Ok(read_all_response.into_inner().content)
    }

    async fn read_range_async(
        client: &mut ClientType,
        path: &Url,
        range: &Range,
    ) -> GResult<Vec<u8>> {
        let read_range_request = tonic::Request::new(ReadRangeRequest {
            path: path.to_string(),
            offset: range.offset,
            length: range.length,
        });
        let read_range_response = client.read_range(read_range_request).await?;
        Ok(read_range_response.into_inner().content)
    }

    async fn get_size_async(client: &mut ClientType, path: &Url) -> GResult<u64> {
        let get_size_request = tonic::Request::new(GetSizeRequest {
            path: path.to_string(),
        });
        let get_size_response = client.get_size(get_size_request).await?;
        Ok(get_size_response.into_inner().size)
    }

    async fn create_async(client: &mut ClientType, path: &Url) -> GResult<()> {
        let create_request = tonic::Request::new(CreateRequest {
            path: path.to_string(),
        });
        let create_response = client.create(create_request).await?;
        if create_response.into_inner().status {
            Ok(())
        } else {
            Err(Box::new(UnknownServerError::new(format!(
                "unexpected response from server for create() for path {}",
                path
            ))) as GenericError)
        }
    }

    async fn append_async(client: &mut ClientType, path: &Url, buf: &[u8]) -> AppendRes<SegSize> {
        let append_request = tonic::Request::new(AppendRequest {
            path: path.to_string(),
            content: buf.to_vec(),
        });
        let append_response = client.append(append_request).await;
        match append_response {
            Ok(res) => {
                let append_res = res.into_inner();
                let status = append_res.status;
                let block_num = Some(append_res.blocknum as BlockNum);
                AppendRes::<SegSize>::append_res_from_code(status, block_num)
            }
            Err(err) => {
                panic!("append request encounters error: {}", err)
            }
        }
    }

    async fn write_all_async(client: &mut ClientType, path: &Url, buf: &[u8]) -> GResult<()> {
        let write_all_request = tonic::Request::new(WriteAllRequest {
            path: path.to_string(),
            content: buf.to_vec(),
        });
        let write_all_response = client.write_all(write_all_request).await?;
        if write_all_response.into_inner().status {
            Ok(())
        } else {
            Err(Box::new(UnknownServerError::new(
                "unexpected response from server for write_all()".to_owned(),
            )) as GenericError)
        }
    }

    async fn remove_async(client: &mut ClientType, path: &Url) -> GResult<()> {
        let remove_request = tonic::Request::new(RemoveRequest {
            path: path.to_string(),
        });
        let remove_response = client.remove(remove_request).await?;
        if remove_response.into_inner().status {
            Ok(())
        } else {
            Err(Box::new(UnknownServerError::new(
                "unexpected response from server for remove()".to_owned(),
            )) as GenericError)
        }
    }

    async fn seal_async(client: &mut ClientType, path: &Url) -> GResult<()> {
        let seal_request = tonic::Request::new(SealRequest {
            path: path.to_string(),
        });
        let seal_response = client.seal(seal_request).await?;
        if seal_response.into_inner().status {
            Ok(())
        } else {
            Err(Box::new(UnknownServerError::new(
                "unexpected response from server for remove()".to_owned(),
            )) as GenericError)
        }
    }

    async fn get_props_async(client: &mut ClientType, path: &Url) -> GResult<SegmentProps> {
        let get_props_request = tonic::Request::new(GetPropsRequest {
            path: path.to_string(),
        });
        let get_props_response = client.get_props(get_props_request).await?.into_inner();
        Ok(SegmentProps::new(
            get_props_response.seglen,
            get_props_response.blocknum as BlockNum,
            get_props_response.sealed,
        ))
    }
}

impl StorageConnector for FakeStoreServiceConnector {
    ///
    /// create the client which will connect to the server
    ///  
    /// # Arguments
    ///
    /// * `props` - necessary properties to initialize and  open the connection  
    ///
    fn open(&mut self, _props: &HashMap<String, String>) -> GResult<()> {
        self.rt.block_on(async {
            // TODO: get connection url from props
            let channel = Channel::from_static("http://[::1]:50051").connect().await?;
            self.client = Some(RefCell::new(FakeStoreServiceClient::new(channel)));
            Ok(())
        })
    }

    fn close(&mut self) -> GResult<()> {
        self.rt.block_on(async {
            Ok(())
        })
    }

    fn read_all(&self, path: &Url) -> GResult<Vec<u8>> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::read_all_async(
                &mut client.borrow_mut(),
                path,
            )),
            None => panic!("The Client is None"),
        }
    }

    fn read_range(&self, path: &Url, range: &Range) -> GResult<Vec<u8>> {
        match self.client {
            Some(ref client) => self
                .rt
                .block_on(FakeStoreServiceConnector::read_range_async(
                    &mut client.borrow_mut(),
                    path,
                    range,
                )),
            None => panic!("The Client is None"),
        }
    }

    fn get_size(&self, path: &Url) -> GResult<u64> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::get_size_async(
                &mut client.borrow_mut(),
                path,
            )),
            None => panic!("The Client is None"),
        }
    }

    fn create(&self, path: &Url) -> GResult<()> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::create_async(
                &mut client.borrow_mut(),
                path,
            )),
            None => panic!("The Client is None"),
        }
    }

    fn append(&self, path: &Url, buf: &[u8]) -> AppendRes<SegSize> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::append_async(
                &mut client.borrow_mut(),
                path,
                buf,
            )),
            None => panic!("The Client is None"),
        }
    }

    fn write_all(&self, path: &Url, buf: &[u8]) -> GResult<()> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::write_all_async(
                &mut client.borrow_mut(),
                path,
                buf,
            )),
            None => panic!("The Client is None"),
        }
    }

    fn remove(&self, path: &Url) -> GResult<()> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::remove_async(
                &mut client.borrow_mut(),
                path,
            )),
            None => panic!("The Client is None"),
        }
    }

    fn seal(&self, path: &Url) -> GResult<()> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::seal_async(
                &mut client.borrow_mut(),
                path,
            )),
            None => panic!("The Client is None"),
        }
    }

    fn get_props(&self, path: &Url) -> GResult<SegmentProps> {
        match self.client {
            Some(ref client) => self.rt.block_on(FakeStoreServiceConnector::get_props_async(
                &mut client.borrow_mut(),
                path,
            )),
            None => panic!("The Client is None"),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{
        collections::HashMap,
        thread::{self, JoinHandle},
    };

    use rand::Rng;
    use tempfile::TempDir;
    use url::Url;

    use crate::{
        common::error::{AppendError, GResult},
        io::{
            fake_store_service_conn::FakeStoreServiceConnector,
            file_utils::{FileUtil, Range, UrlUtil},
            storage_connector::StorageConnector,
        },
        storage::data_entry::AppendRes,
    };

    #[test]
    fn create_remove_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        println!("ready to open");
        first_conn.open(fake_props)?;
        println!("open success");
        first_conn.create(test_url)?;
        println!("create success");
        assert!(FileUtil::exist(test_url)?);
        assert_eq!(first_conn.get_size(test_url)?, 0);
        first_conn.seal(test_url)?;
        println!("seal success");
        let content =
            "The first sentence in a paragraph is sometimes called the key or topic sentence \
    because it gives us the key to what the paragraph will be about.";
        let buf = String::from(content).into_bytes();
        let res = first_conn.append(test_url, &buf);
        assert!(!res.is_success());
        match res {
            AppendRes::AppendToSealedFailure => {},
            _ => panic!("unexpected append response"),
        }
        first_conn.remove(test_url)?;
        assert!(!FileUtil::exist(test_url)?);
        Ok(())
    }

    #[test]
    fn write_read_all_zero_ok() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        // write some data
        let test_data = [0u8; 256];
        first_conn.write_all(test_url, &test_data)?;

        // read and check
        let test_data_reread = first_conn.read_all(test_url)?;
        assert_eq!(
            &test_data[..],
            &test_data_reread[..],
            "Reread data not matched with original one"
        );
        Ok(())
    }

    #[test]
    fn write_read_all_random_ok() -> GResult<()> {
        // write some data
        let temp_dir = TempDir::new()?;
        let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let mut test_data = [0u8; 256];
        rand::thread_rng().fill(&mut test_data[..]);
        first_conn.write_all(test_url, &test_data)?;

        // read and check
        let test_data_reread = first_conn.read_all(test_url)?;
        assert_eq!(
            &test_data[..],
            &test_data_reread[..],
            "Reread data not matched with original one"
        );
        Ok(())
    }

    #[test]
    fn write_twice_read_all_random_ok() -> GResult<()> {
        // write some data
        let temp_dir = TempDir::new()?;
        let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let test_data_old = [1u8; 256];
        first_conn.write_all(test_url, &test_data_old)?;

        // write more, this should completely replace previous result
        let test_data_actual = [2u8; 128];
        first_conn.write_all(test_url, &test_data_actual)?;

        // read and check
        let test_data_reread = first_conn.read_all(test_url)?;
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
    #[test]
    fn write_read_range_random_ok() -> GResult<()> {
        // write some data
        let temp_dir = TempDir::new()?;
        let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

        let mut first_conn = FakeStoreServiceConnector::default();
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
        Ok(())
    }

    #[test]
    /* Test single-thread append */
    fn single_thread_append_test() -> GResult<()> {
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        let temp_dir = TempDir::new()?;
        let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);
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
        Ok(())
    }

    #[test]
    /* Test multi-thread append */
    fn multi_thread_append_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let test_url = &(UrlUtil::url_from_path(temp_dir.path().join("test-file.bin").as_path())?);

        let content =
            "The first sentence in a paragraph is sometimes called the key or topic sentence \
        because it gives us the key to what the paragraph will be about.";
        let buf = String::from(content).into_bytes();

        //create segment
        let mut read_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        read_conn.open(fake_props)?;
        read_conn.create(test_url)?;

        let append_closure = &(|p: Url, b: Vec<u8>| -> GResult<()> {
            let mut first_conn: FakeStoreServiceConnector = FakeStoreServiceConnector::default();
            let fake_props: &HashMap<String, String> = &HashMap::new();
            first_conn.open(fake_props)?;
            match first_conn.append(&p, &b) {
                AppendRes::Success(_) => Ok(()),
                default => Err(Box::new(AppendError::new(format!(
                    "append error: {}",
                    default
                )))),
            }
        });

        let mut clients: Vec<JoinHandle<_>> = vec![];
        //launch three threads to append data simultaneously
        //each thread simulates a client
        for _i in 1..=3 {
            let p_c = test_url.clone();
            let b_c = buf.clone();
            let handle = thread::spawn(move || append_closure(p_c, b_c));
            clients.push(handle);
        }

        clients.into_iter().for_each(|h| {
            h.join().expect("join failure");
        });

        // check correctness
        let res_buf = read_conn.read_all(test_url)?;
        let seal_res = read_conn.seal(test_url);
        assert!(seal_res.is_ok());

        let mut expect_buf = buf.clone();
        expect_buf.extend(&buf);
        expect_buf.extend(&buf);
        assert_eq!(expect_buf, res_buf);
        Ok(())
    }
}
