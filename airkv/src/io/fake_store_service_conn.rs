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
