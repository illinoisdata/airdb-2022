use std::{collections::HashMap, sync::Arc, thread};

use azure_core::{headers::get_from_headers, HttpError};
use azure_storage::clients::{AsStorageClient, StorageAccountClient, StorageClient};
use azure_storage_blobs::prelude::{AsBlobClient, AsContainerClient, BlobClient, ContainerClient};

use itertools::Itertools;
use prost::bytes::Bytes;
use serde_derive::Deserialize;
use tokio::runtime::Runtime;
use tonic::codegen::http::{self};
use url::Url;

use crate::{
    common::error::{GResult, InvalidAzureStorageUrl, MissingAzureAuthetication},
    storage::{
        data_entry::AppendRes,
        segment::{SegSize, SegmentProps},
    },
};

use super::{file_utils::Range, storage_connector::StorageConnector};

#[derive(Debug, Deserialize)]
#[serde(rename = "Error")]
pub struct AzureError {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    _message: String,
}

impl AzureError {
    fn from_str(str: &str) -> AzureError {
        let error_str = AzureUtil::remove_xml_bom(str);
        serde_xml_rs::de::from_reader(error_str.as_bytes())
            .expect("ERROR: failed to parse error response")
    }
}

pub struct AzureConnector {
    client: Option<Arc<StorageClient>>,
    run_time: Runtime,
}

impl Default for AzureConnector {
    fn default() -> Self {
        let account = std::env::var("AZURE_ACCOUNTNAME")
            .map_err(|_| MissingAzureAuthetication::boxed("Set env variable AZURE_ACCOUNTNAME")).expect("failed to get environment variable AZURE_ACCOUNTNAME");

        let key = std::env::var("AZURE_ACCOUNTKEY")
            .map_err(|_| MissingAzureAuthetication::boxed("Set env variable AZURE_ACCOUNTKEY")).expect("failed to get environment variable AZURE_ACCOUNTKEY");

        let http_client = azure_core::new_http_client();
        Self {
            client: Some(
                StorageAccountClient::new_access_key(http_client, &account, &key)
                    .as_storage_client(),
            ),
            run_time: Runtime::new().expect("Failed to initialize runtime"),
        }
    }
}

impl StorageConnector for AzureConnector {
    fn open(&mut self, _props: &HashMap<String, String>) -> GResult<()> {
        Ok(())
    }

    fn close(&mut self) -> GResult<()> {
        //TODO: release resources
        Ok(())
    }

    fn read_all(&self, path: &Url) -> GResult<Vec<u8>> {
        self.run_time.block_on(self.read_all_async(path))
    }

    fn read_range(&self, path: &Url, range: &Range) -> GResult<Vec<u8>> {
        // self.run_time.block_on(self.read_range_async(path, range))
        let response_res = self.run_time.block_on(self.read_range_async(path, range));
        match response_res {
            Ok(response) => {
                // println!("INFO: read-range response: {:?}", response);
                Ok(response)
            }
            Err(err) => {
                println!(
                    "ERROR: {:?} read range error for path {} + error: {:?}",
                    thread::current().id(),
                    path,
                    err
                );
                Err(err)
            }
        }
    }

    fn get_size(&self, path: &Url) -> GResult<u64> {
        self.run_time.block_on(self.get_size_async(path))
    }

    fn get_props(&self, path: &Url) -> GResult<SegmentProps> {
        self.run_time.block_on(self.get_props_async(path))
    }

    fn seal(&self, path: &Url) -> GResult<()> {
        self.run_time.block_on(self.seal_async(path))
    }

    fn create(&self, path: &Url) -> GResult<()> {
        self.run_time.block_on(self.create_async(path))
    }

    fn append(&self, path: &Url, buf: &[u8]) -> AppendRes<SegSize> {
        self.run_time.block_on(self.append_async(path, buf))
    }

    fn write_all(&self, path: &Url, buf: &[u8]) -> GResult<()> {
        self.run_time.block_on(self.write_all_async(path, buf))
    }

    fn remove(&self, path: &Url) -> GResult<()> {
        self.run_time.block_on(self.remove_async(path))
    }
}

impl AzureConnector {
    async fn read_all_async(&self, url: &Url) -> GResult<Vec<u8>> {
        let blob_response = self.blob_client_from_url(url).get().execute().await?;
        Ok(blob_response.data.to_vec())
    }

    async fn write_all_async(&self, url: &Url, buf: &[u8]) -> GResult<()> {
        // TODO: avoid copy?
        // TODO: support append blob
        self.blob_client_from_url(url)
            .put_block_blob(Bytes::copy_from_slice(buf))
            .execute()
            .await?;

        // log::debug!("{:?}", response);
        Ok(())
    }

    async fn read_range_async(&self, path: &Url, range: &Range) -> GResult<Vec<u8>> {
        let storage_client = self.get_storage_client();
        let (url, _, _) = self.get_request_info(storage_client, path);
        // println!("INFO: read-range url: {}", url);

        let (request, _url) = storage_client
            .prepare_request(
                url.as_str(),
                &http::Method::GET,
                &|mut request| {
                    request = request.header("x-ms-range", &format!("{}", range));
                    if !range.reach_seg_end() && range.length < 1024 * 1024 * 4 {
                        request = request.header("x-ms-range-get-content-crc64", "true");
                    }
                    request
                },
                None,
            )
            .expect("ERROR: failed to generate read-range request");

        // println!("INFO: read-range request: {:?}", request);

        let response = storage_client
            .http_client()
            .execute_request_check_status(request, http::StatusCode::PARTIAL_CONTENT)
            .await?;

        // println!("INFO: read-range response: {:?}", response);
        Ok(response.into_body().to_vec())
    }

    async fn get_size_async(&self, path: &Url) -> GResult<u64> {
        let storage_client = self.get_storage_client();
        let (url, _, _) = self.get_request_info(storage_client, path);
        // println!("INFO: get-size url: {}", url);

        let (request, _url) = storage_client
            .prepare_request(url.as_str(), &http::Method::HEAD, &|request| request, None)
            .expect("ERROR: failed to generate get-size request");

        // println!("INFO: get-size request: {:?}", request);

        let response = storage_client
            .http_client()
            .execute_request_check_status(request, http::StatusCode::OK)
            .await?;

        // println!("INFO: get-size {:?}", response);

        let header = response.headers();
        let len = header
            .get("Content-Length")
            .unwrap_or_else(|| panic!("Content-Length not found, current header is {:?}", header))
            .to_str()?
            .parse::<u64>()?;
        Ok(len)
    }

    async fn get_props_async(&self, path: &Url) -> GResult<SegmentProps> {
        let storage_client = self.get_storage_client();
        let (url, _, _) = self.get_request_info(storage_client, path);
        // println!("INFO: get-props url: {}", url);

        let (request, _url) = storage_client
            .prepare_request(url.as_str(), &http::Method::HEAD, &|request| request, None)
            .expect("ERROR: failed to generate get-props request");

        // println!("INFO: get-props request: {:?}", request);

        let response = storage_client
            .http_client()
            .execute_request_check_status(request, http::StatusCode::OK)
            .await?;

        // println!("INFO: get props {:?}", response);

        let header = response.headers();
        let len = header
            .get("Content-Length")
            .unwrap_or_else(|| panic!("Content-Length not found, current header is {:?}", header))
            .to_str()?
            .parse::<u64>()?;
        let block_num = header
            .get("x-ms-blob-committed-block-count")
            .unwrap_or_else(|| {
                panic!(
                    "x-ms-blob-committed-block-count not found, current header is {:?}",
                    header
                )
            })
            .to_str()?
            .parse::<u16>()?;

        let seal_head = header.get("x-ms-blob-sealed");
        let is_sealed = match seal_head {
            Some(content) => content.to_str()?.parse::<bool>()?,
            None => false,
        };

        Ok(SegmentProps::new(len, block_num, is_sealed))
    }

    async fn seal_async(&self, path: &Url) -> GResult<()> {
        let storage_client = self.get_storage_client();
        let (mut url, _, _) = self.get_request_info(storage_client, path);
        url.query_pairs_mut().append_pair("comp", "seal");
        // println!("INFO: seal url: {}", url);

        let (request, _url) = storage_client
            .prepare_request(url.as_str(), &http::Method::PUT, &|request| request, None)
            .expect("ERROR: failed to generate seal request");

        // println!("INFO: seal request: {:?}", request);

        let response = storage_client
            .http_client()
            .execute_request_check_status(request, http::StatusCode::CREATED)
            .await;
        match response {
            Ok(_) => {
                // println!("INFO: seal response: {:?}", response);
                Ok(())
            }
            Err(err) => {
                println!("ERROR: error response for seal request: {:?}", err);
                Err(Box::new(err))
            }
        }
    }

    async fn create_async(&self, path: &Url) -> GResult<()> {
        let (container_name, blob_option) = AzureConnector::parse_container_or_blob(path)?;

        // find out whether to create a container or a blob
        match blob_option {
            Some(blob_name) => {
                self.blob_client(container_name, blob_name)
                    .put_append_blob()
                    .execute()
                    .await?;
            }
            None => {
                self.container_client(container_name)
                    .create()
                    .execute()
                    .await?;
            }
        }
        Ok(())
    }

    async fn remove_async(&self, path: &Url) -> GResult<()> {
        let (container_name, blob_option) = AzureConnector::parse_container_or_blob(path)?;
        // find out whether to delete a container or a blob
        match blob_option {
            Some(blob_name) => {
                self.blob_client(container_name, blob_name)
                    .delete()
                    .execute()
                    .await?;
            }
            None => {
                self.container_client(container_name)
                    .delete()
                    .execute()
                    .await?;

                // match self
                //     .container_client(container_name)
                //     .delete()
                //     .execute()
                //     .await
                // {
                //     Ok(_) => {},
                //     Err(err) => {
                //         println!("ERROR: remove container error {:?}", err);
                //         panic!("ERROR: remove container error");
                //     },
                // }
            }
        }
        Ok(())
    }

    async fn append_async(&self, path: &Url, buf: &[u8]) -> AppendRes<SegSize> {
        //TODO: maintain blob_client for each blob in cache
        let storage_client = self.get_storage_client();
        let (mut url, _, _) = self.get_request_info(storage_client, path);
        url.query_pairs_mut().append_pair("comp", "appendblock");
        // println!("INFO: append url: {}", url);

        let (request, _url) = storage_client
            .prepare_request(
                url.as_str(),
                &http::Method::PUT,
                &|request| request,
                Some(Bytes::copy_from_slice(buf)),
            )
            .expect("ERROR: failed to generate append request");

        // println!("INFO: append request: {:?}", request);

        let response = storage_client
            .http_client()
            .execute_request_check_status(request, http::StatusCode::CREATED)
            .await;
        match response {
            Ok(res) => {
                // println!("INFO: append response: {:?}", res);
                let block_num: u16 =
                    get_from_headers(res.headers(), "x-ms-blob-committed-block-count")
                        .expect("ERROR: parse header error");
                AppendRes::<SegSize>::Success(block_num)
            }
            Err(err) => {
                // println!("INFO: error response for append request: {:?}", err);
                match err {
                    HttpError::StatusCode { status: _, body } => {
                        let azure_error = AzureError::from_str(body.as_str());
                        AppendRes::append_res_from_azure_error(azure_error.code.as_str())
                    }
                    _ => AppendRes::<SegSize>::UnknownFailure,
                }
            }
        }
    }

    fn get_storage_client(&self) -> &Arc<StorageClient> {
        self.client
            .as_ref()
            .expect("ERROR: azure client has not been initialized")
    }

    fn get_request_info(&self, client: &Arc<StorageClient>, path: &Url) -> (Url, String, String) {
        let (container_name, blob_name) =
            AzureConnector::parse_blob(path).expect("parse url failed");
        let url = client
            .blob_url_with_segments(
                Some(container_name.as_str())
                    .into_iter()
                    .chain(blob_name.split('/').into_iter()),
            )
            .expect("ERROR: failed to generate url");
        (url, container_name, blob_name)
    }

    fn blob_client_from_url(&self, url: &Url) -> Arc<BlobClient> {
        let (container_name, blob_name) =
            AzureConnector::parse_blob(url).expect("parse url failed");
        self.blob_client(container_name, blob_name)
    }

    fn blob_client(&self, container_name: String, blob_name: String) -> Arc<BlobClient> {
        match self.client {
            Some(ref c) => c
                .as_container_client(container_name)
                .as_blob_client(&blob_name),
            None => panic!("ERROR: the azure storageaccountclient is none"),
        }
    }

    fn container_client(&self, container_name: String) -> Arc<ContainerClient> {
        match self.client {
            Some(ref c) => c.as_container_client(container_name),
            None => panic!("ERROR: the azure storageaccountclient is none"),
        }
    }

    fn parse_blob(url: &Url) -> GResult<(String, String)> {
        // container name, blob path
        let mut path_segments = url
            .path_segments()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Failed to segment url"))?;
        let container = path_segments
            .next()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Require container name"))?;
        let blob_path = Itertools::intersperse(path_segments, "/").collect();
        Ok((container.to_string(), blob_path))
    }

    fn parse_container_or_blob(url: &Url) -> GResult<(String, Option<String>)> {
        // container name, blob path
        let mut path_segments = url
            .path_segments()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Failed to segment url"))?;
        let container = path_segments
            .next()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Require container name"))?;
        // blob
        match path_segments.next() {
            Some(blob) => {
                if blob.is_empty() {
                    Ok((container.to_string(), None))
                } else {
                    Ok((container.to_string(), Some(blob.to_string())))
                }
            }
            None => Ok((container.to_string(), None)),
        }
    }
}

pub struct AzureUtil {}

impl AzureUtil {
    fn remove_xml_bom(origin_str: &str) -> &str {
        // skip BOM
        let start_index = origin_str.find("<?xml");
        match start_index {
            Some(idx) => &origin_str[idx..],
            None => origin_str,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use serial_test::serial;
    use url::Url;

    use crate::{
        common::error::{GResult, InvalidAzureStorageUrl},
        io::storage_connector::StorageConnector,
        storage::data_entry::AppendRes,
    };

    use super::{AzureConnector, AzureError};

    #[test]
    fn test_azure_error_parser() -> GResult<()> {
        let content = "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>BlobIsSealed</Code><Message>The blob is sealed and its contents cannot be modified.\nRequestId:59236d3a-c01e-0024-5466-55165e000000\nTime:2022-04-21T10:01:50.3399540Z</Message></Error>";
        // skip BOM
        let azure_error: AzureError = AzureError::from_str(content);
        println!("INFO: {:?}", azure_error);
        match AppendRes::append_res_from_azure_error(azure_error.code.as_str()) {
            AppendRes::AppendToSealedFailure => {
                println!("parsing succeed")
            }
            _ => panic!("AppendToSealedFailure Not Found"),
        }

        Ok(())
    }

    #[test]
    fn test_url_parser() -> GResult<()> {
        let blob_url = Url::parse(&format!("az:///{}/{}", "airkv", "test_blob"))?;
        let container_url = Url::parse(&format!("az:///{}", "airkv"))?;
        // container name, blob path
        let mut path_segments = blob_url
            .path_segments()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Failed to segment url"))?;
        let container = path_segments
            .next()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Require container name"))?;
        // let blob_path: String = Itertools::intersperse(path_segments, "/").collect();
        let blob_path = path_segments
            .next()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Require blob name"))?;
        assert_eq!(container, "airkv");
        assert_eq!(blob_path, "test_blob");
        println!("blob path: {}", blob_path);

        let mut path_segments_1 = container_url
            .path_segments()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Failed to segment url"))?;
        let container_1 = path_segments_1
            .next()
            .ok_or_else(|| InvalidAzureStorageUrl::new("Require container name"))?;
        // let blob_path_1: String = Itertools::intersperse(path_segments_1, "/").collect();
        let blob_path_1 = path_segments_1.next();

        assert_eq!(container_1, "airkv");
        assert!(blob_path_1.is_none());

        Ok(())
    }

    #[test]
    #[serial]
    fn test_container_deletion() -> GResult<()> {
        let test_path = format!("az:///{}", "deletiontest");
        let home_url = Url::parse(&test_path)?;
        let blob_path = format!("az:///{}/{}", "deletiontest", "container_test_blob");
        let blob_url = Url::parse(&blob_path)?;
        let mut util_conn = AzureConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        util_conn.open(fake_props)?;
        //create container
        util_conn.create(&home_url)?;
        //create blob
        util_conn.create(&blob_url)?;
        //delete container
        util_conn.remove(&home_url)?;
        Ok(())
    }
}
