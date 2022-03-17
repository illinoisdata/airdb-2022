use crate::common::error::ResultUtil;
use crate::io::fake_append_store::Message;
use crate::io::file_utils::{FileUtil, UrlUtil};
use crate::io::local_storage_conn::FAKE_STORE;
use crate::common::error::GResult;
use super::file_utils::Range;

use fakestoreservice::fake_store_service_server::FakeStoreService;
use fakestoreservice::*;
use tonic::{Request, Response, Status};

pub mod fakestoreservice {
    tonic::include_proto!("fakestoreservice");
}

// defining a struct for our service
#[derive(Default)]
pub struct ServiceImpl {}

#[tonic::async_trait]
impl FakeStoreService for ServiceImpl {
    async fn open(&self, _request: Request<OpenRequest>) -> Result<Response<OpenResponse>, Status> {
        Ok(Response::new(OpenResponse { status: true }))
    }

    async fn close(
        &self,
        _request: Request<CloseRequest>,
    ) -> Result<Response<CloseResponse>, Status> {
        Ok(Response::new(CloseResponse { status: true }))
    }

    async fn create(
        &self,
        _request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let request = _request.into_inner();
        let path = &request.path;
        let url = UrlUtil::url_from_string(path).unwrap();
        ResultUtil::transfer_service_repsonse(
            FileUtil::create_file(&url),
            |_x| CreateResponse { status: true },
            "create path",
            &format!("create {}", path),
        )
    }

    async fn remove(
        &self,
        _request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        let request = _request.into_inner();
        let path = &request.path;
        let url = UrlUtil::url_from_string(path).unwrap();
        ResultUtil::transfer_service_repsonse(
            FileUtil::delete_file(&url),
            |_x| RemoveResponse { status: true },
            "remove path",
            &format!("remove {}", path),
        )
    }

    async fn get_size(
        &self,
        _request: Request<GetSizeRequest>,
    ) -> Result<Response<GetSizeResponse>, Status> {
        let request = _request.into_inner();
        let path = &request.path;
        let url = UrlUtil::url_from_string(path).unwrap();
        ResultUtil::transfer_service_repsonse(
            FileUtil::file_size(&url),
            |x| GetSizeResponse { size: x },
            "get size of a segment",
            &format!("get size of {}", url),
        )
    }

    async fn append(
        &self,
        _request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {

        let request = _request.into_inner();
        let path = &request.path;
        let url = UrlUtil::url_from_string(path).unwrap();
        let content = &request.content;
        type SendFunc = fn(Message) -> GResult<()>;
        let send_data_func: SendFunc = |x: Message| {
            FAKE_STORE.lock().unwrap().get_sender().send(x)?;
            Ok(())
        };
        ResultUtil::transfer_service_repsonse(
            send_data_func(Message::new(url.clone(), content.to_vec())),
            |_x| AppendResponse { status: true },
            "append data to a segment",
            &format!("append to {}", url),
        )
    }

    async fn read_all(
        &self,
        _request: Request<ReadAllRequest>,
    ) -> Result<Response<ReadAllResponse>, Status> {
        let path = &(_request.into_inner().path);
        let url = UrlUtil::url_from_string(path).unwrap();

        ResultUtil::transfer_service_repsonse(
            FileUtil::read_all_from_path(&url),
            |x| ReadAllResponse { content: x },
            "read all contents of a segment",
            &format!("read all {}", path),
        )
    }

    async fn read_range(
        &self,
        _request: Request<ReadRangeRequest>,
    ) -> Result<Response<ReadRangeResponse>, Status> {
        let request = _request.into_inner();
        let path = &request.path;
        let offset:u64 = request.offset;
        let length:u64 = request.length;
        let url = UrlUtil::url_from_string(path).unwrap();

        ResultUtil::transfer_service_repsonse(
            FileUtil::read_range_from_path(&url, &Range::new(offset, length)),
            |x| ReadRangeResponse { content: x },
            "read range of a segment",
            &format!("read range {} for offset {}, length {}", path, offset, length),
        )
    }

    async fn write_all(
        &self,
        _request: Request<WriteAllRequest>,
    ) -> Result<Response<WriteAllResponse>, Status> {
        let request = _request.into_inner();
        let path = &request.path;

        let url = UrlUtil::url_from_string(path).unwrap();
        let content = &request.content;

        ResultUtil::transfer_service_repsonse(
            FileUtil::write_all_to_path(&url, content),
            |_x| WriteAllResponse { status: true },
            "write all contents to a segment",
            &format!("write all {}", path),
        )
    }
}
