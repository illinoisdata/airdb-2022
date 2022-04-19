use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::io::Write;

use super::file_utils::Range;
use crate::common::error::ResultUtil;
use crate::io::file_utils::{FileUtil, UrlUtil};
use crate::storage::data_entry::AppendRes;
use crate::storage::segment::BlockNum;

use dashmap::DashMap;
use fakestoreservice::fake_store_service_server::FakeStoreService;
use fakestoreservice::*;
use tokio::runtime::Runtime;
use tonic::{Request, Response, Status};

pub mod fakestoreservice {
    tonic::include_proto!("fakestoreservice");
}

lazy_static! {
    pub static ref BLOCKNUM_MAP: DashMap<String, BlockNum> = DashMap::new();
}

// defining a struct for our service
// #[derive(Default)]
pub struct ServiceImpl {
    rt: Runtime,
}

impl Default for ServiceImpl {
    fn default() -> Self {
        Self {
            rt: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .build()
                .unwrap(),
        }
    }
}

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
        let url = UrlUtil::url_from_string(&request.path).unwrap();
        let path = url.path();

        let res = ResultUtil::transfer_service_repsonse(
            FileUtil::create_file(&url),
            |_x| CreateResponse { status: true },
            "create path",
            &format!("create {}", path),
        );
        BLOCKNUM_MAP.insert(path.to_string(), 0);
        res
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
        let res = self
            .rt
            .spawn(async move {
                ResultUtil::transfer_service_repsonse(
                    FileUtil::file_size(&(UrlUtil::url_from_string(&request.path).unwrap())),
                    |x| GetSizeResponse { size: x },
                    "get size of a segment",
                    &format!("get size of {}", &request.path),
                )
            })
            .await;
        res.expect("Error: failed to await future for get_size")
    }

    async fn get_props(
        &self,
        _request: Request<GetPropsRequest>,
    ) -> Result<Response<GetPropsResponse>, Status> {
        let request = _request.into_inner();

        let res = self
            .rt
            .spawn(async move {
                let url = UrlUtil::url_from_string(&request.path).expect("get url error");
                let meta = OpenOptions::new()
                    .read(true)
                    .open(url.path())
                    .expect("open file error")
                    .metadata()
                    .expect("get meta error");

                let len = meta.len();
                let is_seal = meta.permissions().readonly();
                let block_num = if is_seal {
                    0u16
                } else {
                    *BLOCKNUM_MAP.get(url.path()).unwrap_or_else(|| {
                        panic!("Problem getting block number of path[{}]", url.path())
                    })
                };
                println!("get props {}, len: {}, block_num: {}, is_sealed: {}", url.path(), len, block_num, is_seal);

                Ok(Response::new(GetPropsResponse {
                    seglen: len,
                    blocknum: block_num as u32,
                    sealed: is_seal,
                }))
            })
            .await;

        res.expect("Error: failed to await future for seal")
    }

    async fn seal(&self, _request: Request<SealRequest>) -> Result<Response<SealResponse>, Status> {
        let request = _request.into_inner();
        let res = self
            .rt
            .spawn(async move {
                let url = UrlUtil::url_from_string(&request.path).unwrap();
                let path = url.path();
                let response = ResultUtil::transfer_service_repsonse(
                    FileUtil::seal_file(&url),
                    |_x| SealResponse { status: true },
                    "seal path",
                    &format!("seal {}", path),
                );
                BLOCKNUM_MAP.remove(path);
                println!("seal {}", url.path());
                response
            })
            .await;
        res.expect("Error: failed to await future for seal")
    }

    async fn append(
        &self,
        _request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let request = _request.into_inner();
        // let path = &request.path;
        // let url = UrlUtil::url_from_string(path).unwrap();

        let res = self
            .rt
            .spawn(async move {
                // status:
                // 0 => success
                // 1 => BlockCountExceedFailure
                // 2 => SegmentLengthExceedFailire
                // 3 => AppendToSealedFailure
                // 4 => SegmentNotExsitFailure
                // 5 => UnknownFailure
                let url = UrlUtil::url_from_string(&request.path).unwrap();
                let path = url.path();
                match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(path)
                {
                    Ok(mut f) => {
                        f.write_all(&request.content).unwrap_or_else(|_| {
                            panic!("Problem flushing the append data to path[{}]", path)
                        });
                        BLOCKNUM_MAP.alter(path, |_, v| v + 1);
                        let block_num = *BLOCKNUM_MAP.get(path).unwrap_or_else(|| {
                            panic!("Problem getting block number of path[{}]", path)
                        });

                        println!("append {}", url.path());


                        AppendResponse {
                            status: AppendRes::Success(0).to_status_code(),
                            blocknum: block_num as u32,
                        }
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::PermissionDenied => AppendResponse {
                            status: AppendRes::AppendToSealedFailure.to_status_code(),
                            blocknum: 0u32,
                        },
                        ErrorKind::NotFound => AppendResponse {
                            status: AppendRes::SegmentNotExsitFailure.to_status_code(),
                            blocknum: 0u32,
                        },
                        default => {
                            println!("ERROR: appending data encounter unknown error: {}", default);
                            AppendResponse {
                                status: AppendRes::UnknownFailure.to_status_code(),
                                blocknum: 0u32,
                            }
                        }
                    },
                }
            })
            .await;

        match res {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => {
                println!("ERROR: append encounter error: {}", err);
                Ok(Response::new(AppendResponse {
                    status: AppendRes::UnknownFailure.to_status_code(),
                    blocknum: 0u32,
                }))
            }
        }
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
        let offset: u64 = request.offset;
        let length: u64 = request.length;
        let url = UrlUtil::url_from_string(path).unwrap();

        ResultUtil::transfer_service_repsonse(
            FileUtil::read_range_from_path(&url, &Range::new(offset, length)),
            |x| ReadRangeResponse { content: x },
            "read range of a segment",
            &format!(
                "read range {} for offset {}, length {}",
                path, offset, length
            ),
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
