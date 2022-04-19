use derive_more::Display;
use std::error::Error;
use tonic::{Code, Response, Status};

pub type GenericError = Box<dyn Error + Send + Sync>;
pub type GResult<T> = Result<T, GenericError>;

#[derive(Display, Debug, Clone)]
pub struct UrlParseFilePathError;
impl Error for UrlParseFilePathError {}
unsafe impl Send for UrlParseFilePathError {}
unsafe impl Sync for UrlParseFilePathError {}

#[derive(Display, Debug, Clone)]
pub struct UnknownServerError {
    msg: String,
}

impl UnknownServerError {
    pub fn new(new_msg: String) -> Self {
        Self { msg: new_msg }
    }
}
impl Error for UnknownServerError {}
unsafe impl Send for UnknownServerError {}
unsafe impl Sync for UnknownServerError {}


#[derive(Display, Debug, Clone)]
pub struct AppendError {
    msg: String,
}

impl AppendError {
    pub fn new(new_msg: String) -> Self {
        Self { msg: new_msg }
    }
}
impl Error for AppendError{}
unsafe impl Send for AppendError{}
unsafe impl Sync for AppendError{}


pub struct ResultUtil;

impl ResultUtil {
    /// The function is designed to transfer a general GResult into a tonic service response
    ///
    /// # Arguments
    ///
    /// * `result` - The input result from certain service operations.
    /// * `resp_transfer` - The function to transfer the input result content into corresponding service response
    /// * `service_name` - the current service name
    /// * `success_msg_print` - the message to print if the input result is Ok
    ///
    pub fn transfer_service_repsonse<T, E, F>(
        result: GResult<T>,
        resp_transfer: F,
        service_name: &str,
        success_msg_print: &str,
    ) -> Result<Response<E>, Status>
    where
        F: Fn(T) -> E,
    {
        match result {
            Ok(res) => {
                println!("{}", success_msg_print);
                Ok(Response::new(resp_transfer(res)))
            }
            Err(error) => {
                eprintln!("encounter error for service {}: {:?}", service_name, error);
                Err(Status::new(Code::Unknown, format!("{:?}", error)))
            }
        }
    }
}
