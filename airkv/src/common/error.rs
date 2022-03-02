use derive_more::Display;
use std::error::Error;


pub type GenericError = Box<dyn Error + Send + Sync>;
pub type GResult<T> = Result<T, GenericError>;


#[derive(Display, Debug, Clone)]
pub struct UrlParseFilePathError;
impl Error for UrlParseFilePathError {}
unsafe impl Send for UrlParseFilePathError {}
unsafe impl Sync for UrlParseFilePathError {}