#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;
    use url::Url;

    use crate::{
        common::error::GResult,
        db::rw_db::DBFactory,
        io::{file_utils::UrlUtil, storage_connector::{StorageType, StorageConnector}, fake_store_service_conn::FakeStoreServiceConnector}, storage::segment::SegmentInfo,
    };

    #[test]
    fn db_test() -> GResult<()> {
        let temp_dir = TempDir::new()?;
        let home_url: Url =
            UrlUtil::url_from_path(temp_dir.path())?;
        println!("home directory: {}", home_url.path());
        // create meta segment and the first tail segment
        let mut first_conn = FakeStoreServiceConnector::default();
        let fake_props: &HashMap<String, String> = &HashMap::new();
        first_conn.open(fake_props)?;
        // meta segment
        let meta_url = SegmentInfo::generate_dir(&home_url, 0, 0);
        first_conn.create(&meta_url)?;
        println!("meta directory: {}", meta_url.path());
        // first tail
        let tail_url = SegmentInfo::generate_dir(&home_url, 1<<30, 0);

        first_conn.create(&tail_url)?;

        let mut db = DBFactory::new_rwdb(home_url, StorageType::RemoteFakeStore);
        db.open(fake_props)?;
        db.close()?;
        Ok(())
    }
}
