use std::{collections::HashMap};

use airkv::{
    io::{azure_conn::AzureConnector, storage_connector::{StorageConnector, StorageType}, fake_store_service_conn::FakeStoreServiceConnector},
    storage::{seg_util::META_SEG_ID, segment::SegmentInfo},
};
use url::Url;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let store_type: StorageType;
    let container_addr: String;
    if args.len() > 2 {
        match args[1].as_str() {
            "azure" => {
                store_type = StorageType::AzureStore;
                container_addr = format!("az:///{}/", args[2]); 
            },
            "fake" => {
                store_type = StorageType::RemoteFakeStore;
                container_addr = format!("file://{}/", args[2]);
            },
            default => panic!("unknown storage type {}, only support azure or fake", default),
        }
    } else {
        store_type = StorageType::AzureStore;
        container_addr = "az:///airkvycsb/".to_string();
    };

    let home_url = Url::parse(container_addr.as_str()).expect("url parse error");
    let mut util_conn: Box<dyn StorageConnector> = match store_type {
        StorageType::RemoteFakeStore => {
            Box::new(FakeStoreServiceConnector::default())
        },
        StorageType::AzureStore => {
            Box::new(AzureConnector::default())
        },
        _default => panic!("unknown store type, only support azure or remote fake store")
    };

    let fake_props: &HashMap<String, String> = &HashMap::new();
    util_conn
        .open(fake_props)
        .expect("failed to open connector");
    //create container
    util_conn
        .create(&home_url)
        .expect("failed to create container");
    // create the meta segment in advance
    let meta_url = SegmentInfo::generate_dir(&home_url, META_SEG_ID);
    util_conn
        .create(&meta_url)
        .expect("Failed to create meta segment");
    println!("meta directory: {}", meta_url.path());

    // create wr client tracker segment in advance
    let client_tracker_dir = home_url
        .join("rw_client_tracker")
        .unwrap_or_else(|_| panic!("Cannot generate a path for rw_client_tracker"));
    util_conn
        .create(&client_tracker_dir)
        .expect("Failed to create client_tracker segment");
    println!(
        "client_tracker seg directory: {}",
        client_tracker_dir.path()
    );
}
