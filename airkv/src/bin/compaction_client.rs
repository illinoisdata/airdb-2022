use std::{collections::HashMap, thread, time::Duration};

use airkv::{db::rw_db::DBFactory, io::storage_connector::StorageType};
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

    let home_dir_new = Url::parse(container_addr.as_str()).expect("url parse error");
    let mut c_db = DBFactory::new_compactiondb(home_dir_new, store_type);

    c_db.open(&HashMap::new())
        .expect("failed to call compactionDB.open()");

    loop {
        let task = c_db.get_task();
        if let Some(task_desc) = task {
            let res = c_db.execute(&task_desc).unwrap();
            if res {
                println!("INFO: run compaction successfully for task: {}", task_desc);
            } else {
                println!(
                    "WARN: finished executing compaction but failed to commit for task: {}",
                    task_desc
                );
            }
        } else {
            thread::sleep(Duration::from_secs(20));
        }
    }
}
