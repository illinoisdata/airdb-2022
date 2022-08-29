use std::{collections::HashMap, thread, time::Duration};

use airkv::{db::rw_db::DBFactory, io::storage_connector::StorageType};
use url::Url;

fn main() {
    let home_dir_new = Url::parse(&format!("az:///{}/", "integration")).expect("url parse error");

    let mut c_db = DBFactory::new_compactiondb(home_dir_new, StorageType::AzureStore);

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
