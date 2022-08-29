use std::{collections::HashMap, env, time::Instant};

use airkv::io::{
    azure_conn::AzureConnector, file_utils::UrlUtil, storage_connector::StorageConnector,
};
use rand::{rngs::ThreadRng, Rng};


fn main() {
    let args: Vec<String> = env::args().collect();
    let url = UrlUtil::url_from_string(&args[1].to_string()).unwrap();
    let mut rng = rand::thread_rng();

    let append_data = gen_random_bytes(&mut rng, 1000);
    let row_count = if args.len() > 2 {
        args[2].parse::<usize>().unwrap()
    } else {
        2000_usize
    };

    let current = Instant::now();
    let mut util_conn = AzureConnector::default();
    let fake_props: &HashMap<String, String> = &HashMap::new();
    util_conn.open(fake_props).expect("open failed");

    (0..row_count).for_each(|_id| {
        util_conn.append(&url, &append_data);
    });

    let query_time = current.elapsed().as_millis() as usize;

    println!("append time {} ms", query_time);
    println!(
        "append throughput {} qps",
        row_count as f32 / (query_time as f32 / 1000_f32) 
    );
    println!("append latency {} ms", query_time as f32 / row_count as f32);
}

// generate random bytes
fn gen_random_bytes(rng: &mut ThreadRng, max: usize) -> Vec<u8> {
    (0..rng.gen_range(10..=max))
        .map(|_| rand::random::<u8>())
        .collect()
}
