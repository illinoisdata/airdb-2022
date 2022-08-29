use std::{env, io::Write, time::Instant};

use airkv::io::file_utils::UrlUtil;
use rand::{rngs::ThreadRng, Rng};
use std::fs::OpenOptions;

fn main() {
    let args: Vec<String> = env::args().collect();
    let url = UrlUtil::url_from_string(&args[1].to_string()).unwrap();
    let path = url.path();
    let mut rng = rand::thread_rng();

    let append_data = gen_random_bytes(&mut rng, 1000);
    let row_count = if args.len() > 2 {
        args[2].parse::<usize>().unwrap()
    } else {
        2000_usize
    };

    let current = Instant::now();

    match OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(mut f) => {
            (0..row_count).for_each(|_i| {
                f.write_all(&append_data).unwrap_or_else(|_| {
                    panic!("Problem flushing the append data to path[{}]", path)
                });
            });
        }
        Err(_) => todo!(),
    }
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
