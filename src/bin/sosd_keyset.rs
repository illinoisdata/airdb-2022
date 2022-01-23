use serde::Serialize;
use std::path::PathBuf;
use std::rc::Rc;
use structopt::StructOpt;

use airindex::common::error::GResult;
use airindex::db::key_rank::generate_keyset;
use airindex::db::key_rank::SOSDRankDB;
use airindex::io::storage::ExternalStorage;
use airindex::io::storage::FileSystemAdaptor;
use airindex::store::array_store::ArrayStore;
use airindex::store::key_position::KeyPositionCollection;


/* Parsed arguments */

#[derive(Debug, Serialize, StructOpt)]
pub struct Cli {
  /// data type in the blob [uint32, uint64]
  #[structopt(long)]
  sosd_dtype: String,
  /// path to sosd data blob
  #[structopt(long)]
  sosd_blob_path: String,
  /// number of elements, in millions (typically 200, 400, 500, 800)
  #[structopt(long)]
  sosd_size: usize,
  
  /// relative path from root_path to path to write the keyset file
  #[structopt(long)]
  keyset_path: String,
  /// number of keysets to generate
  #[structopt(long)]
  num_keyset: usize,
}


fn main() -> GResult<()> {
  // execution init
  env_logger::init();

  // parse args
  let args = Cli::from_args();
  println!("{:?}", args);

  // load sosd db
  let sosd_db = load_sosd(&args)?;
  let kps = sosd_db.reconstruct_key_positions()?;
  observe_kps(&kps, 5);

  // randomly select a subset of keys
  generate_keyset(&kps, args.keyset_path.clone(), args.num_keyset)?;
  println!("Wrote keyset file at {} with {} keys", args.keyset_path, args.num_keyset);
  Ok(())
}

fn load_sosd(args: &Cli) -> GResult<SOSDRankDB> {
  // prepare storage interface
  let fsa = Box::new(FileSystemAdaptor::new(&"./".to_owned()));
  let es = Rc::new(ExternalStorage::new(fsa));

  let array_store = ArrayStore::from_exact(
    &es,
    PathBuf::from(&args.sosd_blob_path),
    match args.sosd_dtype.as_str() {
      "uint32" => 4,
      "uint64" => 8,
      _ => panic!("Invalid sosd dtype \"{}\"", args.sosd_dtype),
    },
    8,  // SOSD array leads with 8-byte encoding of the length
    args.sosd_size * 1_000_000,
  );
  Ok(SOSDRankDB::new(array_store))
}

fn observe_kps(kps: &KeyPositionCollection, num_print_kps: usize) {
  println!("Head:");
  for idx in 0..num_print_kps {
    println!("\t{}: {:?}", idx, kps[idx]);
  }
  println!("Intermediate:");
  let step = kps.len() / num_print_kps;
  for idx in 0..num_print_kps {
    println!("\t{}: {:?}", idx * step, kps[idx * step]);
  }
  println!("Length= {}, where last kp: {:?}", kps.len(), kps[kps.len() - 1]);
}
