use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Instant;
use structopt::StructOpt;

use airindex::common::error::GResult;
use airindex::db::key_rank::SOSDRankDB;
use airindex::index::hierarchical::BalanceStackIndexBuilder;
use airindex::index::IndexBuilder;
use airindex::io::profile::AffineStorageProfile;
use airindex::io::profile::Bandwidth;
use airindex::io::profile::Latency;
use airindex::io::profile::StorageProfile;
use airindex::io::storage::Adaptor;
use airindex::io::storage::ExternalStorage;
use airindex::io::storage::FileSystemAdaptor;
use airindex::meta;
use airindex::meta::Context;
use airindex::model::linear::DoubleLinearMultipleDrafter;
use airindex::store::array_store::ArrayStore;
use airindex::store::key_position::KeyPositionCollection;


/* Parsed arguments */

#[derive(Debug, Serialize, StructOpt)]
pub struct Cli {
  /// path to mount directory for profiling
  #[structopt(long)]
  root_path: String,
  /// directory to store the database
  #[structopt(long)]
  db_path: String,
  /// output path to log experiment results in append mode
  #[structopt(long)]
  out_path: String,

  /// dataset name [sosd]
  #[structopt(long)]
  dataset_name: String,

  /// manual storage profile's latency in nanoseconds (affine)
  #[structopt(long)]
  affine_latency_ns: Option<u64>,
  /// manual storage profile's bandwidth in MB/s (affine)
  #[structopt(long)]
  affine_bandwidth_mbps: Option<f64>,

  /// data type in the blob [uint32, uint64]
  #[structopt(long)]
  sosd_dtype: String,
  /// relative path from root_path to the sosd data blob
  #[structopt(long)]
  sosd_blob_path: String,
  /// number of elements, in millions (typically 200, 400, 500, 800)
  #[structopt(long)]
  sosd_size: usize,
}


/* Serializable result */

#[derive(Serialize)]
pub struct BenchmarkResult<'a> {
  setting: &'a Cli,
  time_measures: &'a [u128],
}


/* Experiment scope */

struct Experiment {
  context: Context,
  db_meta_path: PathBuf,
}

impl Experiment {
  pub fn from(args: &Cli) -> GResult<Experiment> {
    let mut context = Context::new();
    context.put_storage({
      let fsa = Box::new(FileSystemAdaptor::new(&args.root_path));
      &Rc::new(ExternalStorage::new(fsa))
    });
    if let Some(path) = PathBuf::from(&args.db_path).parent() {
      context.storage.as_ref().unwrap().create(path)?;
      println!("Created directory {:?}", path);
    }
    let db_meta_path = PathBuf::from(format!("{}_meta", args.db_path.clone()));
    Ok(Experiment {
      context,
      db_meta_path,
    })
  }

  pub fn build(&mut self, args: &Cli) -> GResult<()> {
    // load storage profile
    let profile = self.load_profile(args);

    // load dataset and generate the first key-position pairs
    let mut sosd_db = self.load_new_sosd(args)?;
    let data_kps = sosd_db.reconstruct_key_positions()?;
    self.observe_kps(&data_kps, 10);

    // build index
    let model_drafter = Box::new(DoubleLinearMultipleDrafter::exponentiation(32, 1_048_576, 2.0));
    let index_builder = BalanceStackIndexBuilder::new(
      self.context.storage.as_ref().unwrap(),
      model_drafter,
      profile.as_ref(),
      args.db_path.clone(),
    );
    let index = index_builder.build_index(&data_kps)?;
    println!("Built index at {}: {:#?}", args.db_path, index);
    sosd_db.attach_index(index);

    // // try search
    // println!("Search: {:?}", sosd_db.rank_of(0)?);
    // println!("Search: {:?}", sosd_db.rank_of(372893832698311040)?);
    // println!("Search: {:?}", sosd_db.rank_of(745859168026519040)?);
    // println!("Search: {:?}", sosd_db.rank_of(1119385857210763072)?);
    // println!("Search: {:?}", sosd_db.rank_of(1502331687042731776)?);
    // println!("Search: {:?}", sosd_db.rank_of(1920975674233238400)?);
    // println!("Search: {:?}", sosd_db.rank_of(2443914903724880384)?);
    // println!("Search: {:?}", sosd_db.rank_of(3116196522710484992)?);
    // println!("Search: {:?}", sosd_db.rank_of(4031396439579815424)?);
    // println!("Search: {:?}", sosd_db.rank_of(5349720687235716608)?);
    // println!("Search: {:?}", sosd_db.rank_of(1)?);
    // println!("Search: {:?}", sosd_db.rank_of(2)?);
    // if let Some(kr) = sosd_db.rank_of(372893832698311040)? {
    //   assert_eq!(kr.key, 372893832698311040);
    //   assert_eq!(kr.rank, 20000000); 
    // }

    // turn into serializable form
    let mut new_ctx = Context::new();
    let meta = sosd_db.to_meta(&mut new_ctx)?;
    let meta_bytes = meta::serialize(&meta)?;

    // write metadata
    self.context.storage.as_ref().unwrap().write_all(&self.db_meta_path, &meta_bytes)?;

    Ok(())
  }

  fn load_new_sosd(&self, args: &Cli) -> GResult<SOSDRankDB> {
    match args.dataset_name.as_str() {
      "blob" => self.load_blob(args),
      _ => panic!("Invalid dataset name \"{}\"", args.dataset_name),
    }
  }

  fn load_blob(&self, args: &Cli) -> GResult<SOSDRankDB> {
    let array_store = ArrayStore::from_exact(
      self.context.storage.as_ref().unwrap(),
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

  fn observe_kps(&self, kps: &KeyPositionCollection, num_print_kps: usize) {
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

  fn load_profile(&self, args: &Cli) -> Box<dyn StorageProfile> {
    if args.affine_latency_ns.is_some() {
      assert!(args.affine_bandwidth_mbps.is_some());
      return Box::new(AffineStorageProfile::new(
        Latency::from_nanos(args.affine_latency_ns.unwrap()),
        Bandwidth::from_mbps(args.affine_bandwidth_mbps.unwrap())
      ))
    }
    // Box::new(AffineStorageProfile::new(
    //   Latency::from_micros(22),
    //   Bandwidth::from_mbps(2500.0)
    // ))  // ssd simplified
    Box::new(AffineStorageProfile::new(
      Latency::from_millis(108),
      Bandwidth::from_mbps(104.0)
    ))  // nfs simplified
  }

  // TODO: multiple time?

  pub fn benchmark(&self) -> GResult<Vec<u128>> {
    let test_keys = 0..10000;  // TODO: read from file

    let start_time = Instant::now();
    let sosd_db = self.reload()?;
    let mut time_measures = Vec::new();
    for test_key in test_keys {
      let test_rank = sosd_db.rank_of(test_key);
      assert!(test_rank.is_ok());
      time_measures.push(start_time.elapsed().as_nanos())
    }
    Ok(time_measures)
  }

  fn reload(&self) -> GResult<SOSDRankDB> {
    let meta_bytes = self.context.storage.as_ref().unwrap().read_all(&self.db_meta_path)?;
    let meta = meta::deserialize(&meta_bytes)?;
    SOSDRankDB::from_meta(meta, &self.context)
  }
}

fn main() -> GResult<()> {
  // execution init
  env_logger::init();

  // parse args
  let args = Cli::from_args();
  println!("{:?}", args);

  // create experiment
  let mut exp = Experiment::from(&args)?;

  // build index
  // TODO: control optional by flag
  exp.build(&args)?;
  println!("Built index");

  // run benchmark
  let time_measures = exp.benchmark()?;
  println!("Collected {} measurements", time_measures.len());

  // write results to log
  log_result(&args, &time_measures)?;
  Ok(())
}

fn log_result(args: &Cli, time_measures: &[u128]) -> GResult<()> {
  // compose json result
  let result_json = serde_json::to_string(&BenchmarkResult {
    setting: args,
    time_measures,
  })?;

  // write appending 
  let mut log_file = OpenOptions::new()
    .create(true)
    .write(true)
    .append(true)
    .open(args.out_path.as_str())?;
  log_file.write_all(result_json.as_bytes())?;
  log_file.write_all(b"\n")?;
  println!("Log result {} characters to {}", result_json.len(), args.out_path.as_str());

  Ok(())
}
