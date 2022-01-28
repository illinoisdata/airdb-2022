use serde::Serialize;
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Instant;
use structopt::StructOpt;

use airindex::common::error::GResult;
use airindex::db::key_rank::read_keyset;
use airindex::db::key_rank::SOSDRankDB;
use airindex::index::hierarchical::BalanceStackIndexBuilder;
use airindex::index::hierarchical::BoundedTopStackIndexBuilder;
use airindex::index::Index;
use airindex::index::IndexBuilder;
use airindex::io::profile::AffineStorageProfile;
use airindex::io::profile::Bandwidth;
use airindex::io::profile::Latency;
use airindex::io::profile::StorageProfile;
use airindex::io::storage::Adaptor;
use airindex::io::storage::ExternalStorage;
use airindex::io::storage::FileSystemAdaptor;
use airindex::io::storage::MmapAdaptor;
use airindex::meta::Context;
use airindex::meta;
use airindex::model::ModelDrafter;
use airindex::model::linear::DoubleLinearMultipleDrafter;
use airindex::model::step::StepMultipleDrafter;
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

  /// action: build index
  #[structopt(long)]
  do_build: bool,
  /// action: benchmark
  #[structopt(long)]
  do_benchmark: bool,

  /// dataset name [blob]
  #[structopt(long)]
  dataset_name: String,

  /// data type in the blob [uint32, uint64]
  #[structopt(long)]
  sosd_dtype: String,
  /// relative path from root_path to the sosd data blob
  #[structopt(long)]
  sosd_blob_path: String,
  /// number of elements, in millions (typically 200, 400, 500, 800)
  #[structopt(long)]
  sosd_size: usize,
  /// relative path from root_path to the sosd data blob
  #[structopt(long)]
  keyset_path: String,

  /// index type [dlst, st, btree]
  #[structopt(long)]
  index_type: String,
  /// manual storage profile's latency in nanoseconds (affine)
  #[structopt(long)]
  affine_latency_ns: Option<u64>,
  /// manual storage profile's bandwidth in MB/s (affine)
  #[structopt(long)]
  affine_bandwidth_mbps: Option<f64>,


  /* For testing/debugging */

  /// disable cache to storage IO interface
  #[structopt(long)]
  no_cache: bool,
  /// disable parallel index building
  #[structopt(long)]
  no_parallel: bool,
  /// number of queries to test
  #[structopt(long)]
  num_samples: Option<usize>,
}


/* Serializable result */

#[derive(Serialize)]
pub struct BenchmarkResult<'a> {
  setting: &'a Cli,
  time_measures: &'a [u128],
  query_counts: &'a [usize],
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
      let adaptor = if args.no_cache {
        Box::new(FileSystemAdaptor::new(&args.root_path)) as Box<dyn Adaptor>
      } else {
        Box::new(MmapAdaptor::new(&args.root_path)) as Box<dyn Adaptor>
      };
      &Rc::new(RefCell::new(ExternalStorage::new(adaptor)))
    });
    if let Some(path) = PathBuf::from(&args.db_path).parent() {
      context.storage.as_ref().unwrap()
        .borrow_mut()
        .create(path)?;
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
    let index = self.build_index_from_kps(args, &data_kps, profile.as_ref())?;
    sosd_db.attach_index(index);

    // turn into serializable form
    let mut new_ctx = Context::new();
    let meta = sosd_db.to_meta(&mut new_ctx)?;
    let meta_bytes = meta::serialize(&meta)?;

    // write metadata
    self.context.storage.as_ref().unwrap()
      .borrow_mut()
      .write_all(&self.db_meta_path, &meta_bytes)?;

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

  fn build_index_from_kps(&self, args: &Cli, data_kps: &KeyPositionCollection, profile: &dyn StorageProfile) -> GResult<Box<dyn Index>> {
    let model_drafter = self.make_drafter(args);
    let index_builder = self.make_index_builder(args, model_drafter, profile);
    let index = index_builder.build_index(data_kps)?;
    println!("Built index at {}: {:#?}", args.db_path, index);
    Ok(index)
  }

  fn make_drafter(&self, args: &Cli) -> Box<dyn ModelDrafter> {
    let model_drafter = match args.index_type.as_str() {
      "dlst" => {
        StepMultipleDrafter::exponentiation(32, 1_048_576, 1.5, 16)
          .extend(DoubleLinearMultipleDrafter::exponentiation(32, 1_048_576, 1.5))
      },
      "st" => {
        StepMultipleDrafter::exponentiation(32, 1_048_576, 1.5, 16)
      },
      "btree" => {
        StepMultipleDrafter::exponentiation(4096, 4096, 1.5, 255)
      },
      _ => panic!("Invalid sosd dtype \"{}\"", args.sosd_dtype),
    };

    // serial or parallel drafting
    let model_drafter = if args.no_parallel {
      model_drafter.to_serial()
    } else {
      model_drafter.to_parallel()
    };
    Box::new(model_drafter)
  }

  fn make_index_builder<'a>(&'a self, args: &Cli, model_drafter: Box<dyn ModelDrafter>, profile: &'a (dyn StorageProfile + 'a)) -> Box<dyn IndexBuilder + 'a> {
    match args.index_type.as_str() {
      "dlst" => {
        Box::new(BalanceStackIndexBuilder::new(
          self.context.storage.as_ref().unwrap(),
          model_drafter,
          profile,
          args.db_path.clone(),
        ))
      },
      "st" => {
        Box::new(BalanceStackIndexBuilder::new(
          self.context.storage.as_ref().unwrap(),
          model_drafter,
          profile,
          args.db_path.clone(),
        ))
      },
      "btree" => {
        Box::new(BoundedTopStackIndexBuilder::new(
          self.context.storage.as_ref().unwrap(),
          model_drafter,
          profile,
          4096,
          args.db_path.clone(),
        ))
      },
      _ => panic!("Invalid sosd dtype \"{}\"", args.sosd_dtype),
    }
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

  // TODO: multiple time?

  pub fn benchmark(&self, args: &Cli) -> GResult<(Vec<u128>, Vec<usize>)> {
    // read keyset
    let test_keyset = read_keyset(args.keyset_path.clone())?;
    let num_samples = match args.num_samples {
      Some(num_samples) => num_samples,
      None => test_keyset.len(),
    };

    // start the clock and begin db/index reconstruction
    log::debug!("starting the benchmark");
    let mut time_measures = Vec::new();
    let mut query_counts = Vec::new();
    let mut count_milestone = 1;
    let freq_mul: f64 = 1.1;
    let start_time = Instant::now();
    let sosd_db = self.reload()?;
    log::debug!("reloaded rank db");
    for (idx, test_kr) in test_keyset.iter().enumerate().take(num_samples) {
      let rcv_kr = sosd_db.rank_of(test_kr.key)?
        .unwrap_or_else(|| panic!("Existing key {} not found", test_kr.key));
      assert_eq!(rcv_kr, *test_kr, "Mismatch rank rcv: {:?}, actual: {:?}", rcv_kr, test_kr);
      if idx + 1 == count_milestone || idx + 1 == num_samples {
        let time_elapsed = start_time.elapsed();
        time_measures.push(time_elapsed.as_nanos());
        query_counts.push(idx + 1);
        log::info!(
            "t= {:?}: {} counts, {:?}/op",
            time_elapsed,
            idx + 1,
            time_elapsed / (idx + 1).try_into().unwrap()
        );
        count_milestone = (count_milestone as f64 * freq_mul).ceil() as usize;
      }
    }
    Ok((time_measures, query_counts))
  }

  fn reload(&self) -> GResult<SOSDRankDB> {
    let meta_bytes = self.context.storage.as_ref().unwrap()
      .borrow_mut()
      .read_all(&self.db_meta_path)?;
    let meta = meta::deserialize(&meta_bytes)?;
    SOSDRankDB::from_meta(meta, &self.context)
  }
}

fn main_guarded() -> GResult<()> {
  // execution init
  env_logger::Builder::from_default_env()
    .format_timestamp_micros()
    .init();

  // parse args
  let args = Cli::from_args();
  println!("{:?}", args);

  // create experiment
  let mut exp = Experiment::from(&args)?;

  // build index
  if args.do_build {
    exp.build(&args)?;
    println!("Built index"); 
  }

  // run benchmark
  let (time_measures, query_counts) = if args.do_benchmark {
    let (time_measures, query_counts) = exp.benchmark(&args)?;
    println!("Collected {} measurements", time_measures.len()); 
    assert_eq!(time_measures.len(), query_counts.len());
    (time_measures, query_counts)
  } else {
    (Vec::new(), Vec::new())
  };

  // write results to log
  log_result(&args, &time_measures, &query_counts)?;
  Ok(())
}

fn log_result(args: &Cli, time_measures: &[u128], query_counts: &[usize]) -> GResult<()> {
  // compose json result
  let result_json = serde_json::to_string(&BenchmarkResult {
    setting: args,
    time_measures,
    query_counts,
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

fn main() {
  main_guarded().expect("Error occur during sosd experiment");
}
