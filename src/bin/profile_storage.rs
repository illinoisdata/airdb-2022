use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;
use url::Url;

use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;
use structopt::StructOpt;

use airindex::common::error::GResult;
use airindex::io::internal::ExternalStorage;
use airindex::io::storage::Adaptor;
use airindex::io::storage::FileSystemAdaptor;
use airindex::io::storage::Range;
use airindex::io::storage::ReadRequest;


#[derive(Debug, Serialize, StructOpt)]
pub struct Cli {
  /// path to mount directory for profiling
  #[structopt(long)]
  root_path: String,
  /// output path to log experiment results in append mode
  #[structopt(long)]
  out_path: String,

  /// number of trials to experiment
  #[structopt(long)]
  num_trials: i32,

  /// number of files to generate
  #[structopt(long)]
  num_files: i32,
  /// size of each file (bytes)
  #[structopt(long)]
  file_size: usize,
  /// type of file content {zero, random_constant}
  #[structopt(long)]
  content: String,

  /// number of read sets to test (retrieval time is measured per read set)
  #[structopt(long)]
  num_readsets: i32,
  /// how to pick a file {random}
  #[structopt(long)]
  file_picking: String,
  /// pattern in a read set {sequential}
  #[structopt(long)]
  read_mode: String,
  /// number of pages to read in a set
  #[structopt(long)]
  num_read_pages: i32,
  /// page size (bytes) for each read in a set
  #[structopt(long)]
  read_page_size: usize,
  /// storage method to benchmark {batch_sequential}
  #[structopt(long)]
  read_method: String,
}

#[derive(Serialize)]
pub struct ProfileResult<'a> {
  setting: &'a Cli,
  time_measures: &'a [Vec<u128>],
}

fn main() -> GResult<()> {
  // execution init
  env_logger::init();
  
  // parse args
  let args = Cli::from_args();
  println!("{:?}", args);

  // benchmark
  let time_measures = benchmark(&args)?;

  // write results with settings
  log_result(&args, &time_measures)?;

  Ok(())
}

#[derive(Debug)]
enum FileContent {
  Zero,
  RandomConstant,
}

#[derive(Debug)]
struct FileSpec {
  size: usize,
  content: FileContent,
}

#[derive(Debug)]
struct FileDescription {
  url: Url,
  spec: FileSpec,
}

enum FilePicking {
  Random,
}

enum ReadMode {
  Sequential,
}

enum ReadMethod {
  BatchSequential,
}

struct ExperimentConfig<'a> {
  file_descs: &'a [FileDescription],
  file_picking: FilePicking,
  read_mode: ReadMode,
  num_read_pages: i32,
  read_page_size: usize,
  read_method: ReadMethod,
}

fn benchmark(args: &Cli) -> GResult<Vec<Vec<u128>>> {
  (0..args.num_trials).map(|i| {
    println!("trial {} / {}", i, args.num_trials);
    benchmark_set(args)
  }).collect()
}

fn benchmark_set(args: &Cli) -> GResult<Vec<u128>> {
  // create storage
  let fsa = Box::new(FileSystemAdaptor::new());
  let es = Rc::new(RefCell::new(ExternalStorage::new()
    .with("file".to_string(), fsa)?
  ));

  // writes
  let file_descs = benchmark_write(&es, args)?;
  println!("Wrote test files {:?}", file_descs);

  // reads
  let exp_config = generate_experiment_config(args, &file_descs);
  let time_measures = benchmark_read(&es, args, &exp_config)?;

  // cleanup
  benchmark_cleanup(&es, file_descs)?;

  // return benchmark numbers
  Ok(time_measures)
}

fn benchmark_write(es: &Rc<RefCell<ExternalStorage>>, args: &Cli) -> GResult<Vec<FileDescription>> {
  let root_url = Url::parse(&args.root_path).expect("Invalid root_path, expecting url");
  (0..args.num_files).map(|_i| {
    let file_spec = generate_one_writeset(args);
    write_file_spec(es, file_spec, &root_url)
  }).collect()
}

fn generate_one_writeset(args: &Cli) -> FileSpec {
  FileSpec {
    size: args.file_size,
    content: match args.content.as_str() {
      "zero" => FileContent::Zero,
      "random_constant" => FileContent::RandomConstant,
      _ => panic!("Invalid file content {}", args.content),
    }
  }
}

fn write_file_spec(es: &Rc<RefCell<ExternalStorage>>, file_spec: FileSpec, root_url: &Url) -> GResult<FileDescription> {
  // randomize name
  let file_name: String = rand::thread_rng().sample_iter(&Alphanumeric)
    .take(7)
    .map(char::from)
    .collect();
  let file_path = "block_".to_owned() + &file_name;
  let file_url = root_url.join(&file_path)?;

  // generate content
  let file_content = match file_spec.content {
    FileContent::Zero => vec![0u8; file_spec.size],
    FileContent::RandomConstant => {
      let file_byte = rand::thread_rng().gen::<u8>();
      vec![file_byte; file_spec.size]
    },
  };
  // rng.fill(&mut file_content);

  // write file
  es.borrow_mut().write_all(&file_url, &file_content)?;

  // return with description
  Ok(FileDescription{url: file_url, spec: file_spec})
}

fn benchmark_read(es: &Rc<RefCell<ExternalStorage>>, args: &Cli, exp_config: &ExperimentConfig) -> GResult<Vec<u128>> {
  // do reading
  (0..args.num_readsets).map(|_i| {
    let readset = generate_one_readset(exp_config);
    read_measure(es, &readset, exp_config)
  }).collect()
}

fn generate_experiment_config<'a>(args: &Cli, file_descs: &'a [FileDescription]) -> ExperimentConfig<'a> {
  ExperimentConfig{
    file_descs,
    file_picking: match args.file_picking.as_str() {
      "random" => FilePicking::Random,
      _ => panic!("Invalid file picking {}", args.file_picking),
    },
    read_mode: match args.read_mode.as_str() {
      "sequential" => ReadMode::Sequential,
      _ => panic!("Invalid read mode {}", args.read_mode),
    },
    num_read_pages: args.num_read_pages,
    read_page_size: args.read_page_size,
    read_method: match args.read_method.as_str() {
      "batch_sequential" => ReadMethod::BatchSequential,
      _ => panic!("Invalid read method {}", args.read_method),
    },
  }
}

fn generate_one_readset(exp_config: &ExperimentConfig) -> Vec<ReadRequest>  {
  let mut rng = rand::thread_rng();

  // select one file
  let file_desc_idx = match exp_config.file_picking {
    FilePicking::Random => rng.gen_range(0..exp_config.file_descs.len()),
  };
  let file_desc = &exp_config.file_descs[file_desc_idx];

  // generate reads
  match &exp_config.read_mode {
    ReadMode::Sequential => {
      let page_size = exp_config.read_page_size;
      let num_pages = exp_config.num_read_pages;
      let last_offset = file_desc.spec.size - page_size * (num_pages as usize);
      let initial_offset = rng.gen_range(0..last_offset+1);
      (0..num_pages).map(|page_idx| {
        let offset = initial_offset + (page_idx as usize) * page_size;
        ReadRequest::Range{
          url: file_desc.url.clone(),
          range: Range{ offset, length: page_size },
        }
      }).collect::<Vec<_>>()
    }
  }
}

fn read_measure(es: &Rc<RefCell<ExternalStorage>>, read_request: &[ReadRequest], exp_config: &ExperimentConfig) -> GResult<u128> {
  let start_time = Instant::now();
  match exp_config.read_method {
    ReadMethod::BatchSequential => es.borrow_mut().read_batch_sequential(read_request)?,
  };
  Ok(start_time.elapsed().as_nanos())
}

fn benchmark_cleanup(es: &Rc<RefCell<ExternalStorage>>, file_descs: Vec<FileDescription>) -> GResult<()> {
  for file_desc in file_descs {
    es.borrow_mut().remove(&file_desc.url)?;
  }
  Ok(())
}

fn log_result(args: &Cli, time_measures: &[Vec<u128>]) -> GResult<()> {
  // compose json result
  let result_json = serde_json::to_string(&ProfileResult {
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
