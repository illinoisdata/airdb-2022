use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::Serialize;

use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;
use tempdir::TempDir;

use airindex::io::storage as astorage;
use airindex::io::storage::Adaptor;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

enum FileContent {
  Zero,
  RandomConstant,
}

struct FileSpec {
  size: usize,
  content: FileContent,
}

struct FileDescription {
  path: PathBuf,
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

fn benchmark(args: &Cli) -> io::Result<Vec<Vec<u128>>> {
  (0..args.num_trials).map(|i| {
    println!("trial {} / {}", i, args.num_trials);
    benchmark_set(args)
  }).collect()
}

fn benchmark_set(args: &Cli) -> io::Result<Vec<u128>> {
  // create storage
  let temp_dir = TempDir::new_in(&args.root_path, "temp_dir")?;
  let fsa = astorage::FileSystemAdaptor::new(&temp_dir);
  let es = astorage::ExternalStorage::new(Box::new(fsa));

  // writes
  let file_descs = benchmark_write(&es, args)?;
  println!("Wrote files for test at {:?}", temp_dir);

  // reads
  let time_measures = benchmark_read(&es, args, &file_descs)?;

  // cleanup
  benchmark_cleanup(file_descs)?;

  // return benchmark numbers
  Ok(time_measures)
}

fn benchmark_write(es: &astorage::ExternalStorage, args: &Cli) -> io::Result<Vec<FileDescription>> {
  (0..args.num_files).map(|_i| {
    let file_spec = generate_one_writeset(args);
    write_file_spec(es, file_spec)
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

fn write_file_spec(es: &astorage::ExternalStorage, file_spec: FileSpec) -> io::Result<FileDescription> {
  // randomize name
  let file_name: String = rand::thread_rng().sample_iter(&Alphanumeric)
    .take(7)
    .map(char::from)
    .collect();
  let file_path = PathBuf::from("block_".to_owned() + &file_name);

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
  es.write_all(file_path.as_path(), &file_content)?;

  // return with description
  Ok(FileDescription{path: file_path, spec: file_spec})
}

fn benchmark_read(es: &astorage::ExternalStorage, args: &Cli, file_descs: &[FileDescription]) -> io::Result<Vec<u128>> {
  // make experiment config
  let exp_config = generate_experiment_config(args, file_descs);

  // do reading
  (0..args.num_readsets).map(|_i| {
    let readset = generate_one_readset(&exp_config);
    read_measure(es, &readset, &exp_config)
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

fn generate_one_readset(exp_config: &ExperimentConfig) -> Vec<astorage::ReadRequest>  {
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
        astorage::ReadRequest::Range{
          path: file_desc.path.clone(),
          range: astorage::Range{ offset, length: page_size },
        }
      }).collect::<Vec<_>>()
    }
  }
}

fn read_measure(es: &astorage::ExternalStorage, read_request: &[astorage::ReadRequest], exp_config: &ExperimentConfig) -> io::Result<u128> {
  let start_time = Instant::now();
  match exp_config.read_method {
    ReadMethod::BatchSequential => es.read_batch_sequential(read_request)?,
  };
  Ok(start_time.elapsed().as_nanos())
}

fn benchmark_cleanup(_file_descs: Vec<FileDescription>) -> io::Result<()> {
  // nothing to do...
  // tempdir will automatically remove files
  Ok(())
}

fn log_result(args: &Cli, time_measures: &[Vec<u128>]) -> io::Result<()> {
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
