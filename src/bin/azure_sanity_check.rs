use rand::Rng;
use serde::Serialize;
use std::path::Path;
use structopt::StructOpt;

use airindex::common::error::GResult;
use airindex::io::storage::Adaptor;
use airindex::io::storage::AzureContainerAdaptor;
use airindex::io::storage::Range;


#[derive(Debug, Serialize, StructOpt)]
pub struct Cli {
  /// azure container name
  #[structopt(long)]
  container: String,

  /// prefix to blobs
  #[structopt(long)]
  blob_prefix: String,

  /// blob type [block, append, page]
  #[structopt(long)]
  blob_type: String,

  /// blob name to write
  #[structopt(long)]
  blob_name: String,
}

fn main() -> GResult<()> {
  // execution init
  env_logger::Builder::from_default_env()
    .format_timestamp_micros()
    .init();
  
  // parse args
  let args = Cli::from_args();
  log::info!("{:?}", args);

  // create adaptor
  log::info!("Creating azure adpator");
  let mut adaptor = AzureContainerAdaptor::new_block(&args.container, &args.blob_prefix);

  // create some blob
  log::info!("Writing a blob for test");
  let content = vec![rand::thread_rng().gen::<u8>(); 2048];
  adaptor.write_all(Path::new(&args.blob_name), &content)?;

  // read blob
  log::info!("Reading whole blob");
  {
    let blob = adaptor.read_all(Path::new(&args.blob_name))?;
    assert_eq!(blob.len(), content.len());
    for idx in 0..content.len() {
      assert_eq!(blob[idx], content[idx]);
    }
  }

  // read blob range
  log::info!("Reading blob in range");
  {
    let range = Range { offset: 512, length: 1024 };
    let blob = adaptor.read_range(Path::new(&args.blob_name), &range)?;
    assert_eq!(blob.len(), range.length);
    for idx in 0..range.length {
      assert_eq!(blob[idx], content[range.offset + idx]);
    }
  }

  // read blob range from different adpator
  log::info!("Reading blob in range with a new adaptor");
  let mut adaptor2 = AzureContainerAdaptor::new_block(&args.container, &args.blob_prefix);
  {
    let range = Range { offset: 512, length: 1024 };
    let blob = adaptor2.read_range(Path::new(&args.blob_name), &range)?;
    assert_eq!(blob.len(), range.length);
    for idx in 0..range.length {
      assert_eq!(blob[idx], content[range.offset + idx]);
    }
  }


  Ok(())
}
