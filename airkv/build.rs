fn main() -> Result<(), Box<dyn std::error::Error>> {
  match tonic_build::compile_protos("proto/fakestore.proto") {
    Ok(()) => {
      Ok(())
    },
    Err(error) => {
      eprintln!(":(");
      Err(Box::new(error))
    }
  }
}
