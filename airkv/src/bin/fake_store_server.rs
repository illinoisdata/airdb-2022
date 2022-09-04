use std::env;

use airkv::io::fake_store_service::fakestoreservice::fake_store_service_server::FakeStoreServiceServer;
use airkv::io::fake_store_service::ServiceImpl;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let host  = if args.len() > 1 {
        &args[1]
    } else {
        "127.0.0.1"
    };
    let addr = format!("{}:50051", host).parse().unwrap();
    let si = ServiceImpl::default();
    println!("Fakestore server listening on {}", addr);
    Server::builder()
        .add_service(FakeStoreServiceServer::new(si))
        .serve(addr)
        .await?;
    Ok(())
}
