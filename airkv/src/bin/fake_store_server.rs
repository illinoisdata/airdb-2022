use airkv::io::fake_store_service::fakestoreservice::fake_store_service_server::FakeStoreServiceServer;
use airkv::io::fake_store_service::ServiceImpl;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let si = ServiceImpl::default();
    println!("Fakestore server listening on {}", addr);
    Server::builder()
        .add_service(FakeStoreServiceServer::new(si))
        .serve(addr)
        .await?;
    Ok(())
}
