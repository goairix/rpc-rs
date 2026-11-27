use greeter_example::pb::{
    HelloReply, HelloRequest,
    greeter_server::{Greeter, GreeterServer},
};
use greeter_example::{SERVICES_FILE, etcd_config};
use rpc_rs::{
    naming::{Registry, etcd::EtcdRegistry, static_registry::StaticRegistry},
    server::RpcServer,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

struct Greetings;

#[tonic::async_trait]
impl Greeter for Greetings {
    /// Return a greeting or a standard gRPC status for a blank name.
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let input = request.into_inner();
        let name = input.name.trim();
        if name.is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        Ok(Response::new(HelloReply {
            greeting: format!("Hello, {name}! (Rust)"),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:45051").await?;
    let registry: Arc<dyn Registry> = match etcd_config()? {
        Some(config) => Arc::new(EtcdRegistry::connect(config).await?),
        None => Arc::new(StaticRegistry::from_file(SERVICES_FILE)?),
    };
    let server = RpcServer::default()
        .add_service(GreeterServer::new(Greetings), Default::default())
        .start(listener, Some(registry.clone()));
    let result = server
        .until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    registry.close().await?;
    result
}
