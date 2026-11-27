use greeter_example::pb::{HelloRequest, greeter_client::GreeterClient};
use rpc_rs::{discovery::Discovery, naming::static_registry::StaticRegistry};
use std::time::Duration;
use tonic::Request;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Wait for the first route, then call the generated client with a per-request timeout.
async fn greet(discovery: &Discovery, name: String) -> Result<String, Error> {
    // Discovery returns before the initial dial completes.
    tokio::time::timeout(Duration::from_secs(5), async {
        while discovery.ready_count() == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let mut request = Request::new(HelloRequest { name });
    request.set_timeout(Duration::from_secs(2));
    let reply = GreeterClient::new(discovery.service())
        .say_hello(request)
        .await?;
    Ok(reply.into_inner().greeting)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let name = std::env::args().nth(1).unwrap_or_else(|| "Ada".into());
    let directory = StaticRegistry::from_file("examples/greeter/services.json")?;
    let mut discovery =
        Discovery::connect(&directory, "example.v1.Greeter", Default::default()).await?;
    let result = greet(&discovery, name).await;
    discovery.close().await;
    println!("{}", result?);
    Ok(())
}
