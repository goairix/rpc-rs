//! Test peer for exercising grpc-go and tonic over real sockets.
use std::pin::Pin;
use std::time::Duration;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
mod pb {
    tonic::include_proto!("interop");
}
use pb::Message;
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};

type Output = Pin<Box<dyn Stream<Item = Result<Message, Status>> + Send>>;
type Error = Box<dyn std::error::Error + Send + Sync>;

struct Peer(String);

impl Peer {
    fn reply(&self, mut message: Message) -> Message {
        message.instance.clone_from(&self.0);
        message
    }
}

#[tonic::async_trait]
impl Echo for Peer {
    type DownloadStream = Output;
    type ChatStream = Output;

    async fn say(&self, req: Request<Message>) -> Result<Response<Message>, Status> {
        let md = req.metadata().get("x-interop").cloned();
        let msg = req.into_inner();
        match msg.value.as_str() {
            "invalid" => return Err(Status::invalid_argument("invalid input")),
            "wait" => std::future::pending::<()>().await,
            _ => {}
        }
        let mut res = Response::new(self.reply(msg));
        if let Some(md) = md {
            res.metadata_mut().insert("x-interop", md);
        }
        Ok(res)
    }

    async fn upload(&self, req: Request<Streaming<Message>>) -> Result<Response<Message>, Status> {
        let mut stream = req.into_inner();
        let mut sequence = 0;
        while let Some(msg) = stream.message().await? {
            sequence += msg.sequence;
        }
        Ok(Response::new(self.reply(Message {
            sequence,
            ..Default::default()
        })))
    }

    async fn download(&self, req: Request<Message>) -> Result<Response<Output>, Status> {
        let msg = req.into_inner();
        let id = self.0.clone();
        let stream = async_stream::try_stream! {
            for sequence in 1..=msg.sequence {
                yield Message {sequence, value:msg.value.clone(), instance:id.clone()};
            }
            if msg.value=="invalid" {Err(Status::invalid_argument("invalid input"))?;}
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn chat(&self, req: Request<Streaming<Message>>) -> Result<Response<Output>, Status> {
        let id = self.0.clone();
        let stream = req.into_inner().map(move |item| {
            item.map(|m| Message {
                instance: id.clone(),
                ..m
            })
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

fn msg(sequence: i32, value: &str) -> Message {
    Message {
        sequence,
        value: value.into(),
        ..Default::default()
    }
}

async fn check<T>(mut client: EchoClient<T>, go_framework: bool) -> Result<(), Error>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    let mut req = Request::new(msg(7, "hello"));
    req.metadata_mut().insert("x-interop", "rust".parse()?);
    let res = client.say(req).await?;
    assert_eq!(res.metadata().get("x-interop").unwrap(), "rust");
    let echoed = res.into_inner();
    assert_eq!((echoed.sequence, echoed.value.as_str()), (7, "hello"));
    assert!(!echoed.instance.is_empty());
    let items = [msg(1, "a"), msg(2, "b")];
    let sum = client
        .upload(tokio_stream::iter(items.clone()))
        .await?
        .into_inner();
    assert_eq!(sum.sequence, 3);
    let mut down = client.download(msg(2, "invalid")).await?.into_inner();
    for i in 1..=2 {
        assert_eq!(down.message().await?.unwrap().sequence, i)
    }
    assert_eq!(
        down.message().await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let mut chat = client
        .chat(tokio_stream::iter(items.clone()))
        .await?
        .into_inner();
    for m in items {
        let reply = chat.message().await?.unwrap();
        assert_eq!((reply.sequence, reply.value), (m.sequence, m.value))
    }
    assert!(chat.message().await?.is_none());
    let invalid = client.say(msg(0, "invalid")).await.unwrap_err();
    assert_eq!(
        invalid.code(),
        if go_framework {
            tonic::Code::Unknown
        } else {
            tonic::Code::InvalidArgument
        }
    );
    if go_framework {
        assert_eq!(
            client.say(msg(0, "business")).await.unwrap_err().code(),
            tonic::Code::Unknown
        );
    }
    let mut wait = Request::new(msg(0, "wait"));
    wait.set_timeout(Duration::from_millis(80));
    let err = rpc_rs::governance::UnaryPolicy::default()
        .call(wait, None, |r| {
            let mut client = client.clone();
            async move { client.say(r).await }
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded, "{err:?}");
    println!("{}", echoed.instance);
    Ok(())
}

fn config() -> Result<rpc_rs::registry::EtcdConfig, Error> {
    Ok(rpc_rs::registry::EtcdConfig {
        endpoints: vec![std::env::var("ETCD_ENDPOINT")?],
        namespace: std::env::var("RPC_NAMESPACE")?,
        ..Default::default()
    })
}

/// Serve the same generated implementation with optional discovery registration.
async fn serve(addr: &str, id: &str) -> Result<(), Error> {
    use rpc_rs::{
        registry::{EtcdRegistry, RegistrationOptions},
        server::RpcServer,
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let registry = if std::env::var_os("ETCD_ENDPOINT").is_some() {
        Some(EtcdRegistry::connect(config()?).await?)
    } else {
        None
    };
    let mut builder = RpcServer::default()
        .with_reflection()
        .with_descriptor(tonic::include_file_descriptor_set!("echo_descriptor"));
    if std::env::var_os("RPC_LIMIT_ALL").is_some() {
        builder = builder.with_limiter(rpc_rs::governance::TokenBucket::new(1.0, 0));
    }
    let server = builder
        .add_service(
            EchoServer::new(Peer(id.into())),
            RegistrationOptions::default(),
        )
        .start(
            listener,
            registry.clone().map(|r| std::sync::Arc::new(r) as _),
        );
    // The harness terminates this peer with SIGTERM; wait for lease revocation before exiting.
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let result = server
        .until(async {
            term.recv().await;
        })
        .await;
    if let Some(r) = registry {
        r.close().await?;
    }
    result
}

async fn discovered(mode: &str) -> Result<(), Error> {
    use rpc_rs::discovery::{Discovery, DiscoveryOptions, LbPolicy};
    use std::io::BufRead;
    let options = DiscoveryOptions {
        policy: LbPolicy::WeightedRoundRobin,
        ..Default::default()
    };
    let mut discovery = Discovery::connect(&config()?, "interop.Echo", options).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while discovery.ready_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let mut client = EchoClient::new(discovery.service());
    match mode {
        "check" => check(client, true).await?,
        "limited" => assert_eq!(
            client.say(msg(0, "test")).await.unwrap_err().code(),
            tonic::Code::Unknown
        ),
        "sample" => {
            // Keep a single client and watch active across governance updates.
            for line in std::io::stdin().lock().lines() {
                let n: usize = line?.parse()?;
                let mut ids = Vec::new();
                for _ in 0..n {
                    ids.push(client.say(msg(0, "sample")).await?.into_inner().instance);
                }
                println!("{}", ids.join(","));
            }
        }
        _ => return Err("unexpected mode".into()),
    }
    discovery.close().await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [mode, addr, id] if mode == "serve" => serve(addr, id).await,
        [mode, target] if target == "discovery" => discovered(mode).await,
        [mode, addr] if mode == "check" => {
            check(EchoClient::connect(format!("http://{addr}")).await?, false).await
        }
        _ => Err("usage: serve ADDRESS ID | check ADDRESS | check|limited|sample discovery".into()),
    }
}
