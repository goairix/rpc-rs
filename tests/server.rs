use rpc_rs::{
    discovery::Discovery,
    governance::TokenBucket,
    registry::{
        EtcdConfig, EtcdRegistry, Registration, RegistrationHandle, RegistrationOptions,
        RegistryError,
    },
    server::{Registry, RpcServer},
};
use std::{
    convert::Infallible,
    future::{Ready, ready},
    net::SocketAddr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc},
    time::{sleep, timeout},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status, body::Body, server::NamedService, transport::Endpoint};
use tonic_health::pb::{
    HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
};
use tower::Service;

const LIMIT: Duration = Duration::from_secs(8);

async fn listener() -> (TcpListener, SocketAddr) {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    (socket, addr)
}

fn list_services() -> ServerReflectionRequest {
    ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }
}

async fn connect(addr: SocketAddr) -> tonic::transport::Channel {
    Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(LIMIT)
        .timeout(LIMIT)
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
async fn health_reflection_and_unary_only_rate_limit() {
    let (socket, addr) = listener().await;
    let server = RpcServer::default()
        .with_reflection()
        .with_limiter(TokenBucket::new(1.0, 1))
        .start(socket, None);
    let channel = connect(addr).await;
    let mut client = HealthClient::new(channel.clone());
    let request = HealthCheckRequest {
        service: "unknown.service".into(),
    };
    assert_eq!(
        client
            .check(request.clone())
            .await
            .unwrap()
            .into_inner()
            .status(),
        ServingStatus::Serving
    );
    assert_eq!(
        client.check(request).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let mut reflect = ServerReflectionClient::new(channel);
    let mut stream = reflect
        .server_reflection_info(tokio_stream::iter([list_services()]))
        .await
        .unwrap()
        .into_inner();
    let result = stream
        .message()
        .await
        .unwrap()
        .unwrap()
        .message_response
        .unwrap();
    let MessageResponse::ListServicesResponse(list) = result else {
        panic!("expected services");
    };
    assert!(
        list.service
            .iter()
            .any(|s| s.name == "grpc.health.v1.Health")
    );
    assert!(stream.message().await.unwrap().is_none());
    server.shutdown().await.unwrap();
}

// Three distinct mounted routes exercise registration ownership independently of the builtin health route.
#[derive(Clone)]
struct Stub<const N: usize>;

impl<const N: usize> NamedService for Stub<N> {
    const NAME: &'static str = ["test.A", "test.B", "test.C"][N];
}

impl<const N: usize> Service<http::Request<Body>> for Stub<N> {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: http::Request<Body>) -> Self::Future {
        ready(Ok(Status::unimplemented("stub").into_http()))
    }
}

#[tokio::test]
async fn missing_descriptor_returns_startup_error_and_releases_listener() {
    let (socket, addr) = listener().await;
    let server = RpcServer::default()
        .add_service(Stub::<0>, Default::default())
        .with_limiter(TokenBucket::new(10.0, 5))
        .start(socket, None);
    let error = timeout(LIMIT, server.wait()).await.unwrap().unwrap_err();
    assert!(error.to_string().contains("test.A"));
    TcpListener::bind(addr).await.unwrap();
}

#[derive(Default)]
struct FailingWithdrawal {
    withdrawn: Notify,
}

#[tonic::async_trait]
impl Registry for FailingWithdrawal {
    async fn register(&self, options: RegistrationOptions) -> Result<Registration, RegistryError> {
        Ok(Registration::Created(RegistrationHandle {
            service_name: options.service_name,
            instance_id: "owned".into(),
        }))
    }
    async fn unregister_handle(&self, handle: &RegistrationHandle) -> Result<(), RegistryError> {
        assert_eq!(handle.instance_id, "owned");
        self.withdrawn.notify_one();
        Err(RegistryError::Invalid("withdrawal failure"))
    }
}

#[tokio::test]
async fn graceful_shutdown_drains_active_stream_even_if_withdrawal_fails() {
    let registry = Arc::new(FailingWithdrawal::default());
    let (socket, addr) = listener().await;
    let reflected = tonic_reflection::server::Builder::configure()
        .build_v1()
        .unwrap();
    let server = RpcServer::default()
        .add_service(reflected, Default::default())
        .start(socket, Some(registry.clone()));
    let channel = connect(addr).await;
    let mut client = ServerReflectionClient::new(channel);
    let (requests, rx) = mpsc::channel(1);
    requests.send(list_services()).await.unwrap();
    let mut responses = client
        .server_reflection_info(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert!(responses.message().await.unwrap().is_some());
    let shutting_down = tokio::spawn(server.shutdown());
    timeout(LIMIT, registry.withdrawn.notified()).await.unwrap();
    assert!(!shutting_down.is_finished());
    requests.send(list_services()).await.unwrap();
    drop(requests);
    assert!(responses.message().await.unwrap().is_some());
    assert!(responses.message().await.unwrap().is_none());
    let error = timeout(LIMIT, shutting_down)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("withdrawal failure"));
    TcpListener::bind(addr).await.unwrap();
}

/// Inject the final registration failure after recording the newly published B lease.
struct FailThird {
    registry: EtcdRegistry,
    created_b: Mutex<Option<(String, i64)>>,
}
#[tonic::async_trait]
impl Registry for FailThird {
    async fn register(&self, options: RegistrationOptions) -> Result<Registration, RegistryError> {
        if options.service_name == "test.C" {
            return Err(RegistryError::Invalid("C failed"));
        }
        let registration = self.registry.register(options).await?;
        if let Registration::Created(handle) = &registration {
            let key = format!(
                "/{}/{}/{}",
                self.registry.namespace(),
                handle.service_name,
                handle.instance_id
            );
            let response = self.registry.client().get(key.clone(), None).await?;
            let kv = response.kvs().first().expect("B must have been published");
            *self.created_b.lock().unwrap() = Some((key, kv.lease()));
        }
        Ok(registration)
    }
    async fn unregister_handle(&self, handle: &RegistrationHandle) -> Result<(), RegistryError> {
        self.registry.unregister_handle(handle).await
    }
}

#[tokio::test]
#[ignore = "requires RPC_ETCD_ENDPOINT"]
async fn failed_start_revokes_only_created_registration_and_lease() {
    let config = EtcdConfig {
        namespace: format!(
            "server-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
        endpoints: vec![std::env::var("RPC_ETCD_ENDPOINT").unwrap()],
        ..Default::default()
    };
    let registry = EtcdRegistry::connect(config.clone()).await.unwrap();
    let (first_socket, first_addr) = listener().await;
    let first_server = RpcServer::default().start(first_socket, None);
    let initial = registry
        .register(RegistrationOptions {
            service_name: "test.A".into(),
            address: first_addr.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    let Registration::Created(a) = initial else {
        panic!("expected isolated namespace");
    };
    let key = format!("/{}/test.A/{}", config.namespace, a.instance_id);
    let mut client = config.connect_client().await.unwrap();
    let a_lease = client.get(key.clone(), None).await.unwrap().kvs()[0].lease();
    let injected = Arc::new(FailThird {
        registry: registry.clone(),
        created_b: Mutex::new(None),
    });
    let (socket, addr) = listener().await;
    let server = RpcServer::default()
        .add_service(Stub::<0>, Default::default())
        .add_service(Stub::<1>, Default::default())
        .add_service(Stub::<2>, Default::default())
        .start(socket, Some(injected.clone()));
    let error = timeout(LIMIT, server.wait()).await.unwrap().unwrap_err();
    assert!(error.to_string().contains("C failed"));
    let (b_key, b_lease) = injected.created_b.lock().unwrap().take().unwrap();
    assert!(b_key.starts_with(&format!("/{}/test.B/", config.namespace)));
    assert!(client.get(b_key, None).await.unwrap().kvs().is_empty());
    assert_eq!(
        client
            .lease_time_to_live(b_lease, None)
            .await
            .unwrap()
            .ttl(),
        -1
    );
    assert_eq!(
        client.get(key, None).await.unwrap().kvs()[0].lease(),
        a_lease
    );
    assert!(
        client
            .lease_time_to_live(a_lease, None)
            .await
            .unwrap()
            .ttl()
            > 0
    );
    TcpListener::bind(addr).await.unwrap();

    let mut discovery = Discovery::connect(&config, "test.A", Default::default())
        .await
        .unwrap();
    timeout(LIMIT, async {
        while discovery.ready_count() == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let response = HealthClient::new(discovery.service())
        .check(HealthCheckRequest::default())
        .await
        .unwrap();
    assert_eq!(response.into_inner().status(), ServingStatus::Serving);
    discovery.close().await;
    registry.close().await.unwrap();
    first_server.shutdown().await.unwrap();
}
