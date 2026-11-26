//! Exercise the public backend interfaces through a generated client and a real listener.
use rpc_rs::{
    discovery::{Discovery, DiscoveryError, DiscoverySubscription, Resolver, Snapshot},
    registry::{
        Registration, RegistrationHandle, RegistrationOptions, Registry, RegistryError,
        ServiceMetadata, ServiceStatus,
    },
    server::RpcServer,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::watch,
    time::{sleep, timeout},
};
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
};

// A single-service fixture. The sender always contains the authoritative membership.
struct Directory(watch::Sender<Snapshot>);
const NAME: &str = "grpc.reflection.v1.ServerReflection";
const ID: &str = "reflection-instance";

#[tonic::async_trait]
impl Registry for Directory {
    async fn register(&self, opts: RegistrationOptions) -> Result<Registration, RegistryError> {
        assert_eq!(opts.service_name, NAME);
        let created = self.0.send_if_modified(|snapshot| {
            let members = snapshot.as_mut().unwrap();
            if members.contains_key(ID) {
                return false;
            }
            members.insert(
                ID.into(),
                ServiceMetadata {
                    service_name: opts.service_name,
                    address: opts.address,
                    instance_id: ID.into(),
                    status: ServiceStatus::Up,
                    ..Default::default()
                },
            );
            true
        });
        Ok(if created {
            Registration::Created(RegistrationHandle {
                service_name: NAME.into(),
                instance_id: ID.into(),
            })
        } else {
            Registration::Existing
        })
    }
    async fn unregister_handle(&self, handle: &RegistrationHandle) -> Result<(), RegistryError> {
        assert_eq!(handle.service_name, NAME);
        self.0.send_modify(|s| {
            s.as_mut().unwrap().remove(&handle.instance_id);
        });
        Ok(())
    }
}

#[tonic::async_trait]
impl Resolver for Directory {
    async fn subscribe(
        &self,
        service: &str,
        _: Duration,
    ) -> Result<Box<dyn DiscoverySubscription>, DiscoveryError> {
        assert_eq!(service, NAME);
        Ok(Box::new(LocalSubscription(self.0.subscribe())))
    }
}

struct LocalSubscription(watch::Receiver<Snapshot>);

#[tonic::async_trait]
impl DiscoverySubscription for LocalSubscription {
    fn snapshots(&self) -> watch::Receiver<Snapshot> {
        self.0.clone()
    }
    async fn refresh(&self) -> Result<(), DiscoveryError> {
        self.0.borrow().clone().map(|_| ())
    }
    async fn close(&mut self) {}
}

/// Bound the wait for asynchronous route reconciliation after each membership change.
async fn wait_ready(discovery: &Discovery, expected: usize) {
    timeout(Duration::from_secs(5), async {
        while discovery.ready_count() != expected {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn register_resolve_call_and_withdraw_without_etcd() {
    let directory = Arc::new(Directory(watch::channel(Ok(Default::default())).0));
    let mut discovery = Discovery::connect(directory.as_ref(), NAME, Default::default())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reflection = tonic_reflection::server::Builder::configure()
        .build_v1()
        .unwrap();
    let server = RpcServer::default()
        .add_service(reflection, Default::default())
        .start(listener, Some(directory.clone()));
    wait_ready(&discovery, 1).await;
    let mut client = ServerReflectionClient::new(discovery.service());
    let req = ServerReflectionRequest {
        message_request: Some(MessageRequest::ListServices(String::new())),
        ..Default::default()
    };
    let mut replies = client
        .server_reflection_info(tokio_stream::iter([req]))
        .await
        .unwrap()
        .into_inner();
    assert!(replies.message().await.unwrap().is_some());
    drop(replies);

    for (status, ready) in [(ServiceStatus::Down, 0), (ServiceStatus::Up, 1)] {
        directory
            .0
            .send_modify(|s| s.as_mut().unwrap().get_mut(ID).unwrap().status = status);
        discovery.refresh().await.unwrap();
        wait_ready(&discovery, ready).await;
    }
    server.shutdown().await.unwrap();
    assert!(directory.0.borrow().as_ref().unwrap().is_empty());
    wait_ready(&discovery, 0).await;
    discovery.close().await;
}
