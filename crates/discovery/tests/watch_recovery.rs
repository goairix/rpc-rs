//! An HTTP/2 proxy controls watch availability while forwarding real etcd KV reads.
//! This exercises the normal subscription entry point without changing cluster settings.
use http::{Request, Response};
use rpc_discovery::{Discovery, DiscoveryOptions, Subscription};
use rpc_naming::etcd::EtcdConfig;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, oneshot},
    time::timeout,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Status,
    body::Body,
    server::NamedService,
    transport::{Channel, Endpoint, Server},
};
use tower::{Service, ServiceExt};

#[derive(Clone)]
struct Proxy<const WATCH: bool> {
    upstream: Channel,
    // 0 = forward, 1 = transient disconnect, 2 = watch permission revoked, 3 = KV permission revoked.
    mode: Arc<AtomicUsize>,
    attempted: Arc<Notify>,
}
impl<const WATCH: bool> NamedService for Proxy<WATCH> {
    const NAME: &'static str = if WATCH {
        "etcdserverpb.Watch"
    } else {
        "etcdserverpb.KV"
    };
}
impl<const WATCH: bool> Service<Request<Body>> for Proxy<WATCH> {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let channel = self.upstream.clone();
        if WATCH {
            self.attempted.notify_one();
        }
        let mode = self.mode.load(Ordering::SeqCst);
        Box::pin(async move {
            if !WATCH && mode == 3 {
                return Ok(Status::permission_denied("snapshot permission revoked").into_http());
            }
            if WATCH {
                match mode {
                    1 => return Ok(Status::unavailable("watch transport interrupted").into_http()),
                    2 => {
                        return Ok(
                            Status::permission_denied("watch permission revoked").into_http()
                        );
                    }
                    _ => {}
                }
            }
            Ok(channel.oneshot(request).await.unwrap())
        })
    }
}

#[tokio::test]
#[ignore = "requires RPC_ETCD_ENDPOINT"]
async fn resumes_after_disconnect_resyncs_without_watch_and_exposes_permanent_errors() {
    let endpoint = std::env::var("RPC_ETCD_ENDPOINT").unwrap();
    let upstream = Endpoint::from_shared(endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mode = Arc::new(AtomicUsize::new(1));
    let attempted = Arc::new(Notify::new());
    let watch = Proxy::<true> {
        upstream: upstream.clone(),
        mode: mode.clone(),
        attempted: attempted.clone(),
    };
    let kv = Proxy::<false> {
        upstream,
        mode: mode.clone(),
        attempted: attempted.clone(),
    };
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let (stop, done) = oneshot::channel();
    let proxy = tokio::spawn(async move {
        Server::builder()
            .add_service(kv)
            .add_service(watch)
            .serve_with_incoming_shutdown(TcpListenerStream::new(socket), async {
                let _ = done.await;
            })
            .await
            .unwrap();
    });
    let mut cfg = EtcdConfig {
        namespace: format!("watch-recovery-{}", fastrand::u64(..)),
        endpoints: vec![endpoint],
        ..Default::default()
    };
    let mut client = cfg.connect_client().await.unwrap();
    cfg.endpoints = vec![format!("http://{local}")];
    let key = format!("/{}/echo/peer", cfg.namespace);
    let record =
        |w: i64| serde_json::json!({"address":"localhost:1","status":"up","weight":w}).to_string();
    let mut sub = Subscription::connect(&cfg, "echo", Duration::from_millis(500))
        .await
        .unwrap();
    let mut updates = sub.snapshots();
    timeout(Duration::from_secs(3), attempted.notified())
        .await
        .unwrap();
    // Watch is continuously failing; only the periodic snapshot can see this update.
    client.put(key.clone(), record(4), None).await.unwrap();
    timeout(
        Duration::from_secs(3),
        updates.wait_for(|s| {
            s.as_ref()
                .unwrap()
                .get("peer")
                .is_some_and(|m| m.weight == 4)
        }),
    )
    .await
    .unwrap()
    .unwrap();
    sub.close().await;
    // Disable periodic reads to establish that the next update is replayed by watch.
    let mut sub = Subscription::connect(&cfg, "echo", Duration::from_secs(3600))
        .await
        .unwrap();
    let mut updates = sub.snapshots();
    timeout(Duration::from_secs(3), attempted.notified())
        .await
        .unwrap();
    client.put(key.clone(), record(7), None).await.unwrap();
    mode.store(0, Ordering::SeqCst);
    timeout(
        Duration::from_secs(3),
        updates.wait_for(|s| {
            s.as_ref()
                .unwrap()
                .get("peer")
                .is_some_and(|m| m.weight == 7)
        }),
    )
    .await
    .unwrap()
    .unwrap();
    // Refresh closes the stream, then the new watch receives a permanent error.
    mode.store(2, Ordering::SeqCst);
    sub.refresh().await.unwrap();
    timeout(Duration::from_secs(3), updates.wait_for(|s| s.is_err()))
        .await
        .unwrap()
        .unwrap();
    assert!(
        sub.refresh()
            .await
            .unwrap_err()
            .to_string()
            .contains("watch permission revoked")
    );
    sub.close().await;
    snapshot_denial_withdraws_established_routes(&cfg, &mode, &mut client, &key).await;
    client.delete(key, None).await.unwrap();
    stop.send(()).unwrap();
    proxy.await.unwrap();
}

/// Losing permission on a full read invalidates established routes, whether an
/// application requests that read or the periodic timer initiates it.
async fn snapshot_denial_withdraws_established_routes(
    cfg: &EtcdConfig,
    mode: &AtomicUsize,
    etcd: &mut etcd_client::Client,
    key: &str,
) {
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let (_reporter, health) = tonic_health::server::health_reporter();
    let (stop, done) = oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(health)
            .serve_with_incoming_shutdown(TcpListenerStream::new(socket), async {
                let _ = done.await;
            })
            .await
            .unwrap();
    });
    etcd.put(
        key,
        serde_json::json!({"status":"up", "address":address.to_string()}).to_string(),
        None,
    )
    .await
    .unwrap();
    for explicit in [true, false] {
        mode.store(0, Ordering::SeqCst);
        let mut discovery = Discovery::connect(
            cfg,
            "echo",
            DiscoveryOptions {
                resync: Duration::from_millis(250),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut client = HealthClient::new(discovery.service());
        timeout(Duration::from_secs(3), async {
            while discovery.ready_count() != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        client.check(HealthCheckRequest::default()).await.unwrap();
        mode.store(3, Ordering::SeqCst);
        if explicit {
            assert!(
                discovery
                    .refresh()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("snapshot permission revoked")
            );
        }
        timeout(Duration::from_secs(3), async {
            while discovery.ready_count() != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let status = client
            .check(HealthCheckRequest::default())
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("snapshot permission revoked"));
        discovery.close().await;
    }
    assert!(
        Subscription::connect(cfg, "echo", Duration::from_secs(1))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("snapshot permission revoked")
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}
