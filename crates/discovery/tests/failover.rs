use etcd_client::DeleteOptions;
use rpc_discovery::{DiscoveredService, Discovery, DiscoveryOptions, LbPolicy};
use rpc_registry::EtcdConfig;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::{Code, Request, Status, transport::Server};
use tonic_health::{
    ServingStatus,
    pb::{HealthCheckRequest, health_client::HealthClient},
};

struct Listener {
    address: SocketAddr,
    accepted: Arc<AtomicUsize>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}
impl Listener {
    async fn start(address: &str, status: ServingStatus) -> Self {
        let socket = TcpListener::bind(address).await.unwrap();
        let address = socket.local_addr().unwrap();
        let (reporter, service) = tonic_health::server::health_reporter();
        reporter.set_service_status("", status).await;
        let service =
            tonic::service::interceptor::InterceptedService::new(service, |req: Request<()>| {
                if req.metadata().contains_key("business-error") {
                    Err(Status::unavailable("from handler"))
                } else {
                    Ok(req)
                }
            });
        let (stop, shutdown) = oneshot::channel();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let incoming = TcpListenerStream::new(socket).map(move |conn| {
            if conn.is_ok() {
                count.fetch_add(1, Ordering::Relaxed);
            }
            conn
        });
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            accepted,
            stop,
            task,
        }
    }

    async fn close(self) {
        self.stop.send(()).unwrap();
        timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

async fn call(client: &mut HealthClient<DiscoveredService>, failure: bool) -> Result<i32, Status> {
    let mut req = Request::new(HealthCheckRequest::default());
    if failure {
        req.metadata_mut()
            .insert("business-error", "true".parse().unwrap());
    }
    Ok(timeout(Duration::from_secs(2), client.check(req))
        .await
        .unwrap()?
        .into_inner()
        .status)
}
async fn wait_count(discovery: &Discovery, count: usize) {
    timeout(Duration::from_secs(5), async {
        while discovery.ready_count() != count {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn run_policy(policy: LbPolicy) {
    let cfg = EtcdConfig {
        namespace: format!("failover-{}-{}", std::process::id(), fastrand::u64(..)),
        endpoints: vec![std::env::var("RPC_ETCD_ENDPOINT").unwrap()],
        ..Default::default()
    };
    let mut etcd = cfg.connect_client().await.unwrap();
    let prefix = format!("/{}/echo/", cfg.namespace);
    let mut discovery = Discovery::connect(
        &cfg,
        "echo",
        DiscoveryOptions {
            policy,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut client = HealthClient::new(discovery.service());
    assert_eq!(
        call(&mut client, false).await.unwrap_err().code(),
        Code::Unavailable
    );
    let first = Listener::start("127.0.0.1:0", ServingStatus::Serving).await;
    let second = Listener::start("127.0.0.1:0", ServingStatus::NotServing).await;
    let record = |address: SocketAddr, w: i64| {
        serde_json::json!({"status":"up","address":address.to_string(),"weight":w}).to_string()
    };
    for (id, node) in [("a", &first), ("b", &second)] {
        etcd.put(format!("{prefix}{id}"), record(node.address, 1), None)
            .await
            .unwrap();
        if id == "a" {
            wait_count(&discovery, 1).await;
        }
    }
    wait_count(&discovery, 2).await;
    assert_eq!(
        call(&mut client, true).await.unwrap_err().message(),
        "from handler"
    );
    assert_eq!(discovery.ready_count(), 2);
    // The generated client uses two distinct health values as response instance IDs.
    if policy == LbPolicy::WeightedRoundRobin {
        for (wa, wb) in [(5, 1), (1, 5)] {
            etcd.txn(etcd_client::Txn::new().and_then(vec![
                etcd_client::TxnOp::put(format!("{prefix}a"), record(first.address, wa), None),
                etcd_client::TxnOp::put(format!("{prefix}b"), record(second.address, wb), None),
            ]))
            .await
            .unwrap();
            timeout(Duration::from_secs(3), async {
                loop {
                    let mut count = 0;
                    for _ in 0..6 {
                        count += usize::from(call(&mut client, false).await.unwrap() == 1);
                    }
                    if count == wa as usize {
                        break;
                    }
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(first.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(second.accepted.load(Ordering::Relaxed), 1);
    }
    let selected = call(&mut client, false).await.unwrap();
    let (broken, healthy, status) = if selected == 1 {
        (first, second, ServingStatus::Serving)
    } else {
        (second, first, ServingStatus::NotServing)
    };
    let addr = broken.address;
    broken.close().await;
    wait_count(&discovery, 1).await;
    for _ in 0..6 {
        assert_ne!(call(&mut client, false).await.unwrap(), selected);
    }
    let recovered = Listener::start(&addr.to_string(), status).await;
    wait_count(&discovery, 2).await;
    if policy == LbPolicy::PickFirst {
        assert_ne!(call(&mut client, false).await.unwrap(), selected);
    } else {
        let mut replies = vec![];
        for _ in 0..6 {
            replies.push(call(&mut client, false).await.unwrap());
        }
        assert!(replies.contains(&1) && replies.contains(&2));
    }
    healthy.close().await;
    wait_count(&discovery, 1).await;
    assert_eq!(call(&mut client, false).await.unwrap(), selected);
    recovered.close().await;
    wait_count(&discovery, 0).await;
    let unavailable = call(&mut client, false).await.unwrap_err();
    assert_eq!(unavailable.code(), Code::Unavailable);
    assert!(unavailable.message().contains(&addr.to_string()));
    if policy == LbPolicy::PickFirst {
        let old = Listener::start("127.0.0.1:0", ServingStatus::Serving).await;
        let new = Listener::start("127.0.0.1:0", ServingStatus::NotServing).await;
        let key = format!("{prefix}replacement");
        etcd.put(key.clone(), record(old.address, 1), None)
            .await
            .unwrap();
        wait_count(&discovery, 1).await;
        assert_eq!(call(&mut client, false).await.unwrap(), 1);
        etcd.put(key, record(new.address, 1), None).await.unwrap();
        timeout(Duration::from_secs(3), async {
            while call(&mut client, false).await.ok() != Some(2) {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        old.close().await;
        assert_eq!(call(&mut client, false).await.unwrap(), 2);
        new.close().await;
    }
    discovery.close().await;
    etcd.delete(prefix, Some(DeleteOptions::new().with_prefix()))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires RPC_ETCD_ENDPOINT"]
async fn real_connections_fail_over_under_all_policies() {
    for policy in [
        LbPolicy::PickFirst,
        LbPolicy::RoundRobin,
        LbPolicy::WeightedRoundRobin,
    ] {
        run_policy(policy).await;
    }
}
