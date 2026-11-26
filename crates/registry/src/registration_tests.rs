//! These tests own every endpoint they compact. Run with ETCD_BIN=... cargo test -p rpc-registry -- --ignored.
use super::*;
use crate::{EtcdRegistry, Registration};
use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
};

struct Cluster {
    child: Child,
    data: PathBuf,
    config: EtcdConfig,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data);
    }
}

impl Cluster {
    async fn start() -> Self {
        let bind = || TcpListener::bind("127.0.0.1:0").unwrap();
        let (client, peer) = (bind(), bind());
        let endpoint = format!("http://{}", client.local_addr().unwrap());
        let peer_url = format!("http://{}", peer.local_addr().unwrap());
        drop((client, peer));
        let data = std::env::temp_dir().join(format!("rpc-registry-{}", Uuid::new_v4()));
        let child = Command::new(std::env::var_os("ETCD_BIN").unwrap_or_else(|| "etcd".into()))
            .args(["--name", "registry", "--data-dir"])
            .arg(&data)
            .args([
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer_url,
                "--initial-advertise-peer-urls",
                &peer_url,
                "--initial-cluster",
                &format!("registry={peer_url}"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start a dedicated etcd using ETCD_BIN or PATH");
        let config = EtcdConfig {
            endpoints: vec![endpoint],
            namespace: format!("test-{}", Uuid::new_v4()),
            lease_ttl: Duration::from_secs(2),
            ..Default::default()
        };
        let cluster = Self {
            child,
            data,
            config,
        };
        timeout(Duration::from_secs(12), async {
            loop {
                if let Ok(mut client) = cluster.config.connect_client().await
                    && client.status().await.is_ok()
                {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("etcd ready");
        cluster
    }
}

async fn watch_gap(compact: bool) {
    let cluster = Cluster::start().await;
    let config = Arc::new(cluster.config.clone());
    let mut client = config.connect_client().await.unwrap();
    let (updates, mut changes) = mpsc::channel(8);
    let handle = RegistrationHandle {
        service_name: "example.Echo".into(),
        instance_id: Uuid::new_v4().to_string(),
    };
    let key = format!(
        "/{}/{}/{}",
        config.namespace, handle.service_name, handle.instance_id
    );
    let original = ServiceMetadata {
        service_name: handle.service_name.clone(),
        instance_id: handle.instance_id.clone(),
        register_time: 123456,
        address: "localhost:5432".into(),
        status: ServiceStatus::Up,
        weight: 100,
        ..Default::default()
    };
    let mut ctx = Context {
        client: client.clone(),
        config,
        key: key.clone(),
        record: Arc::new(Mutex::new(original.clone())),
        updates,
    };
    let lease = ctx.establish().await.unwrap();
    let old_lease = lease.id;
    // No watch exists yet. Use the original lease exactly as an external control plane would.
    let mut external = original.clone();
    external.status = ServiceStatus::Down;
    external.weight = 17;
    external.address = "remote-must-not-change-identity".into();
    client
        .put(
            key.clone(),
            serde_json::to_vec(&external).unwrap(),
            Some(PutOptions::new().with_lease(old_lease)),
        )
        .await
        .unwrap();
    if compact {
        let rev = client
            .put("marker", "", None)
            .await
            .unwrap()
            .header()
            .unwrap()
            .revision();
        client.compact(rev, None).await.unwrap();
    }
    let instance = Instance::launch(ctx, lease, handle, CancellationToken::new());
    let record = timeout(Duration::from_secs(5), changes.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, ServiceStatus::Down);
    assert_eq!(record.address, original.address);
    client.lease_revoke(old_lease).await.unwrap();
    timeout(Duration::from_secs(8), async {
        loop {
            if let Some(kv) = client.get(key.clone(), None).await.unwrap().kvs().first() {
                let restored: ServiceMetadata = serde_json::from_slice(kv.value()).unwrap();
                assert_eq!(restored, record);
                assert_ne!(kv.lease(), old_lease);
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("lease restored using current governance record");
    instance.stop().await.unwrap();
    assert!(client.get(key, None).await.unwrap().kvs().is_empty());
}

#[tokio::test]
#[ignore = "starts its own etcd server (requires ETCD_BIN or etcd in PATH)"]
async fn replays_the_put_watch_gap() {
    watch_gap(false).await;
}

#[tokio::test]
#[ignore = "compacts its own etcd server (requires ETCD_BIN or etcd in PATH)"]
async fn reloads_the_compacted_put_watch_gap() {
    watch_gap(true).await;
}

#[tokio::test]
#[ignore = "starts its own etcd server (requires ETCD_BIN or etcd in PATH)"]
async fn idempotent_registration_reentrant_removal_and_stale_owner() {
    let cluster = Cluster::start().await;
    let registry = EtcdRegistry::connect(cluster.config.clone()).await.unwrap();
    let opts = RegistrationOptions {
        service_name: "example.Echo".into(),
        address: "127.0.0.1:1234".into(),
        ..Default::default()
    };
    let (a, b) = tokio::join!(
        registry.register(opts.clone()),
        registry.register(opts.clone())
    );
    let first = match (a.unwrap(), b.unwrap()) {
        (Registration::Created(a), Registration::Existing)
        | (Registration::Existing, Registration::Created(a)) => a,
        other => panic!("unexpected concurrent result {other:?}"),
    };
    let prefix = format!("/{}/example.Echo/", registry.namespace());
    let mut client = registry.client();
    let list = client
        .get(
            prefix.clone(),
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .unwrap();
    assert_eq!(list.kvs().len(), 1);
    let kv = &list.kvs()[0];
    let mut value: ServiceMetadata = serde_json::from_slice(kv.value()).unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    let observer_registry = registry.downgrade();
    registry.on_status_change(move |record| {
        let nested = observer_registry.clone();
        let tx = tx.clone();
        async move {
            if record.status == ServiceStatus::Down
                && let Some(nested) = nested.upgrade()
            {
                nested.unregister(&record.service_name).await.unwrap();
                tx.send(()).await.unwrap();
            }
        }
    });
    value.status = ServiceStatus::Down;
    client
        .put(
            kv.key(),
            serde_json::to_vec(&value).unwrap(),
            Some(PutOptions::new().with_lease(kv.lease())),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(4), rx.recv())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), async {
        while registry.worker_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unregister retires the service command worker");
    assert!(client.get(kv.key(), None).await.unwrap().kvs().is_empty());
    assert!(matches!(
        registry.register(opts.clone()).await.unwrap(),
        Registration::Created(_)
    ));
    registry.unregister_handle(&first).await.unwrap();
    let entries = client
        .get(
            prefix.clone(),
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .unwrap();
    assert_eq!(entries.kvs().len(), 1);
    assert_ne!(entries.kvs()[0].lease(), kv.lease());
    // Stop during the recovery backoff; close must not recreate the revoked key.
    client.lease_revoke(entries.kvs()[0].lease()).await.unwrap();
    let observer_registry = registry.downgrade();
    let (a, b) = tokio::join!(registry.close(), registry.close());
    a.unwrap();
    b.unwrap();
    assert!(observer_registry.upgrade().is_none());
    assert!(registry.register(opts).await.is_err());
    sleep(Duration::from_millis(850)).await;
    assert!(
        client
            .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "starts its own etcd server (requires ETCD_BIN or etcd in PATH)"]
async fn put_failure_revokes_granted_lease() {
    let cluster = Cluster::start().await;
    let registry = EtcdRegistry::connect(cluster.config.clone()).await.unwrap();
    let mut client = registry.client();
    let opts = RegistrationOptions {
        service_name: "Echo".into(),
        address: "localhost:1234".into(),
        tags: vec!["x".repeat(2_000_000)],
        ..Default::default()
    };
    assert!(registry.register(opts).await.is_err());
    assert!(client.leases().await.unwrap().leases().is_empty());
    registry.close().await.unwrap();
}

#[tokio::test]
#[ignore = "starts its own etcd server (requires ETCD_BIN or etcd in PATH)"]
async fn keepalive_setup_failure_revokes_key_and_lease() {
    let cluster = Cluster::start().await;
    let mut client = cluster.config.connect_client().await.unwrap();
    let key = format!("/{}/example.Echo/1", cluster.config.namespace);
    let (updates, _changes) = mpsc::channel(1);
    let mut ctx = Context {
        client: client.clone(),
        config: Arc::new(cluster.config.clone()),
        key: key.clone(),
        record: Arc::new(Mutex::new(ServiceMetadata::default())),
        updates,
    };
    let error = ctx
        .establish_with(async |client, id| {
            let kv = client.get(key.clone(), None).await?;
            assert_eq!(kv.kvs()[0].lease(), id);
            Err(etcd_client::Error::LeaseKeepAliveError(
                "injected stream failure".into(),
            ))
        })
        .await;
    assert!(matches!(
        error,
        Err(RegistryError::Etcd(
            etcd_client::Error::LeaseKeepAliveError(_)
        ))
    ));
    assert!(client.get(key, None).await.unwrap().kvs().is_empty());
    assert!(client.leases().await.unwrap().leases().is_empty());
}

#[tokio::test]
#[ignore = "starts its own etcd server (requires ETCD_BIN or etcd in PATH)"]
async fn callback_allows_last_owner_drop_to_release_registration() {
    let cluster = Cluster::start().await;
    let registry = EtcdRegistry::connect(cluster.config.clone()).await.unwrap();
    let mut client = registry.client();
    let observer_registry = registry.downgrade();
    registry.on_status_change(move |record| {
        let nested = observer_registry.clone();
        async move {
            if record.status == ServiceStatus::Down
                && let Some(nested) = nested.upgrade()
            {
                nested.unregister(&record.service_name).await.unwrap();
            }
        }
    });
    let Registration::Created(handle) = registry
        .register(RegistrationOptions {
            service_name: "example.Echo".into(),
            address: "localhost:1234".into(),
            ..Default::default()
        })
        .await
        .unwrap()
    else {
        panic!("expected a new registration")
    };
    let key = format!(
        "/{}/{}/{}",
        registry.namespace(),
        handle.service_name,
        handle.instance_id
    );
    assert_eq!(client.get(key.as_str(), None).await.unwrap().kvs().len(), 1);
    drop(registry);
    timeout(Duration::from_secs(5), async {
        while !client
            .get(key.as_str(), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
        {
            sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .expect("last owner drop should stop renewing the registration");
}
