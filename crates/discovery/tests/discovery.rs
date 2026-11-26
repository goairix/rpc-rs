use etcd_client::{DeleteOptions, Txn, TxnOp};
use rpc_discovery::Subscription;
use rpc_naming::etcd::EtcdConfig;
use std::time::Duration;

/// Exercises linearized startup, same-revision updates, filtering, refresh and deletion.
#[tokio::test]
#[ignore = "requires RPC_ETCD_ENDPOINT"]
async fn snapshots_and_transactions() {
    let cfg = EtcdConfig {
        namespace: format!("rpc-discovery-{}-{}", std::process::id(), fastrand::u64(..)),
        endpoints: vec![std::env::var("RPC_ETCD_ENDPOINT").unwrap()],
        ..Default::default()
    };
    let prefix = format!("/{}/echo/", cfg.namespace);
    let mut client = cfg.connect_client().await.unwrap();
    let value = |status: &str, weight: i64| {
        serde_json::json!({"address":"localhost:1","status":status,"weight":weight}).to_string()
    };
    client
        .put(format!("{prefix}a"), value("up", 1), None)
        .await
        .unwrap();
    let mut sub = Subscription::connect(&cfg, "echo", Duration::from_secs(3600))
        .await
        .unwrap();
    let mut rx = sub.snapshots();
    assert_eq!(rx.borrow_and_update().as_ref().unwrap()["a"].weight, 1);
    client
        .txn(Txn::new().and_then(vec![
            TxnOp::put(format!("{prefix}a"), value("up", 5), None),
            TxnOp::put(format!("{prefix}b"), value("up", 1), None),
            TxnOp::put(format!("{prefix}c"), value("down", 1), None),
            TxnOp::put(format!("{prefix}d"), "invalid", None),
        ]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), rx.changed())
        .await
        .unwrap()
        .unwrap();
    {
        let updated = rx.borrow_and_update();
        let updated = updated.as_ref().unwrap();
        assert_eq!(updated.len(), 2);
        assert_eq!(updated["a"].weight, 5);
    }
    sub.refresh().await.unwrap();
    client
        .delete(prefix, Some(DeleteOptions::new().with_prefix()))
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        rx.wait_for(|s| s.as_ref().unwrap().is_empty()),
    )
    .await
    .unwrap()
    .unwrap();
    sub.close().await;
}
