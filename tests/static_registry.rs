use rpc_rs::naming::{
    Registration, RegistrationOptions, Registry, Resolver, static_registry::StaticRegistry,
};
use std::time::Duration;

const DIRECTORY: &str = r#"{"instances":[
  {"service_name":"echo","ip":"127.0.0.1","port":5051},
  {"service_name":"echo","ip":"::1","port":5052,"weight":3},
  {"service_name":"billing","ip":"127.0.0.1","port":5051}
]}"#;

#[tokio::test]
async fn membership_and_registration_are_scoped_by_name_and_address() {
    let registry = StaticRegistry::from_reader(DIRECTORY.as_bytes()).unwrap();
    let mut echo = registry.subscribe("echo", Duration::ZERO).await.unwrap();
    let updates = echo.snapshots();
    {
        let view = updates.borrow();
        let members = view.as_ref().unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(members["127.0.0.1:5051"].weight, 100);
        assert_eq!(members["[::1]:5052"].weight, 3);
    }
    let register = |name: &str, address: &str| {
        registry.register(RegistrationOptions {
            service_name: name.into(),
            address: address.into(),
            ..Default::default()
        })
    };
    assert_eq!(
        register("echo", "[0:0:0:0:0:0:0:1]:5052").await.unwrap(),
        Registration::Existing
    );
    assert!(register("billing", "[::1]:5052").await.is_err());
    assert!(register("typo", "127.0.0.1:5051").await.is_err());
    assert!(registry.subscribe("typo", Duration::ZERO).await.is_err());
    echo.refresh().await.unwrap();
    echo.close().await;
    assert!(echo.refresh().await.is_err());
    assert!(updates.has_changed().is_err());
}

#[test]
fn rejects_bad_files_before_exposing_a_partial_directory() {
    let entry = r#"{"service_name":"echo","ip":"127.0.0.1","port":5051}"#;
    for entries in [
        format!("{entry},{entry}"),
        entry.replace("5051", "0"),
        entry.replace("5051", "65536"),
        entry.replace("127.0.0.1", "0.0.0.0"),
        entry.replace("127.0.0.1", "oops"),
        entry.replace("echo", ""),
        entry.replace("5051", "5051,\"weight\":0"),
    ] {
        let input = format!(r#"{{"instances":[{entries}]}}"#);
        assert!(
            StaticRegistry::from_reader(input.as_bytes()).is_err(),
            "{input}"
        );
    }
}

/// The same service name can resolve through two independent backends in one process.
#[tokio::test]
#[ignore = "requires RPC_ETCD_ENDPOINT"]
async fn concurrent_resolvers_keep_their_own_service_directory() {
    use rpc_rs::naming::etcd::{EtcdConfig, EtcdRegistry};
    use std::time::{SystemTime, UNIX_EPOCH};

    let config = EtcdConfig {
        endpoints: vec![std::env::var("RPC_ETCD_ENDPOINT").unwrap()],
        namespace: format!(
            "mixed-resolvers-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
        ..Default::default()
    };
    let registry = EtcdRegistry::connect(config.clone()).await.unwrap();
    registry
        .register(RegistrationOptions {
            service_name: "billing".into(),
            address: "127.0.0.1:6060".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let directory = StaticRegistry::from_reader(DIRECTORY.as_bytes()).unwrap();
    let (mut from_file, mut from_etcd) = tokio::try_join!(
        directory.subscribe("billing", Duration::from_secs(60)),
        config.subscribe("billing", Duration::from_secs(60)),
    )
    .unwrap();

    for (subscription, expected) in [
        (&from_file, "127.0.0.1:5051"),
        (&from_etcd, "127.0.0.1:6060"),
    ] {
        let updates = subscription.snapshots();
        let snapshot = updates.borrow();
        let instances = snapshot.as_ref().unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances.values().next().unwrap().address, expected);
    }
    from_etcd.close().await;
    registry.close().await.unwrap();
    // Releasing the dynamic backend must leave the other subscription usable.
    from_file.refresh().await.unwrap();
    from_file.close().await;
}
