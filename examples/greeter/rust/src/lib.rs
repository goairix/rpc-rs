//! Shared Greeter protocol and example configuration.
pub mod pb {
    tonic::include_proto!("example.v1");
}

/// Static directory used when the example runs without an etcd endpoint.
pub const SERVICES_FILE: &str = "examples/greeter/services.json";

/// Read this example's optional endpoint. The library itself takes its backend from the caller.
pub fn etcd_config() -> Result<Option<rpc_rs::naming::etcd::EtcdConfig>, std::env::VarError> {
    match std::env::var("GREETER_ETCD_ENDPOINT") {
        Ok(endpoint) => Ok(Some(rpc_rs::naming::etcd::EtcdConfig {
            endpoints: vec![endpoint],
            ..Default::default()
        })),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    }
}
