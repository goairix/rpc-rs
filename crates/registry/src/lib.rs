//! Compatibility exports for applications using the original registry crate.
#[cfg(feature = "etcd")]
pub use rpc_naming::etcd::{EtcdConfig, EtcdRegistry, WeakEtcdRegistry, config};
pub use rpc_naming::{
    Registration, RegistrationHandle, RegistrationOptions, Registry, RegistryError,
    ServiceMetadata, ServiceStatus, metadata,
};
