//! Backend-independent contracts for publishing and resolving RPC services.
pub mod etcd;
pub mod metadata;
mod registry;
mod resolver;
pub mod static_registry;

pub use metadata::{RegistrationOptions, ServiceMetadata, ServiceStatus};
pub use registry::{
    Registration, RegistrationHandle, Registry, RegistryError, StatusChangeCallback,
};
pub use resolver::{DiscoveryError, DiscoverySubscription, Resolver, Snapshot};
