//! Service registration and metadata shared with the Go RPC framework.
#[cfg(feature = "etcd")]
pub mod config;
pub mod metadata;
#[cfg(feature = "etcd")]
pub use config::EtcdConfig;
pub use metadata::{RegistrationOptions, ServiceMetadata, ServiceStatus};

/// Identifies exactly the registration this caller created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationHandle {
    /// Fully qualified service name used to locate the local worker.
    pub service_name: String,
    /// Stable instance ID checked before unregistering.
    pub instance_id: String,
}

/// Only a newly created registration grants an ownership handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Registration {
    /// This request established the instance and owns its removal.
    Created(RegistrationHandle),
    /// A previous request already registered this name.
    Existing,
}

/// Register services before serving; only entries created by this server are removed.
#[tonic::async_trait]
pub trait Registry: Send + Sync + 'static {
    /// Publish metadata or report that another owner already registered this name.
    async fn register(&self, options: RegistrationOptions) -> Result<Registration, RegistryError>;
    /// Withdraw exactly the registration represented by this handle.
    async fn unregister_handle(&self, handle: &RegistrationHandle) -> Result<(), RegistryError>;
}

/// A backend, lifecycle or configuration error.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Invalid local configuration or registration.
    #[error("{0}")]
    Invalid(&'static str),
    /// The registry has started closing.
    #[error("registry closed")]
    Closed,
    /// An application-supplied registry failed. The original error is available as its source.
    #[error(transparent)]
    Backend(Box<dyn std::error::Error + Send + Sync>),
    #[cfg(feature = "etcd")]
    /// etcd returned an error.
    #[error(transparent)]
    Etcd(#[from] etcd_client::Error),
    /// An operation did not finish within the configured timeout.
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    /// A background task exited unexpectedly.
    #[error("registry worker exited: {0}")]
    Worker(String),
}

#[cfg(feature = "etcd")]
mod instance;
#[cfg(feature = "etcd")]
mod registry;
#[cfg(feature = "etcd")]
pub use registry::{EtcdRegistry, WeakEtcdRegistry};
