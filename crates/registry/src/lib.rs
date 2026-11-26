//! Service registration and metadata shared with the Go RPC framework.
pub mod config;
pub mod metadata;
pub use config::EtcdConfig;
pub use metadata::{RegistrationOptions, ServiceMetadata, ServiceStatus};

/// An etcd request, lifecycle or configuration error.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// Invalid local configuration or registration.
    #[error("{0}")]
    Invalid(&'static str),
    /// The registry has started closing.
    #[error("registry closed")]
    Closed,
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
