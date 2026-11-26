//! Revision-consistent service discovery and routing for tonic clients.
mod balancer;
pub use balancer::LbPolicy;
#[cfg(feature = "etcd")]
pub use rpc_naming::etcd::Subscription;
pub use rpc_naming::{DiscoveryError, DiscoverySubscription, Resolver, Snapshot};
mod discovery;
mod transport;
pub use discovery::Discovery;
pub use transport::{DiscoveredService, DiscoveryOptions};
#[cfg(test)]
mod tls_test;
