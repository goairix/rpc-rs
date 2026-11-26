//! Revision-consistent service discovery and routing for tonic clients.
mod balancer;
mod subscription;
pub use balancer::LbPolicy;
pub use subscription::{DiscoveryError, Snapshot, Subscription};
mod discovery;
mod transport;
pub use discovery::Discovery;
pub use transport::{DiscoveredService, DiscoveryOptions};
#[cfg(test)]
mod tls_test;
