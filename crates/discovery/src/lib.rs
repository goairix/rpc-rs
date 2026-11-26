//! Revision-consistent service discovery and routing for tonic clients.
mod balancer;
mod resolver;
pub use resolver::{DiscoveryError, DiscoverySubscription, Resolver, Snapshot};
mod subscription;
pub use balancer::LbPolicy;
pub use subscription::Subscription;
mod discovery;
mod transport;
pub use discovery::Discovery;
pub use transport::{DiscoveredService, DiscoveryOptions};
#[cfg(test)]
mod tls_test;
