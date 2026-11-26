//! Revision-consistent service discovery and routing for tonic clients.
mod balancer;
mod subscription;
pub use balancer::LbPolicy;
pub use subscription::{DiscoveryError, Snapshot, Subscription};
