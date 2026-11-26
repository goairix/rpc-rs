//! Go-compatible etcd registration, leases and revision-based name resolution.
pub mod config;
mod instance;
mod registry;
mod subscription;
pub use config::EtcdConfig;
pub use registry::{EtcdRegistry, WeakEtcdRegistry};
pub use subscription::Subscription;
