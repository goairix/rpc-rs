//! Backend interface for resolving a service name into a changing set of instances.
use crate::ServiceMetadata;
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::watch;

/// A lookup, subscription or discovery lifecycle failure.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct DiscoveryError(pub String);

/// Complete membership keyed by stable instance ID. An error terminates routing.
pub type Snapshot = Result<BTreeMap<String, ServiceMetadata>, DiscoveryError>;

/// Opens an independent subscription for each discovered service.
#[tonic::async_trait]
pub trait Resolver: Send + Sync {
    /// Return a subscription with an initial snapshot already available. Polling backends
    /// can use the requested resync interval to reconcile their local view.
    async fn subscribe(
        &self,
        service: &str,
        resync: Duration,
    ) -> Result<Box<dyn DiscoverySubscription>, DiscoveryError>;
}

/// Owns the resources used to observe one service. Drop should signal any background
/// work to stop. Explicit close also waits for that work to finish.
#[tonic::async_trait]
pub trait DiscoverySubscription: Send + Sync {
    /// Each value replaces the entire membership; a closed sender ends routing.
    fn snapshots(&self) -> watch::Receiver<Snapshot>;
    /// Reconcile the current membership with the backend before returning.
    async fn refresh(&self) -> Result<(), DiscoveryError>;
    /// Release this subscription without closing the shared backend.
    async fn close(&mut self);
}
