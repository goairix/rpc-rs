use super::EtcdConfig;
use crate::{DiscoveryError, DiscoverySubscription, Resolver, Snapshot};
use crate::{ServiceMetadata, ServiceStatus};
use etcd_client::{Client, EventType, GetOptions, KeyValue, WatchOptions};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, interval_at, sleep},
};
use tokio_util::sync::CancellationToken;

type Members = BTreeMap<String, ServiceMetadata>;
type Refresh = oneshot::Sender<Result<(), DiscoveryError>>;

/// Owns one service-prefix watch and publishes complete, revision-consistent snapshots.
pub struct Subscription {
    updates: watch::Receiver<Snapshot>,
    refresh: mpsc::Sender<Refresh>,
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl Subscription {
    /// Read a linearizable initial snapshot, then start the watch at the next revision.
    pub async fn connect(
        config: &EtcdConfig,
        service: &str,
        resync: Duration,
    ) -> Result<Self, DiscoveryError> {
        if resync.is_zero() || service.is_empty() || service.contains('/') {
            return Err(DiscoveryError(format!(
                "invalid service name or resync interval for {service:?}"
            )));
        }
        let client = config
            .connect_client()
            .await
            .map_err(|e| DiscoveryError(format!("{service}: connect: {e}")))?;
        let mut state = State {
            client,
            prefix: format!("/{}/{service}/", config.namespace.trim_matches('/')),
            next_revision: 0,
            members: Members::new(),
        };
        state.reload().await.map_err(|e| state.snapshot_error(&e))?;
        let (output, updates) = watch::channel(Ok(state.members.clone()));
        let (refresh, requests) = mpsc::channel(1);
        let stop = CancellationToken::new();
        let token = stop.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = token.cancelled() => {},
                () = state.run(output, requests, resync) => {},
            }
        });
        Ok(Self {
            updates,
            refresh,
            stop,
            task: Some(task),
        })
    }

    /// Subscribe to the current snapshot and subsequent changes.
    pub fn snapshots(&self) -> watch::Receiver<Snapshot> {
        self.updates.clone()
    }

    /// Wait for a new linearizable full read and publication.
    pub async fn refresh(&self) -> Result<(), DiscoveryError> {
        let (reply, done) = oneshot::channel();
        self.refresh
            .send(reply)
            .await
            .map_err(|_| self.closed_error())?;
        done.await.map_err(|_| self.closed_error())?
    }

    fn closed_error(&self) -> DiscoveryError {
        self.updates
            .borrow()
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(|| DiscoveryError("discovery closed".into()))
    }

    /// Cancel outstanding I/O and wait for the subscription task to exit.
    pub async fn close(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

#[tonic::async_trait]
impl Resolver for EtcdConfig {
    async fn subscribe(
        &self,
        service: &str,
        resync: Duration,
    ) -> Result<Box<dyn DiscoverySubscription>, DiscoveryError> {
        Ok(Box::new(
            Subscription::connect(self, service, resync).await?,
        ))
    }
}

#[tonic::async_trait]
impl DiscoverySubscription for Subscription {
    fn snapshots(&self) -> watch::Receiver<Snapshot> {
        self.snapshots()
    }
    async fn refresh(&self) -> Result<(), DiscoveryError> {
        self.refresh().await
    }
    async fn close(&mut self) {
        self.close().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

struct State {
    client: Client,
    prefix: String,
    /// First revision not yet applied, including across transient reconnects.
    next_revision: i64,
    members: Members,
}

impl State {
    fn put(&mut self, kv: &KeyValue) {
        let Ok(key) = kv.key_str() else { return };
        let Some(id) = key
            .strip_prefix(&self.prefix)
            .filter(|id| !id.is_empty() && !id.contains('/'))
        else {
            return;
        };
        match serde_json::from_slice::<ServiceMetadata>(kv.value()) {
            Ok(m) if m.status == ServiceStatus::Up && !m.address.is_empty() => {
                self.members.insert(id.into(), m);
            }
            Ok(_) => {
                self.members.remove(id);
            }
            Err(error) => {
                tracing::warn!(key, %error, "invalid service registration");
                self.members.remove(id);
            }
        }
    }

    fn snapshot_error(&self, error: &etcd_client::Error) -> DiscoveryError {
        DiscoveryError(format!("{}: snapshot: {error}", self.prefix))
    }

    async fn reload(&mut self) -> Result<(), etcd_client::Error> {
        let response = self
            .client
            .get(
                self.prefix.as_bytes(),
                Some(GetOptions::new().with_prefix()),
            )
            .await?;
        self.next_revision = response
            .header()
            .ok_or_else(|| etcd_client::Error::InvalidArgs("missing snapshot revision".into()))?
            .revision()
            + 1;
        self.members.clear();
        for kv in response.kvs() {
            self.put(kv);
        }
        Ok(())
    }

    async fn follow(
        &mut self,
        output: &watch::Sender<Snapshot>,
    ) -> Result<End, etcd_client::Error> {
        let opts = WatchOptions::new()
            .with_prefix()
            .with_start_revision(self.next_revision);
        let mut stream = self
            .client
            .watch(self.prefix.as_bytes(), Some(opts))
            .await?;
        while let Some(response) = stream.message().await? {
            if response.compact_revision() > 0 {
                return Ok(End::Compacted);
            }
            if response.canceled() {
                return Ok(End::Fatal(response.cancel_reason().to_owned()));
            }
            // WatchCreated may report a newer header before replay has started. Only
            // advance from actual events and apply all entries of a transaction together.
            let mut next = self.next_revision;
            for event in response.events() {
                let Some(kv) = event.kv() else { continue };
                if kv.mod_revision() < self.next_revision {
                    continue;
                }
                next = next.max(kv.mod_revision() + 1);
                match event.event_type() {
                    EventType::Put => self.put(kv),
                    EventType::Delete => {
                        if let Some(id) =
                            kv.key_str().ok().and_then(|s| s.strip_prefix(&self.prefix))
                        {
                            self.members.remove(id);
                        }
                    }
                }
            }
            if next != self.next_revision {
                self.next_revision = next;
                let _ = output.send_replace(Ok(self.members.clone()));
            }
        }
        Ok(End::Disconnected)
    }

    async fn run(
        &mut self,
        output: watch::Sender<Snapshot>,
        mut requests: mpsc::Receiver<Refresh>,
        resync: Duration,
    ) {
        let mut periodic = interval_at(Instant::now() + resync, resync);
        periodic.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut backoff = Duration::ZERO;
        let mut reload = false;
        loop {
            let revision = self.next_revision;
            let mut reply = None;
            tokio::select! {
                request = requests.recv() => {
                    let Some(request) = request else { return };
                    reply = Some(request);
                },
                _ = periodic.tick() => {},
                outcome = async {
                    sleep(backoff).await;
                    if reload { return Ok(End::Compacted) }
                    self.follow(&output).await
                } => {
                    match outcome {
                        Ok(End::Fatal(reason)) => {
                            let _ = output.send_replace(Err(DiscoveryError(format!("{}: watch canceled: {reason}", self.prefix))));
                            return;
                        }
                        Ok(End::Compacted) => {},
                        Err(error) if permanent(&error) => {
                            let _ = output.send_replace(Err(DiscoveryError(format!("{}: watch: {error}", self.prefix))));
                            return;
                        }
                        other => {
                            tracing::warn!(prefix=%self.prefix, ?other, "watch disconnected");
                            if revision != self.next_revision { backoff = Duration::ZERO; }
                            backoff = increase(backoff);
                            continue;
                        }
                    }
                }
            }
            let result = match self.reload().await {
                Ok(()) => {
                    let _ = output.send_replace(Ok(self.members.clone()));
                    Ok(())
                }
                Err(cause) => {
                    let error = self.snapshot_error(&cause);
                    tracing::warn!(%error, "snapshot refresh failed");
                    if permanent(&cause) {
                        // The authority can no longer confirm this membership. Withdraw the
                        // last snapshot for all clients, including callers that never refresh.
                        let _ = output.send_replace(Err(error.clone()));
                        if let Some(reply) = reply {
                            let _ = reply.send(Err(error));
                        }
                        return;
                    }
                    Err(error)
                }
            };
            reload = result.is_err();
            if let Some(reply) = reply {
                let _ = reply.send(result);
            }
            backoff = if reload {
                increase(backoff)
            } else {
                Duration::ZERO
            };
        }
    }
}

fn permanent(error: &etcd_client::Error) -> bool {
    match error {
        etcd_client::Error::InvalidArgs(_) => true,
        etcd_client::Error::GRpcStatus(status) => matches!(
            status.code(),
            tonic::Code::PermissionDenied
                | tonic::Code::Unauthenticated
                | tonic::Code::InvalidArgument
        ),
        _ => false,
    }
}

fn increase(d: Duration) -> Duration {
    (d * 2).clamp(Duration::from_millis(100), Duration::from_secs(5))
}

#[derive(Debug)]
enum End {
    Compacted,
    Disconnected,
    Fatal(String),
}
