use crate::{
    EtcdConfig, RegistrationHandle, RegistrationOptions, RegistryError, ServiceMetadata,
    ServiceStatus,
};
use etcd_client::{Client, EventType, LeaseKeepAliveStream, LeaseKeeper, PutOptions, WatchOptions};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Only the service's command worker owns this handle. Stop joins the watch and revokes the lease.
pub(crate) struct Instance {
    pub(crate) handle: RegistrationHandle,
    stop: CancellationToken,
    task: JoinHandle<Result<(), RegistryError>>,
}

#[derive(Clone)]
struct Context {
    client: Client,
    config: Arc<EtcdConfig>,
    key: String,
    /// Latest governance record; identity fields always come from the initial registration.
    record: Arc<Mutex<ServiceMetadata>>,
    updates: mpsc::Sender<ServiceMetadata>,
}

struct Lease {
    id: i64,
    /// Revision of the Put that attached this lease to the key.
    revision: i64,
    keeper: LeaseKeeper,
    stream: LeaseKeepAliveStream,
}

impl Instance {
    pub(crate) async fn start(
        client: Client,
        config: Arc<EtcdConfig>,
        options: RegistrationOptions,
        updates: mpsc::Sender<ServiceMetadata>,
        stop: CancellationToken,
    ) -> Result<Self, RegistryError> {
        let handle = RegistrationHandle {
            service_name: options.service_name.clone(),
            instance_id: Uuid::new_v4().to_string(),
        };
        let key = format!(
            "/{}/{}/{}",
            config.namespace.trim_matches('/'),
            handle.service_name,
            handle.instance_id
        );
        let record = ServiceMetadata {
            service_name: options.service_name,
            address: options.address,
            instance_id: handle.instance_id.clone(),
            register_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            version: options.version,
            env: options.env,
            tags: options.tags,
            weight: 100,
            status: ServiceStatus::Up,
            ..Default::default()
        };
        let mut ctx = Context {
            client,
            config,
            key,
            record: Arc::new(Mutex::new(record)),
            updates,
        };
        let lease = ctx.establish().await?;
        Ok(Self::launch(ctx, lease, handle, stop))
    }

    /// Start both loops only after the first Put has established the replay cursor.
    fn launch(
        mut ctx: Context,
        lease: Lease,
        handle: RegistrationHandle,
        stop: CancellationToken,
    ) -> Self {
        let token = stop.clone();
        let task = tokio::spawn(async move {
            // Keep the original watch through lease replacement, including the initial Put/watch gap.
            let start_revision = lease.revision + 1;
            let mut watcher = ctx.clone();
            let (_, result) = tokio::join!(
                token.run_until_cancelled(watcher.watch(start_revision)),
                ctx.maintain(lease, &token)
            );
            result
        });
        Self { handle, stop, task }
    }

    pub(crate) async fn stop(self) -> Result<(), RegistryError> {
        self.stop.cancel();
        self.task
            .await
            .map_err(|e| RegistryError::Worker(e.to_string()))?
    }
}

impl Context {
    /// Finish or roll back each grant even when the caller stops waiting for registration.
    async fn establish(&mut self) -> Result<Lease, RegistryError> {
        self.establish_with(Client::lease_keep_alive).await
    }

    /// Opening the streaming RPC is part of registration: even if Put succeeded,
    /// failure to establish keepalive must roll the newly granted lease back.
    async fn establish_with(
        &mut self,
        open: impl AsyncFnOnce(
            &mut Client,
            i64,
        )
            -> Result<(LeaseKeeper, LeaseKeepAliveStream), etcd_client::Error>
        + Send,
    ) -> Result<Lease, RegistryError> {
        let id = self
            .client
            .lease_grant(self.config.lease_ttl.as_secs() as i64, None)
            .await?
            .id();
        let result = async {
            let revision = self.put(id).await?;
            let (keeper, stream) =
                timeout(self.config.timeout, open(&mut self.client, id)).await??;
            Ok(Lease {
                id,
                revision,
                keeper,
                stream,
            })
        }
        .await;
        if result.is_err() {
            let _ = self.revoke(id).await;
        }
        result
    }

    async fn put(&mut self, id: i64) -> Result<i64, RegistryError> {
        let value = serde_json::to_vec(&*self.record.lock().unwrap())
            .expect("registration fields serialize to JSON");
        let put = self
            .client
            .put(
                self.key.clone(),
                value,
                Some(PutOptions::new().with_lease(id)),
            )
            .await?;
        Ok(put
            .header()
            .ok_or(RegistryError::Invalid("missing Put revision"))?
            .revision())
    }

    async fn revoke(&mut self, id: i64) -> Result<(), RegistryError> {
        match self.client.lease_revoke(id).await {
            Ok(_) => Ok(()),
            Err(etcd_client::Error::GRpcStatus(s)) if s.code() as i32 == 5 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Own the current lease until it is revoked or replaced. Cancellation during an in-flight
    /// grant is checked after its bounded establishment, so a newly granted lease is accounted for.
    async fn maintain(
        &mut self,
        mut lease: Lease,
        stop: &CancellationToken,
    ) -> Result<(), RegistryError> {
        loop {
            let interval = self.config.lease_ttl / 3;
            let keepalive = async {
                loop {
                    sleep(interval).await;
                    lease.keeper.keep_alive().await?;
                    match timeout(self.config.timeout, lease.stream.message()).await?? {
                        Some(response) if response.ttl() > 0 => {}
                        _ => return Err::<(), _>(RegistryError::Invalid("lease keepalive ended")),
                    }
                }
            };
            if stop.run_until_cancelled(keepalive).await.is_none() {
                return self.revoke(lease.id).await;
            }
            // A broken stream may still own a live lease. Revoke before attaching a replacement.
            let _ = self.revoke(lease.id).await;
            let mut delay = BACKOFF;
            loop {
                if stop.run_until_cancelled(sleep(delay)).await.is_none() {
                    return Ok(());
                }
                match self.establish().await {
                    Ok(new) => {
                        lease = new;
                        break;
                    }
                    Err(error) => tracing::warn!(%error, key = %self.key, "lease recovery failed"),
                }
                delay = (delay * 2).min(MAX_BACKOFF);
            }
        }
    }

    async fn update(&self, value: &[u8]) {
        let mut new = match serde_json::from_slice::<ServiceMetadata>(value) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(%error, key = %self.key, "invalid registration");
                return;
            }
        };
        {
            let mut old = self.record.lock().unwrap();
            new.service_name.clone_from(&old.service_name);
            new.address.clone_from(&old.address);
            new.instance_id.clone_from(&old.instance_id);
            new.register_time = old.register_time;
            *old = new.clone();
        }
        let _ = self.updates.send(new).await;
    }

    async fn watch(&mut self, mut next_revision: i64) {
        let mut delay = BACKOFF;
        loop {
            if let Err(error) = self.watch_once(&mut next_revision, &mut delay).await {
                tracing::warn!(%error, key = %self.key, "registration watch disconnected");
            }
            sleep(delay).await;
            delay = (delay * 2).min(MAX_BACKOFF);
        }
    }

    /// The cursor points to the next event revision. Watch-created headers can be newer
    /// than unconsumed historical events and must never advance this cursor.
    async fn watch_once(
        &mut self,
        next: &mut i64,
        delay: &mut Duration,
    ) -> Result<(), RegistryError> {
        let mut stream = self
            .client
            .watch(
                self.key.clone(),
                Some(WatchOptions::new().with_start_revision(*next)),
            )
            .await?;
        while let Some(response) = stream.message().await? {
            if response.compact_revision() > 0 {
                let current = self.client.get(self.key.clone(), None).await?;
                if let Some(kv) = current.kvs().first() {
                    self.update(kv.value()).await;
                }
                *next = current
                    .header()
                    .ok_or(RegistryError::Invalid("missing Get revision"))?
                    .revision()
                    + 1;
                return Ok(());
            }
            if response.canceled() {
                return Ok(());
            }
            for event in response.events() {
                if let Some(kv) = event.kv() {
                    if event.event_type() == EventType::Put {
                        self.update(kv.value()).await;
                    }
                    *next = (*next).max(kv.mod_revision() + 1);
                }
            }
            if !response.events().is_empty() {
                *delay = BACKOFF;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "registration_tests.rs"]
mod tests;
