use super::{EtcdConfig, instance::Instance};
use crate::{
    Registration, RegistrationHandle, RegistrationOptions, Registry, RegistryError, ServiceMetadata,
};
use etcd_client::Client;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

type Callback =
    Arc<dyn Fn(ServiceMetadata) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
type Completion = watch::Receiver<Option<Result<(), String>>>;

#[tonic::async_trait]
impl Registry for EtcdRegistry {
    async fn register(&self, options: RegistrationOptions) -> Result<Registration, RegistryError> {
        EtcdRegistry::register(self, options).await
    }
    async fn unregister_handle(&self, h: &RegistrationHandle) -> Result<(), RegistryError> {
        EtcdRegistry::unregister_handle(self, h).await
    }
}

enum Command {
    Register(
        RegistrationOptions,
        oneshot::Sender<Result<Registration, RegistryError>>,
    ),
    Remove(Option<String>, oneshot::Sender<Result<(), RegistryError>>),
}

struct Worker {
    /// The directory cannot keep an unregistered service mailbox alive.
    sender: mpsc::WeakSender<Command>,
    task: JoinHandle<Result<(), RegistryError>>,
}

struct State {
    /// Registry-owned connection pool, released when close starts. Workers hold their own clones until joined.
    client: Option<Client>,
    workers: HashMap<String, Worker>,
    /// Concurrent close calls observe the same cleanup result.
    close: Option<Completion>,
    dispatch: Option<JoinHandle<()>>,
}

struct Inner {
    config: Arc<EtcdConfig>,
    state: Mutex<State>,
    stop: CancellationToken,
    callbacks: Arc<Mutex<Vec<Callback>>>,
    updates: mpsc::Sender<ServiceMetadata>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// Cloneable registry. Clones share the connection, registrations and close operation.
#[derive(Clone)]
pub struct EtcdRegistry(Arc<Inner>);

/// Non-owning registry reference for callbacks that need to register or remove services.
#[derive(Clone)]
pub struct WeakEtcdRegistry(Weak<Inner>);

impl WeakEtcdRegistry {
    /// Temporarily acquire a handle while the registry is open. A concurrent close can
    /// still cause a subsequent registry operation to return `RegistryError::Closed`.
    pub fn upgrade(&self) -> Option<EtcdRegistry> {
        self.0
            .upgrade()
            .filter(|inner| !inner.stop.is_cancelled())
            .map(EtcdRegistry)
    }
}

impl EtcdRegistry {
    /// Connect to etcd and initialize independent per-service registration workers on demand.
    pub async fn connect(config: EtcdConfig) -> Result<Self, RegistryError> {
        let client = config.connect_client().await?;
        let callbacks = Arc::new(Mutex::new(Vec::<Callback>::new()));
        let stop = CancellationToken::new();
        let (updates, mut receiver) = mpsc::channel::<ServiceMetadata>(16);
        let token = stop.clone();
        let listeners = callbacks.clone();
        let dispatch = tokio::spawn(async move {
            token
                .run_until_cancelled(async {
                    while let Some(record) = receiver.recv().await {
                        // User callbacks can register or unregister services, including this one.
                        let callbacks = listeners.lock().unwrap().clone();
                        for callback in callbacks {
                            callback(record.clone()).await;
                        }
                    }
                })
                .await;
        });
        Ok(Self(Arc::new(Inner {
            config: Arc::new(config),
            stop,
            callbacks,
            updates,
            state: Mutex::new(State {
                client: Some(client),
                workers: HashMap::new(),
                close: None,
                dispatch: Some(dispatch),
            }),
        })))
    }

    /// Obtain a non-owning reference. Capture it in observers and upgrade it when
    /// accessing the registry to let the last application handle trigger shutdown.
    pub fn downgrade(&self) -> WeakEtcdRegistry {
        WeakEtcdRegistry(Arc::downgrade(&self.0))
    }

    /// Borrow an independently owned clone of the pool while the registry is open.
    /// The caller must drop that clone when it no longer needs the connection.
    pub fn client(&self) -> Result<Client, RegistryError> {
        self.0
            .state
            .lock()
            .unwrap()
            .client
            .clone()
            .ok_or(RegistryError::Closed)
    }

    /// Namespace used to construct registration keys.
    pub fn namespace(&self) -> &str {
        self.0.config.namespace.trim_matches('/')
    }

    /// Register an async observer of governance changes. Each invocation receives an owned record.
    /// The registry retains observers until close. Use [`Self::downgrade`] for
    /// reentrant access because capturing an owning clone creates a reference cycle.
    pub fn on_status_change<F, Fut>(&self, callback: F)
    where
        F: Fn(ServiceMetadata) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut callbacks = self.0.callbacks.lock().unwrap();
        if !self.0.stop.is_cancelled() {
            callbacks.push(Arc::new(move |meta| Box::pin(callback(meta))));
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.0.state.lock().unwrap().workers.len()
    }

    fn worker(
        &self,
        name: &str,
        create: bool,
    ) -> Result<Option<mpsc::Sender<Command>>, RegistryError> {
        let mut state = self.0.state.lock().unwrap();
        if state.close.is_some() {
            return Err(RegistryError::Closed);
        }
        if let Some(sender) = state.workers.get(name).and_then(|w| w.sender.upgrade()) {
            return Ok(Some(sender));
        }
        if !create {
            return Ok(None);
        }
        let (sender, commands) = mpsc::channel(8);
        let weak = sender.downgrade();
        let owner = Arc::downgrade(&self.0);
        let service = name.to_owned();
        let commands = serve_commands(
            state.client.clone().ok_or(RegistryError::Closed)?,
            self.0.config.clone(),
            self.0.updates.clone(),
            self.0.stop.child_token(),
            weak,
            commands,
        );
        let task = tokio::spawn(async move {
            let result = commands.await;
            if let Some(inner) = owner.upgrade() {
                let mut state = inner.state.lock().unwrap();
                // A new caller can already have created a replacement mailbox.
                if state
                    .workers
                    .get(&service)
                    .is_some_and(|w| w.task.id() == tokio::task::id())
                {
                    state.workers.remove(&service);
                }
            }
            result
        });
        state.workers.insert(
            name.to_owned(),
            Worker {
                sender: sender.downgrade(),
                task,
            },
        );
        Ok(Some(sender))
    }

    /// Establish a leased registration, or return Existing for a previously registered service.
    pub async fn register(
        &self,
        options: RegistrationOptions,
    ) -> Result<Registration, RegistryError> {
        if options.service_name.is_empty()
            || options.service_name.contains('/')
            || options.address.is_empty()
        {
            return Err(RegistryError::Invalid(
                "expected a service name and advertised address",
            ));
        }
        let worker = self
            .worker(&options.service_name, true)?
            .expect("requested worker creation");
        let (reply, result) = oneshot::channel();
        worker
            .send(Command::Register(options, reply))
            .await
            .map_err(|_| RegistryError::Closed)?;
        result.await.map_err(|_| RegistryError::Closed)?
    }

    /// Stop recovery, join the tasks, then revoke the current lease for this service.
    pub async fn unregister(&self, name: &str) -> Result<(), RegistryError> {
        self.remove(name, None).await
    }

    /// Unregister only if the handle still identifies the current local registration.
    pub async fn unregister_handle(
        &self,
        handle: &RegistrationHandle,
    ) -> Result<(), RegistryError> {
        self.remove(&handle.service_name, Some(handle.instance_id.clone()))
            .await
    }

    async fn remove(&self, name: &str, id: Option<String>) -> Result<(), RegistryError> {
        let Some(worker) = self.worker(name, false)? else {
            return Ok(());
        };
        let (reply, result) = oneshot::channel();
        worker
            .send(Command::Remove(id, reply))
            .await
            .map_err(|_| RegistryError::Closed)?;
        result.await.map_err(|_| RegistryError::Closed)?
    }

    /// Prohibit new registrations and await cleanup of all local tasks and remote leases.
    /// Dropping the waiting future leaves cleanup running; another close can await it again.
    pub async fn close(&self) -> Result<(), RegistryError> {
        let mut completion = {
            let mut state = self.0.state.lock().unwrap();
            if let Some(close) = &state.close {
                close.clone()
            } else {
                self.0.stop.cancel();
                self.0.callbacks.lock().unwrap().clear();
                let (done, result) = watch::channel(None);
                state.close = Some(result.clone());
                state.client.take();
                let workers = std::mem::take(&mut state.workers);
                let dispatch = state.dispatch.take();
                tokio::spawn(async move {
                    let mut errors = Vec::new();
                    for worker in workers.into_values() {
                        match worker.task.await {
                            Ok(Ok(())) => (),
                            Ok(Err(e)) => errors.push(e.to_string()),
                            Err(e) => errors.push(e.to_string()),
                        }
                    }
                    if let Some(task) = dispatch
                        && let Err(e) = task.await
                    {
                        errors.push(e.to_string());
                    }
                    done.send_replace(Some(if errors.is_empty() {
                        Ok(())
                    } else {
                        Err(errors.join("; "))
                    }));
                });
                result
            }
        };
        let status = completion
            .wait_for(Option::is_some)
            .await
            .map_err(|_| RegistryError::Closed)?;
        status
            .as_ref()
            .unwrap()
            .clone()
            .map_err(RegistryError::Worker)
    }
}

async fn serve_commands(
    client: Client,
    config: Arc<EtcdConfig>,
    updates: mpsc::Sender<ServiceMetadata>,
    stop: CancellationToken,
    mailbox: mpsc::WeakSender<Command>,
    mut commands: mpsc::Receiver<Command>,
) -> Result<(), RegistryError> {
    // Hold the mailbox while leased or processing a command. After unregistration,
    // outstanding callers alone keep it alive until every queued command completes.
    let mut current: Option<(Instance, mpsc::Sender<Command>)> = None;
    while let Some(Some(command)) = stop.run_until_cancelled(commands.recv()).await {
        if stop.is_cancelled() {
            break;
        }
        let Some(hold) = mailbox.upgrade() else {
            break;
        };
        match command {
            Command::Register(options, reply) => {
                if current.is_some() {
                    let _ = reply.send(Ok(Registration::Existing));
                    continue;
                }
                match Instance::start(
                    client.clone(),
                    config.clone(),
                    options,
                    updates.clone(),
                    stop.child_token(),
                )
                .await
                {
                    Ok(instance) => {
                        if reply
                            .send(Ok(Registration::Created(instance.handle.clone())))
                            .is_err()
                        {
                            instance.stop().await?;
                        } else {
                            current = Some((instance, hold));
                        }
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            }
            Command::Remove(id, reply) => {
                let matches = current.as_ref().is_some_and(|(instance, _)| {
                    id.as_ref()
                        .is_none_or(|owner| owner == &instance.handle.instance_id)
                });
                let result = if matches {
                    current.take().unwrap().0.stop().await
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
            }
        }
    }
    if let Some((instance, _hold)) = current {
        instance.stop().await?;
    }
    Ok(())
}
