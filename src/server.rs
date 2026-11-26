//! Typed service assembly and ownership of server registrations.
use crate::{
    governance::{GovernanceLayer, TokenBucket},
    registry::{Registration, RegistrationHandle, RegistrationOptions},
};
use prost::Message;
use std::{collections::HashSet, convert::Infallible, future::Future, sync::Arc};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{body::Body, server::NamedService, service::RoutesBuilder, transport::Server};
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub use crate::registry::Registry;

/// Builder for tonic routes, descriptors and optional service discovery.
#[derive(Default)]
pub struct RpcServer {
    routes: RoutesBuilder,
    /// Protobuf route names and the independent, user-selected discovery names.
    services: Vec<(&'static str, RegistrationOptions)>,
    descriptors: Vec<&'static [u8]>,
    reflection: bool,
    limiter: Option<TokenBucket>,
    governance: GovernanceLayer,
    transport: Server,
}

impl RpcServer {
    /// Supply a configured tonic server builder, e.g. for TLS or HTTP/2 keepalive.
    pub fn with_transport(mut self, server: Server) -> Self {
        self.transport = server;
        self
    }
    /// Install the application's logger/metric/admission layer configuration.
    pub fn with_governance(mut self, layer: GovernanceLayer) -> Self {
        self.governance = layer;
        self
    }
    /// Apply a process-local token bucket to descriptor-identified unary methods.
    pub fn with_limiter(mut self, bucket: TokenBucket) -> Self {
        self.limiter = Some(bucket);
        self
    }
    /// Register descriptors for method identification and reflection.
    pub fn with_descriptor(mut self, bytes: &'static [u8]) -> Self {
        self.descriptors.push(bytes);
        self
    }
    /// Expose v1 and v1alpha reflection for mounted services.
    pub fn with_reflection(mut self) -> Self {
        self.reflection = true;
        self
    }

    /// Mount a generated service. Empty registration name and address use the protobuf name and listener address.
    pub fn add_service<S>(mut self, service: S, mut options: RegistrationOptions) -> Self
    where
        S: NamedService
            + Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        if options.service_name.is_empty() {
            options.service_name = S::NAME.into();
        }
        self.routes.add_service(service);
        self.services.push((S::NAME, options));
        self
    }

    /// Start a supervised task. Pass None for direct serving, or a shared registry to publish services.
    /// Dropping the handle signals shutdown; await shutdown() to observe cleanup errors.
    pub fn start(
        self,
        listener: TcpListener,
        registry: Option<Arc<dyn Registry>>,
    ) -> RunningServer {
        let stop = CancellationToken::new();
        let task = tokio::spawn(self.run(listener, registry, stop.clone()));
        RunningServer {
            stop,
            task: Some(task),
        }
    }

    async fn run(
        mut self,
        listener: TcpListener,
        registry: Option<Arc<dyn Registry>>,
        stop: CancellationToken,
    ) -> Result<(), BoxError> {
        let address = listener.local_addr()?.to_string();
        self.routes
            .add_service(tonic_health::pb::health_server::HealthServer::new(
                crate::health::Serving,
            ));
        self.descriptors.push(tonic_health::pb::FILE_DESCRIPTOR_SET);
        let names: HashSet<_> = self
            .services
            .iter()
            .map(|(name, _)| *name)
            .chain(["grpc.health.v1.Health"])
            .collect();
        if self.limiter.is_some() || self.reflection {
            let paths = unary_paths(&self.descriptors, &names)?;
            if let Some(bucket) = self.limiter {
                self.governance = self.governance.with_limiter(bucket, paths);
            }
        }
        if self.reflection {
            let builder = || {
                let mut b = tonic_reflection::server::Builder::configure();
                for bytes in &self.descriptors {
                    b = b.register_encoded_file_descriptor_set(bytes);
                }
                for name in &names {
                    b = b.with_service_name(*name);
                }
                b.with_service_name("grpc.reflection.v1.ServerReflection")
                    .with_service_name("grpc.reflection.v1alpha.ServerReflection")
                    .register_encoded_file_descriptor_set(
                        tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET,
                    )
                    .register_encoded_file_descriptor_set(
                        tonic_reflection::pb::v1alpha::FILE_DESCRIPTOR_SET,
                    )
            };
            self.routes.add_service(builder().build_v1()?);
            self.routes.add_service(builder().build_v1alpha()?);
        }
        let mut owned = Vec::new();
        if let Some(reg) = &registry {
            for (_, mut options) in self.services {
                if stop.is_cancelled() {
                    break;
                }
                if options.address.is_empty() {
                    options.address = address.clone();
                }
                match reg.register(options).await {
                    Ok(Registration::Created(handle)) => owned.push(handle),
                    Ok(Registration::Existing) => {}
                    Err(e) => return combine(Err(Box::new(e)), withdraw(reg, &mut owned).await),
                }
            }
        }
        let drain = CancellationToken::new();
        let server = self
            .transport
            .layer(self.governance)
            .add_routes(self.routes.routes())
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                drain.clone().cancelled_owned(),
            );
        tokio::pin!(server);
        // Keep polling the transport while withdrawing leases, then drain active streams.
        let mut early = None;
        tokio::select! {result=&mut server=>early=Some(result), ()=stop.cancelled()=>{}}
        let cleanup = async {
            if let Some(reg) = &registry {
                withdraw(reg, &mut owned).await
            } else {
                vec![]
            }
        };
        let (served, errors) = if let Some(result) = early {
            (result, cleanup.await)
        } else {
            let (errors, result) = tokio::join!(
                async {
                    let errors = cleanup.await;
                    drain.cancel();
                    errors
                },
                server
            );
            (result, errors)
        };
        combine(served.map_err(|e| Box::new(e) as BoxError), errors)
    }
}

/// Check that descriptors describe every mounted service before publishing it.
fn unary_paths(descriptors: &[&[u8]], names: &HashSet<&str>) -> Result<Vec<String>, BoxError> {
    let mut missing = names.clone();
    let mut paths = Vec::new();
    for bytes in descriptors {
        for file in prost_types::FileDescriptorSet::decode(*bytes)?.file {
            let prefix = file.package.unwrap_or_default();
            for service in file.service {
                let name = if prefix.is_empty() {
                    service.name().to_owned()
                } else {
                    format!("{prefix}.{}", service.name())
                };
                if !missing.remove(name.as_str()) {
                    continue;
                }
                for m in service.method {
                    if !m.client_streaming() && !m.server_streaming() {
                        paths.push(format!("/{name}/{}", m.name()));
                    }
                }
            }
        }
    }
    if !missing.is_empty() {
        return Err(format!("missing service descriptors: {missing:?}").into());
    }
    Ok(paths)
}

async fn withdraw(reg: &Arc<dyn Registry>, owned: &mut Vec<RegistrationHandle>) -> Vec<String> {
    let mut errors = Vec::new();
    for h in owned.drain(..).rev() {
        if let Err(e) = reg.unregister_handle(&h).await {
            errors.push(format!("withdraw {}: {e}", h.service_name))
        }
    }
    errors
}

fn combine(result: Result<(), BoxError>, mut cleanup: Vec<String>) -> Result<(), BoxError> {
    if cleanup.is_empty() {
        return result;
    }
    if let Err(e) = result {
        cleanup.insert(0, e.to_string())
    }
    Err(cleanup.join("; ").into())
}

/// Owned supervisor for one listener and its registration handles.
pub struct RunningServer {
    stop: CancellationToken,
    task: Option<JoinHandle<Result<(), BoxError>>>,
}

impl RunningServer {
    /// Drain the server and finish withdrawing registrations.
    pub async fn shutdown(self) -> Result<(), BoxError> {
        self.stop.cancel();
        self.wait().await
    }
    /// Wait for transport failure or an application shutdown signal, whichever comes first.
    pub async fn until(mut self, signal: impl Future<Output = ()>) -> Result<(), BoxError> {
        let mut task = self.task.take().expect("server task");
        tokio::select! {result=&mut task=>result?, ()=signal=>{self.stop.cancel();task.await?}}
    }
    /// Wait for the server to terminate and report its final result.
    pub async fn wait(mut self) -> Result<(), BoxError> {
        self.task.take().expect("server task").await?
    }
}
impl Drop for RunningServer {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
