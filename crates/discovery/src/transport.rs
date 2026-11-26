use crate::{LbPolicy, balancer::Picker};
use arc_swap::ArcSwap;
use http::{Request, Response};
use hyper_util::rt::TokioIo;
use std::{
    error::Error,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
};
use tokio_util::sync::PollSemaphore;
use tonic::{
    Status,
    body::Body,
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use tower::{Service, ServiceExt, service_fn};

/// Connection, refresh and selection settings shared by the instances of a service.
#[derive(Clone)]
pub struct DiscoveryOptions {
    /// Load balancing over established connections.
    pub policy: LbPolicy,
    /// Period between full linearizable snapshots.
    pub resync: Duration,
    /// Maximum time for each TCP, TLS and HTTP/2 setup attempt together.
    pub connect_timeout: Duration,
    /// When provided use TLS, including certificate and service-name verification.
    pub tls: Option<ClientTlsConfig>,
    /// Optional HTTP/2 ping interval, including on idle connections.
    pub keepalive: Option<Duration>,
    /// Maximum delay allowed for an HTTP/2 ping acknowledgement.
    pub keepalive_timeout: Duration,
}
impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            policy: LbPolicy::default(),
            resync: Duration::from_secs(7200),
            connect_timeout: Duration::from_secs(2),
            tls: None,
            keepalive: None,
            keepalive_timeout: Duration::from_secs(20),
        }
    }
}

/// Immutable picker and its channels are published together at each routing change.
pub(crate) struct Routes {
    pub(crate) channels: Vec<Channel>,
    pub(crate) picker: Picker,
    pub(crate) reason: String,
}
pub(crate) type SharedRoutes = Arc<ArcSwap<Routes>>;

impl Routes {
    pub(crate) fn empty(reason: &str) -> Self {
        Self {
            channels: Vec::new(),
            picker: Picker::new(LbPolicy::RoundRobin, &[]),
            reason: reason.into(),
        }
    }
}

/// Cloneable transport accepted directly by tonic generated clients.
pub struct DiscoveredService {
    routes: SharedRoutes,
    capacity: PollSemaphore,
    /// Reservation made by poll_ready and transferred into the request future.
    permit: Option<OwnedSemaphorePermit>,
}
impl Clone for DiscoveredService {
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
            capacity: self.capacity.clone(),
            permit: None,
        }
    }
}

impl DiscoveredService {
    pub(crate) fn new(routes: SharedRoutes) -> Self {
        Self {
            routes,
            capacity: PollSemaphore::new(Arc::new(Semaphore::new(1024))),
            permit: None,
        }
    }
}

impl Service<Request<Body>> for DiscoveredService {
    type Response = Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.permit.is_none() {
            self.permit = std::task::ready!(self.capacity.poll_acquire(cx));
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let routes = self.routes.load();
        let channel = routes.picker.pick().map(|i| routes.channels[i].clone());
        let permit = self
            .permit
            .take()
            .expect("call requires a successful poll_ready");
        let Some(channel) = channel else {
            let response = Status::unavailable(&routes.reason).into_http();
            return Box::pin(async move { Ok(response) });
        };
        Box::pin(async move {
            let _permit = permit;
            Ok(match channel.oneshot(request).await {
                Ok(response) => response,
                Err(error) => request_status(Box::new(error)).into_http(),
            })
        })
    }
}

/// Tonic maps its own transport request timer to Cancelled. For that specific timer
/// surface the RPC deadline, preserving explicit cancellations and other wire statuses.
fn request_status(error: Box<dyn Error + Send + Sync>) -> Status {
    let mut source: Option<&(dyn Error + 'static)> = Some(error.as_ref());
    while let Some(cause) = source {
        if cause.is::<tonic::TimeoutExpired>() {
            return Status::deadline_exceeded(cause.to_string());
        }
        source = cause.source();
    }
    Status::try_from_error(error)
        .unwrap_or_else(|error| Status::unavailable(describe(error.as_ref())))
}

pub(crate) fn describe(error: &(dyn Error + 'static)) -> String {
    let mut msg = error.to_string();
    let mut cause = error.source();
    while let Some(e) = cause {
        msg.push_str(": ");
        msg.push_str(&e.to_string());
        cause = e.source();
    }
    msg
}

/// Use a single underlying socket per published Channel. The outer discovery worker
/// owns reconnects so a Channel that has lost its socket cannot silently dial a replacement.
pub(crate) async fn connect(
    address: &str,
    options: &DiscoveryOptions,
) -> Result<(Channel, oneshot::Receiver<String>), Box<dyn Error + Send + Sync>> {
    let authority: http::uri::Authority = address.parse()?;
    if authority.port_u16().is_none() {
        return Err("missing endpoint port".into());
    }
    let scheme = if options.tls.is_some() {
        "https"
    } else {
        "http"
    };
    let mut ep = Endpoint::from_shared(format!("{scheme}://{authority}"))?;
    if let Some(tls) = options.tls.clone() {
        ep = ep.tls_config(tls)?;
    }
    if let Some(ping) = options.keepalive {
        ep = ep
            .http2_keep_alive_interval(ping)
            .keep_alive_timeout(options.keepalive_timeout)
            .keep_alive_while_idle(true);
    }
    let (signal, ended) = oneshot::channel();
    let mut initial = Some((address.to_owned(), signal));
    let connector = service_fn(move |_| {
        let args = initial.take();
        async move {
            let (address, signal) = args.ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "connection generation ended")
            })?;
            let socket = TcpStream::connect(address).await?;
            socket.set_nodelay(true)?;
            Ok::<_, io::Error>(TokioIo::new(Observed {
                socket,
                signal: Some(signal),
            }))
        }
    });
    let channel = tokio::time::timeout(
        options.connect_timeout,
        ep.connect_with_connector(connector),
    )
    .await??;
    Ok((channel, ended))
}

struct Observed {
    socket: TcpStream,
    /// Fired exactly once, even if the HTTP/2 driver drops the socket on a protocol error.
    signal: Option<oneshot::Sender<String>>,
}
impl Observed {
    fn end(&mut self, reason: String) {
        if let Some(tx) = self.signal.take() {
            let _ = tx.send(reason);
        }
    }
}
impl Drop for Observed {
    fn drop(&mut self) {
        self.end("HTTP/2 connection closed".into());
    }
}
impl AsyncRead for Observed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let available = buf.remaining();
        let result = Pin::new(&mut self.socket).poll_read(cx, buf);
        match &result {
            Poll::Ready(Ok(())) if available > 0 && available == buf.remaining() => {
                self.end("peer closed the connection".into())
            }
            Poll::Ready(Err(e)) => self.end(e.to_string()),
            _ => {}
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write(cx, buf);
        if let Poll::Ready(Err(e)) = &result {
            self.end(e.to_string());
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transport_timer_is_a_deadline_and_explicit_cancellation_survives() {
        // A transport error wraps a concrete timer through its source chain.
        #[derive(Debug, thiserror::Error)]
        #[error("transport failure")]
        struct Transport(#[source] tonic::TimeoutExpired);
        assert_eq!(
            request_status(Box::new(Transport(tonic::TimeoutExpired(())))).code(),
            tonic::Code::DeadlineExceeded
        );
        assert_eq!(
            request_status(Box::new(Status::cancelled("caller"))).code(),
            tonic::Code::Cancelled
        );
        assert_eq!(
            request_status(Box::new(io::Error::other("socket"))).code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn poll_ready_reserves_bounded_capacity_and_drop_releases_it() {
        let routes = Arc::new(ArcSwap::from_pointee(Routes::empty("empty")));
        let mut service = DiscoveredService::new(routes);
        let mut reservations = Vec::new();
        for _ in 0..1024 {
            reservations.push(service.clone().ready_oneshot().await.unwrap());
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), service.ready())
                .await
                .is_err()
        );
        reservations.pop();
        tokio::time::timeout(Duration::from_secs(1), service.ready())
            .await
            .unwrap()
            .unwrap();
    }
}
