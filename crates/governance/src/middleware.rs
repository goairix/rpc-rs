use std::{
    collections::HashSet,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use bytes::Bytes;
use futures_util::FutureExt;
use http::{HeaderMap, Request, Response};
use http_body::{Body as HttpBody, Frame};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use tonic::{Code, Status, body::Body};
use tower::{Layer, Service};

use crate::TokenBucket;

/// Tower layer for panic recovery, terminal-status logging, metrics and unary admission.
#[derive(Clone, Default)]
pub struct GovernanceLayer {
    metrics: Option<Instruments>,
    /// Per-application logging policy for expected business failures.
    business: Arc<Vec<Code>>,
    /// The descriptor-derived set excludes streaming methods.
    admission: Option<(TokenBucket, Arc<HashSet<String>>)>,
}

#[derive(Clone)]
struct Instruments {
    requests: Counter<u64>,
    latency: Histogram<f64>,
}

impl GovernanceLayer {
    /// Register RPC instruments with an application-owned meter.
    pub fn with_meter(mut self, meter: &Meter, name: &str) -> Self {
        self.metrics = Some(Instruments {
            requests: meter
                .u64_counter(format!("{name}_rpc_requests_total"))
                .build(),
            latency: meter
                .f64_histogram(format!("{name}_rpc_request_duration_seconds"))
                .with_unit("s")
                .build(),
        });
        self
    }

    /// Log the selected error codes at info level.
    pub fn with_business_codes(mut self, codes: Vec<Code>) -> Self {
        self.business = Arc::new(codes);
        self
    }

    /// Enforce one shared token bucket on the supplied unary method paths.
    pub fn with_limiter(
        mut self,
        bucket: TokenBucket,
        unary_paths: impl IntoIterator<Item = String>,
    ) -> Self {
        self.admission = Some((bucket, Arc::new(unary_paths.into_iter().collect())));
        self
    }
}

impl<S> Layer<S> for GovernanceLayer {
    type Service = Governed<S>;
    fn layer(&self, inner: S) -> Governed<S> {
        Governed {
            inner,
            layer: self.clone(),
        }
    }
}

/// Service produced by the governance layer. Readiness is delegated to the inner service.
#[derive(Clone)]
pub struct Governed<S> {
    inner: S,
    layer: GovernanceLayer,
}

/// Carries sole ownership of the terminal event from dispatch into the body.
struct Call {
    method: Option<String>,
    started: Instant,
    layer: GovernanceLayer,
}

impl Call {
    fn finish(&mut self, code: Code) {
        let Some(method) = self.method.take() else {
            return;
        };
        let elapsed = self.started.elapsed().as_secs_f64();
        let status = match code {
            Code::Ok => "OK".to_owned(),
            Code::Cancelled => "Canceled".to_owned(),
            other => format!("{other:?}"),
        };
        if let Some(metrics) = &self.layer.metrics {
            let attrs = [
                KeyValue::new("method", method.clone()),
                KeyValue::new("status", status.clone()),
            ];
            metrics.requests.add(1, &attrs);
            metrics.latency.record(elapsed, &attrs[..1]);
        }
        match code {
            Code::Ok => tracing::debug!(
                method,
                status,
                duration_seconds = elapsed,
                "rpc request completed"
            ),
            code if self.layer.business.contains(&code) => tracing::info!(
                method,
                status,
                duration_seconds = elapsed,
                "rpc business error"
            ),
            _ => tracing::error!(
                method,
                status,
                duration_seconds = elapsed,
                "rpc request failed"
            ),
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        self.finish(Code::Cancelled);
    }
}

impl<S, B> Service<Request<B>> for Governed<S>
where
    S: Service<Request<B>, Response = Response<Body>>,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let deadline = crate::deadline::from_headers(req.headers());
        let method = req.uri().path().to_owned();
        let rejected = self
            .layer
            .admission
            .as_ref()
            .is_some_and(|(bucket, paths)| paths.contains(&method) && !bucket.allow());
        let mut call = Call {
            method: Some(method),
            started: Instant::now(),
            layer: self.layer.clone(),
        };
        if rejected {
            call.finish(Code::ResourceExhausted);
            return Box::pin(async {
                Ok(Status::resource_exhausted("rate limit exceeded").into_http())
            });
        }
        // Invoke the same service that was polled ready; catch both dispatch and poll panics.
        let dispatched = catch_unwind(AssertUnwindSafe(|| self.inner.call(req)));
        Box::pin(async move {
            let response = match dispatched {
                Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => Status::from_error(error.into()).into_http(),
                    Err(_) => panic_status().into_http(),
                },
                Err(_) => panic_status().into_http(),
            };
            if let Some(code) = header_code(response.headers()) {
                call.finish(code);
                return Ok(response);
            }
            Ok(response.map(|inner| {
                let inner = match deadline {
                    Some(end) => crate::deadline::limit_body(inner, end),
                    None => inner,
                };
                Body::new(ObservedBody {
                    inner,
                    call,
                    ended: false,
                })
            }))
        })
    }
}

fn header_code(headers: &HeaderMap) -> Option<Code> {
    headers
        .get(Status::GRPC_STATUS)
        .map(|value| Code::from_bytes(value.as_bytes()))
}

fn panic_status() -> Status {
    Status::internal("RPC handler panicked")
}

struct ObservedBody {
    inner: Body,
    call: Call,
    /// No more polls of the wrapped stream after a terminal error, panic or trailers.
    ended: bool,
}

impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let polled = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut self.inner).poll_frame(cx)
        }));
        let failure = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(Some(Ok(frame)))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    self.ended = true;
                    self.call
                        .finish(header_code(trailers).unwrap_or(Code::Unknown));
                }
                return Poll::Ready(Some(Ok(frame)));
            }
            Ok(Poll::Ready(None)) => {
                self.ended = true;
                self.call.finish(Code::Unknown);
                return Poll::Ready(None);
            }
            Ok(Poll::Ready(Some(Err(status)))) => status,
            Err(_) => panic_status(),
        };
        self.ended = true;
        self.call.finish(failure.code());
        // Finish with gRPC trailers so the peer receives the application status over HTTP/2.
        let mut trailers = HeaderMap::new();
        if let Err(error) = failure.add_header(&mut trailers) {
            return Poll::Ready(Some(Err(error)));
        }
        Poll::Ready(Some(Ok(Frame::trailers(trailers))))
    }
}
