//! Preserve a wire deadline across HTTP response headers and the streaming body.
use bytes::Bytes;
use futures_util::task::AtomicWaker;
use http::HeaderMap;
use http_body::{Body as HttpBody, Frame};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep_until},
};
use tonic::{Status, body::Body};

/// Convert the relative gRPC wire timeout once, at dispatch.
pub fn from_headers(headers: &HeaderMap) -> Option<Instant> {
    let value = headers.get("grpc-timeout")?.to_str().ok()?;
    Instant::now().checked_add(wire_timeout(value)?)
}

/// Parse the gRPC TimeoutValue (up to eight decimal digits) and TimeoutUnit.
pub(crate) fn wire_timeout(value: &str) -> Option<Duration> {
    let (unit, digits) = value.as_bytes().split_last()?;
    if digits.len() > 8 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let amount = std::str::from_utf8(digits).ok()?.parse::<u64>().ok()?;
    Some(match unit {
        b'H' => Duration::from_secs(amount * 3600),
        b'M' => Duration::from_secs(amount * 60),
        b'S' => Duration::from_secs(amount),
        b'm' => Duration::from_millis(amount),
        b'u' => Duration::from_micros(amount),
        b'n' => Duration::from_nanos(amount),
        _ => return None,
    })
}

/// Apply the original deadline without prefetching frames. The timer drops the inner
/// body even if its consumer is idle, releasing an outbound producer or inbound stream.
pub fn limit_body(body: Body, end: Instant) -> Body {
    if body.is_end_stream() {
        return body;
    }
    let shared = Arc::new(StreamState {
        body: Mutex::new(Some(body)),
        reader: AtomicWaker::new(),
    });
    let weak = Arc::downgrade(&shared);
    let timer = tokio::spawn(async move {
        sleep_until(end).await;
        if let Some(state) = weak.upgrade() {
            let expired = state.body.lock().unwrap().take();
            drop(expired);
            state.reader.wake();
        }
    });
    Body::new(DeadlineBody {
        shared,
        end,
        timer,
        finished: false,
    })
}

struct StreamState {
    /// Either the consumer or the timer may be first to release the body.
    body: Mutex<Option<Body>>,
    reader: AtomicWaker,
}

struct DeadlineBody {
    shared: Arc<StreamState>,
    end: Instant,
    timer: JoinHandle<()>,
    /// Prevent a second terminal frame after trailers, error or EOF.
    finished: bool,
}

impl Drop for DeadlineBody {
    fn drop(&mut self) {
        self.timer.abort();
    }
}

impl HttpBody for DeadlineBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        self.shared.reader.register(cx.waker());
        let polled = {
            let mut slot = self.shared.body.lock().unwrap();
            match slot.as_mut() {
                Some(body) if Instant::now() < self.end => Pin::new(body).poll_frame(cx),
                _ => Poll::Ready(Some(Err(Status::deadline_exceeded(
                    "RPC deadline exceeded",
                )))),
            }
        };
        if let Poll::Ready(frame) = &polled
            && !matches!(frame, Some(Ok(frame)) if frame.is_data())
        {
            self.finished = true;
            self.timer.abort();
            let finished = self.shared.body.lock().unwrap().take();
            drop(finished);
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.finished
    }
}
