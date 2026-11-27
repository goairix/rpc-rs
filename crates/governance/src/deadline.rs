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
    TimedStreams::<1>::new(end).wrap(0, body)
}

/// Bound the client's request and its eventual response with one timer. Consuming
/// the returned closure attaches the response exactly once, even for duplex RPCs.
pub fn limit_request(
    request: http::Request<Body>,
    end: Instant,
) -> (http::Request<Body>, impl FnOnce(Body) -> Body) {
    let streams = TimedStreams::<2>::new(end);
    let request = request.map(|body| streams.clone().wrap(0, body));
    (request, move |body| streams.wrap(1, body))
}

#[derive(Default)]
struct StreamState {
    /// Either the consumer or the timer may be first to release the body.
    body: Mutex<Option<Body>>,
    reader: AtomicWaker,
}

impl StreamState {
    fn release(&self) {
        // A user body may panic during poll_frame. Cleanup must still be safe
        // during unwinding, and arbitrary body destructors run outside the lock.
        let finished = self.body.lock().unwrap_or_else(|e| e.into_inner()).take();
        drop(finished);
    }
}

struct TimedStreams<const N: usize> {
    end: Instant,
    /// Each half has an independent lock and waker so it can progress concurrently.
    slots: [StreamState; N],
    timer: JoinHandle<()>,
}

impl<const N: usize> TimedStreams<N> {
    fn new(end: Instant) -> Arc<Self> {
        Arc::new_cyclic(|weak: &std::sync::Weak<Self>| {
            let weak = weak.clone();
            let timer = tokio::spawn(async move {
                sleep_until(end).await;
                if let Some(streams) = weak.upgrade() {
                    for slot in &streams.slots {
                        slot.release();
                        slot.reader.wake();
                    }
                }
            });
            Self {
                end,
                slots: std::array::from_fn(|_| StreamState::default()),
                timer,
            }
        })
    }

    fn wrap(self: Arc<Self>, index: usize, body: Body) -> Body {
        if body.is_end_stream() {
            return body;
        }
        {
            let mut slot = self.slots[index].body.lock().unwrap();
            // The deadline can pass before response headers arrive or even before
            // the timer task first runs. Never install a body after expiry.
            if Instant::now() < self.end {
                *slot = Some(body);
            }
        }
        Body::new(DeadlineBody {
            shared: Some(self),
            index,
        })
    }
}

impl<const N: usize> Drop for TimedStreams<N> {
    fn drop(&mut self) {
        self.timer.abort();
    }
}

struct DeadlineBody<const N: usize> {
    /// Taken on the terminal frame so the final half can retire the timer immediately.
    shared: Option<Arc<TimedStreams<N>>>,
    index: usize,
}

impl<const N: usize> DeadlineBody<N> {
    fn finish(&mut self) {
        // Dropping one half releases its transport even if the other half is live.
        if let Some(shared) = self.shared.take() {
            shared.slots[self.index].release();
            shared.slots[self.index].reader.take();
        }
    }
}

impl<const N: usize> Drop for DeadlineBody<N> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl<const N: usize> HttpBody for DeadlineBody<N> {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let Some(shared) = &self.shared else {
            return Poll::Ready(None);
        };
        let state = &shared.slots[self.index];
        state.reader.register(cx.waker());
        let polled = {
            let mut slot = state.body.lock().unwrap();
            match slot.as_mut() {
                Some(body) if Instant::now() < shared.end => Pin::new(body).poll_frame(cx),
                _ => Poll::Ready(Some(Err(Status::deadline_exceeded(
                    "RPC deadline exceeded",
                )))),
            }
        };
        if let Poll::Ready(frame) = &polled
            && !matches!(frame, Some(Ok(frame)) if frame.is_data())
        {
            self.finish();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.shared.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;

    struct TrackedByte(Arc<u8>);
    impl AsRef<[u8]> for TrackedByte {
        fn as_ref(&self) -> &[u8] {
            std::slice::from_ref(self.0.as_ref())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn finishing_request_keeps_timer_for_unread_response() {
        let byte = Arc::new(1);
        let body = || Body::new(Full::new(Bytes::from_owner(TrackedByte(byte.clone()))));
        let budget = Duration::from_secs(1);
        let (request, wrap_response) =
            limit_request(http::Request::new(body()), Instant::now() + budget);
        let _unread = wrap_response(body());
        drop(request);
        assert_eq!(Arc::strong_count(&byte), 2);
        tokio::time::advance(budget).await;
        tokio::task::yield_now().await;
        assert_eq!(Arc::strong_count(&byte), 1);
    }
}
