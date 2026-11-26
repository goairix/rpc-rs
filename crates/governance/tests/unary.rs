use rpc_governance::{CircuitBreaker, RetryPolicy, UnaryPolicy};
use std::{
    future::{pending, ready},
    time::Duration,
};
use tokio::time::{Instant, advance};
use tonic::{Code, Request, Status};

#[tokio::test(start_paused = true)]
async fn retry_rebuilds_request_with_remaining_budget() {
    let policy = UnaryPolicy::default().with_retry(RetryPolicy::default());
    let mut req = Request::new("echo".to_owned());
    req.metadata_mut().insert("test-id", "42".parse().unwrap());
    let mut timeouts = vec![];
    let end = Instant::now() + Duration::from_secs(1);
    policy
        .call(req, Some(end), |r| {
            assert_eq!(r.get_ref(), "echo");
            assert_eq!(r.metadata().get("test-id").unwrap(), "42");
            timeouts.push(
                r.metadata()
                    .get("grpc-timeout")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
            ready(if timeouts.len() == 1 {
                Err(Status::unavailable("again"))
            } else {
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(timeouts.len(), 2);
    assert_ne!(timeouts[0], timeouts[1]);
    assert_eq!(end - Instant::now(), Duration::from_millis(900));
}

#[tokio::test(start_paused = true)]
async fn attempt_limit_classification_and_timeout() {
    for (max_attempts, code, expected_calls) in [
        (0, Code::Unavailable, 1),
        (3, Code::InvalidArgument, 1),
        (3, Code::Unavailable, 3),
    ] {
        let policy = UnaryPolicy::default().with_retry(RetryPolicy {
            max_attempts,
            ..Default::default()
        });
        let mut calls = 0;
        assert_eq!(
            policy
                .call(Request::new(()), None, |_| {
                    calls += 1;
                    ready(Err::<(), _>(Status::new(code, "test")))
                })
                .await
                .unwrap_err()
                .code(),
            code
        );
        assert_eq!(calls, expected_calls);
    }
    let policy = UnaryPolicy::default().with_retry(RetryPolicy::default());
    let end = Instant::now() + Duration::from_millis(20);
    let mut calls = 0;
    let result = policy
        .call(Request::new(()), Some(end), |_| {
            calls += 1;
            ready(Err::<(), _>(Status::unavailable("test")))
        })
        .await;
    assert_eq!(calls, 1);
    assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
    let end = Instant::now() + Duration::from_secs(1);
    assert_eq!(
        policy
            .call(
                Request::new(()),
                Some(end),
                |_| pending::<Result<(), Status>>()
            )
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
}

#[tokio::test(start_paused = true)]
async fn half_open_cancellation_and_late_results() {
    let policy = UnaryPolicy::default().with_breaker(CircuitBreaker::default());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut rx = Some(rx);
    let mut old = Box::pin(policy.call(Request::new(()), None, |_| {
        let rx = rx.take().unwrap();
        async {
            rx.await.unwrap();
            Ok(())
        }
    }));
    assert!(
        tokio::time::timeout(Duration::ZERO, &mut old)
            .await
            .is_err()
    );
    for _ in 0..5 {
        assert_eq!(
            policy
                .call(Request::new(()), None, |_| ready(Err::<(), _>(
                    Status::internal("oops")
                )))
                .await
                .unwrap_err()
                .code(),
            Code::Internal
        );
    }
    tx.send(()).unwrap();
    old.await.unwrap();
    let check_blocked = || {
        policy.call(
            Request::new(()),
            None,
            |_| -> std::future::Ready<Result<(), Status>> { panic!("admitted") },
        )
    };
    assert_eq!(check_blocked().await.unwrap_err().code(), Code::Unavailable);
    advance(Duration::from_secs(10)).await;
    for _ in 0..3 {
        let mut probe =
            Box::pin(policy.call(Request::new(()), None, |_| pending::<Result<(), Status>>()));
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut probe)
                .await
                .is_err()
        );
        assert_eq!(check_blocked().await.unwrap_err().code(), Code::Unavailable);
        drop(probe);
        policy
            .call(Request::new(()), None, |_| ready(Ok(())))
            .await
            .unwrap();
    }
    let mut slow =
        Box::pin(policy.call(Request::new(()), None, |_| pending::<Result<(), Status>>()));
    assert!(
        tokio::time::timeout(Duration::ZERO, &mut slow)
            .await
            .is_err()
    );
    policy
        .call(Request::new(()), None, |_| ready(Ok(())))
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_backoff_prevents_replay() {
    let policy = UnaryPolicy::default().with_retry(RetryPolicy::default());
    let mut attempts = 0;
    let call = policy.call(Request::new(()), None, |_| {
        attempts += 1;
        ready(Err::<(), _>(Status::unavailable("again")))
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), call)
            .await
            .is_err()
    );
    advance(Duration::from_secs(5)).await;
    assert_eq!(attempts, 1);
}

#[tokio::test(start_paused = true)]
async fn circuit_classifies_one_outcome_per_logical_call() {
    let policy = UnaryPolicy::default()
        .with_retry(RetryPolicy::default())
        .with_breaker(CircuitBreaker::new(|code| code != Code::InvalidArgument));

    for _ in 0..10 {
        let failure = policy
            .call(Request::new(()), None, |_| {
                ready(Err::<(), _>(Status::invalid_argument("business error")))
            })
            .await
            .unwrap_err();
        assert_eq!(failure.code(), Code::InvalidArgument);
    }
    for _ in 0..5 {
        let mut attempts = 0;
        let failure = policy
            .call(Request::new(()), None, |_| {
                attempts += 1;
                ready(Err::<(), _>(Status::internal("upstream error")))
            })
            .await
            .unwrap_err();
        assert_eq!(failure.code(), Code::Internal);
        assert_eq!(attempts, 3);
    }
    let result = policy.call(Request::new(()), None, |_| ready(Ok(()))).await;
    assert_eq!(result.unwrap_err().code(), Code::Unavailable);
}

#[tokio::test(start_paused = true)]
async fn request_timeout_bounds_all_attempts() {
    let policy = UnaryPolicy::default().with_retry(RetryPolicy::default());
    for deadline in [None, Some(Instant::now() + Duration::from_secs(10))] {
        let mut request = Request::new(());
        request.set_timeout(Duration::from_millis(50));
        let mut attempts = 0;
        let result = policy
            .call(request, deadline, |_| {
                attempts += 1;
                ready(Err::<(), _>(Status::unavailable("retry later")))
            })
            .await;
        assert_eq!(attempts, 1);
        assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
    }
}

#[tokio::test(start_paused = true)]
async fn elapsed_budget_wins_when_response_is_ready_on_same_poll() {
    for outcome in [Ok(()), Err(Status::cancelled("transport cancelled"))] {
        let policy = UnaryPolicy::default();
        let end = Instant::now() + Duration::from_millis(50);
        let (reply, received) = tokio::sync::oneshot::channel();
        let mut received = Some(received);
        let call = policy.call(Request::new(()), Some(end), |_| {
            let received = received.take().unwrap();
            async move { received.await.unwrap() }
        });
        tokio::pin!(call);
        assert!(futures_util::poll!(&mut call).is_pending());
        advance(Duration::from_millis(50)).await;
        reply.send(outcome).unwrap();
        assert_eq!(call.await.unwrap_err().code(), Code::DeadlineExceeded);
    }
}
