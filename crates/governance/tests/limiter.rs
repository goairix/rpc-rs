use rpc_governance::TokenBucket;
use std::time::Duration;
use tokio::time::{Instant, advance, timeout};

#[tokio::test(start_paused = true)]
async fn starts_full_and_cancelled_wait_does_not_consume_next_token() {
    let bucket = TokenBucket::new(0.5, 2);
    assert!(bucket.allow());
    assert!(bucket.allow());
    assert!(!bucket.allow());
    assert!(
        timeout(Duration::from_millis(499), bucket.wait())
            .await
            .is_err()
    );
    assert!(!bucket.allow());
    let start = Instant::now();
    bucket.wait().await;
    assert!(start.elapsed() <= Duration::from_millis(502));
    assert!(!bucket.allow());
    advance(Duration::from_secs(10)).await;
    assert!(bucket.allow());
    assert!(bucket.allow());
    assert!(!bucket.allow());
}
