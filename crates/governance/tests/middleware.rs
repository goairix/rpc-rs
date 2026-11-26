use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http::{HeaderMap, Request, Response};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData},
};
use rpc_governance::{GovernanceLayer, TokenBucket};
use std::{collections::HashMap, convert::Infallible};
use tonic::{Status, body::Body};
use tower::{Layer, ServiceExt, service_fn};

#[tokio::test]
async fn observes_final_status_exactly_once_and_recovers_panics() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let layer = GovernanceLayer::default()
        .with_meter(&provider.meter("rpc"), "test")
        .with_limiter(TokenBucket::new(1.0, 0), vec!["/limited".to_owned()]);

    let service = layer.layer(service_fn(|r: Request<()>| async move {
        let path = r.uri().path();
        let response = match path {
            "/limited" => panic!("limiter should have rejected"),
            "/invalid" => Status::invalid_argument("bad").into_http(),
            "/handler-panic" => panic!("handler failed"),
            path => {
                let status = if path == "/stream-error" { "13" } else { "0" };
                let mut trailers = HeaderMap::new();
                trailers.insert("grpc-status", status.parse().unwrap());
                let data = stream::iter([Ok::<_, Status>(Frame::data(Bytes::from_static(b"x")))]);
                let tail = match path {
                    "/cancel" => stream::pending().boxed(),
                    "/body-panic" => stream::once(async { panic!("poll failed") }).boxed(),
                    _ => stream::iter([Ok(Frame::trailers(trailers))]).boxed(),
                };
                Response::new(Body::new(StreamBody::new(data.chain(tail))))
            }
        };
        Ok::<_, Infallible>(response)
    }));
    let cases = [
        ("/invalid", "InvalidArgument", Some("3")),
        ("/limited", "ResourceExhausted", Some("8")),
        ("/handler-panic", "Internal", Some("13")),
        ("/stream-error", "Internal", None),
        ("/success", "OK", None),
        ("/cancel", "Canceled", None),
        ("/body-panic", "Internal", None),
    ];
    for (method, _, immediate) in cases {
        let response = service
            .clone()
            .oneshot(Request::builder().uri(method).body(()).unwrap())
            .await
            .unwrap();
        if let Some(code) = immediate {
            assert_eq!(response.headers()["grpc-status"], code);
        } else {
            let mut body = response.into_body();
            assert_eq!(
                body.frame().await.unwrap().unwrap().into_data().unwrap(),
                "x"
            );
            if method != "/cancel" {
                let trailers = body
                    .frame()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_trailers()
                    .unwrap();
                assert_eq!(
                    trailers["grpc-status"],
                    if method == "/success" { "0" } else { "13" }
                );
                assert!(body.frame().await.is_none());
            }
        }
    }
    provider.force_flush().unwrap();
    let finished = exporter.get_finished_metrics().unwrap();
    let mut counted = HashMap::new();
    let mut timed = HashMap::new();
    for metric in finished
        .last()
        .unwrap()
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
    {
        match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                assert_eq!(metric.name(), "test_rpc_requests_total");
                for point in sum.data_points() {
                    let attrs: HashMap<_, _> = point
                        .attributes()
                        .map(|kv| (kv.key.as_str(), kv.value.to_string()))
                        .collect();
                    assert_eq!(attrs.len(), 2);
                    assert_eq!(point.value(), 1);
                    counted.insert(attrs["method"].clone(), attrs["status"].clone());
                }
            }
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                assert_eq!(metric.name(), "test_rpc_request_duration_seconds");
                assert_eq!(metric.unit(), "s");
                for point in histogram.data_points() {
                    let attrs: Vec<_> = point.attributes().collect();
                    assert_eq!(attrs.len(), 1);
                    assert_eq!(attrs[0].key.as_str(), "method");
                    timed.insert(attrs[0].value.to_string(), point.count());
                }
            }
            _ => panic!("unexpected metric"),
        }
    }
    for (method, status, _) in cases {
        assert_eq!(counted[method], status);
        assert_eq!(timed[method], 1);
    }
}
