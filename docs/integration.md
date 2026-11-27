# Rust 接入指南

本文使用 Greeter 作为业务服务，演示如何编译 proto、启动服务，并从直连切换到按名称发现实例。[运行完整示例](../examples/greeter/README.md)

## 生成服务和客户端

按[安装说明](../README.md#安装)添加 `rpc-rs`，再将以下依赖合并到应用的 `Cargo.toml`，用于运行服务和编译业务 proto。

```toml
[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net", "signal", "time"] }
tonic = "0.14"
tonic-prost = "0.14"
prost = "0.14"

[build-dependencies]
tonic-prost-build = "0.14"
```

将业务协议保存为 `proto/greeter.proto`。在应用的 `build.rs` 中编译协议，同时输出供反射和限流使用的描述符。

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .file_descriptor_set_path(out.join("greeter_descriptor.bin"))
        .compile_protos(&["proto/greeter.proto"], &["proto"])?;
    Ok(())
}
```

用 `tonic::include_proto!("example.v1")` 导入与 proto 包名对应的生成代码。实现 `Greeter` trait 后，在异步入口挂载服务。下面的 `Greetings` 实现见 [Greeter 服务端](../examples/greeter/rust/src/bin/server.rs)。

```rust
use rpc_rs::server::RpcServer;
use tokio::net::TcpListener;

let listener = TcpListener::bind("127.0.0.1:45051").await?;
let server = RpcServer::default()
    .with_descriptor(tonic::include_file_descriptor_set!("greeter_descriptor"))
    .with_reflection()
    .add_service(GreeterServer::new(Greetings), Default::default())
    .start(listener, None);

server.until(async { let _ = tokio::signal::ctrl_c().await; }).await?;
```

`start(listener, None)` 启动直连服务。同一个 `RpcServer` 可以多次调用 `add_service`。客户端已知地址时，可直接使用生成的客户端。

```rust
let mut client = GreeterClient::connect("http://127.0.0.1:45051").await?;
let mut request = tonic::Request::new(HelloRequest { name: "Ada".into() });
request.set_timeout(std::time::Duration::from_secs(2));
let reply = client.say_hello(request).await?.into_inner();
```

## 按服务名注册与发现

服务端向 `RpcServer::start` 传入 `Arc<dyn naming::Registry>`，客户端向 `Discovery::connect` 传入 `&dyn naming::Resolver`。默认注册名是 protobuf 的全限定服务名，默认通告地址是监听器的实际地址。监听 `0.0.0.0` 或部署在容器内时，应通过 `RegistrationOptions::address` 指定客户端可达的地址。

| 后端 | 服务端注册 | 客户端解析 | 实例变更 |
| --- | --- | --- | --- |
| 静态 JSON | `StaticRegistry` 校验服务名和通告地址 | `StaticRegistry` | 启动时加载，修改文件后需重新加载并创建发现对象 |
| etcd | `EtcdRegistry` 注册并维护租约 | `EtcdConfig` | watch 跟踪实例、状态和权重变化 |
| 自定义后端 | 实现 `naming::Registry` | 实现 `naming::Resolver` 和 `DiscoverySubscription` | 订阅按稳定实例 ID 发布完整快照 |

### 静态服务目录

在 JSON 中填写服务名、可达的 IPv4 或 IPv6 地址和非零端口。同名服务可配置多个实例，`weight` 为可选正整数，默认 100。

```json
{
  "instances": [
    { "service_name": "example.v1.Greeter", "ip": "10.0.0.11", "port": 45051, "weight": 200 },
    { "service_name": "example.v1.Greeter", "ip": "10.0.0.12", "port": 45051, "weight": 100 }
  ]
}
```

两端各自用 `StaticRegistry::from_file("services.json")?` 加载目录。客户端使用发现层创建生成客户端的传输句柄。

```rust
use rpc_rs::discovery::{Discovery, DiscoveryOptions, LbPolicy};
use rpc_rs::naming::static_registry::StaticRegistry;

let directory = StaticRegistry::from_file("services.json")?;
let options = DiscoveryOptions { policy: LbPolicy::WeightedRoundRobin, ..Default::default() };
let mut discovery = Discovery::connect(&directory, "example.v1.Greeter", options).await?;
let client = GreeterClient::new(discovery.service());
```

初次建连是异步的。需要首个请求等到实例上线时，可在限定时间内等待 `ready_count() > 0`。只有状态为 `up` 且连接可用的实例才参与分流，无可用实例时调用返回 `UNAVAILABLE`。默认策略为 `RoundRobin`，也支持 `PickFirst` 和 `WeightedRoundRobin`。应用应保留 `Discovery` 供客户端复用，使用结束后调用 `discovery.close().await`。[包含就绪等待和关闭的客户端](../examples/greeter/rust/src/bin/client.rs) · [自定义后端接入示例](../tests/custom_backend.rs)

### etcd 动态注册

默认连接 `http://127.0.0.1:2379`，namespace 为 `grpc`，租约 TTL 为 5 秒，etcd 操作超时为 2 秒。可通过 `EtcdConfig` 配置 endpoints、认证和 TLS。客户端用同一 namespace 的 `EtcdConfig` 作为 resolver。

```rust
use rpc_rs::naming::etcd::{EtcdConfig, EtcdRegistry};
use std::sync::Arc;

let config = EtcdConfig::default();
let registry = Arc::new(EtcdRegistry::connect(config).await?);
let server = RpcServer::default()
    .add_service(GreeterServer::new(Greetings), Default::default())
    .start(listener, Some(registry.clone()));

let result = server.until(async { let _ = tokio::signal::ctrl_c().await; }).await;
registry.close().await?;
result?;
```

每条记录的 key 为 `/<namespace>/<service_name>/<instance_id>`。更新状态或权重时应保留原租约，权重热更新会复用现有连接。服务器停机时撤销自己创建的注册并排空在途请求。多个服务端共用注册器时，由所有者在最后一个服务端退出后关闭它。

## 服务治理

| 能力 | Rust 接入方式 |
| --- | --- |
| 健康检查与反射 | 默认提供标准健康服务。为业务服务传入描述符并调用 `with_reflection()` 可开启反射。 |
| Unary 限流 | `with_descriptor(...)` 配合 `with_limiter(TokenBucket::new(rate, burst))`，按每秒补充量和突发容量限制请求，超限返回 `RESOURCE_EXHAUSTED`。 |
| 重试与熔断 | 用 `UnaryPolicy::call` 包装幂等 unary 方法，按需配置 `RetryPolicy` 和 `CircuitBreaker`。所有尝试共享原截止时间。流式方法直接使用生成客户端。 |
| TLS 与 keepalive | 服务端通过 `with_transport` 传入配置好的 `tonic::transport::Server`，发现客户端通过 `DiscoveryOptions` 配置 TLS、HTTP/2 keepalive 和连接超时。 |
| 日志与指标 | 服务端通过 `with_governance` 接入 `GovernanceLayer`，应用负责初始化 tracing subscriber、OpenTelemetry meter 和 exporter。 |

## 开发验证

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

TLS 测试使用 `openssl`。运行额外的 etcd 集成用例时，将 `RPC_ETCD_ENDPOINT` 指向测试专用的 etcd，并将 `ETCD_BIN` 设为 etcd 服务端可执行文件的路径，然后执行 `cargo test --locked --workspace -- --ignored`。
