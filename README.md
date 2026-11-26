# rpc-rs

基于 tonic 的 Rust RPC 库。Go 和 Rust 服务使用同一份 protobuf 定义，可以直连，也可以通过共同的注册发现后端按服务名互调。库入口是 `rpc_rs`，生成的 tonic 服务和客户端可以直接接入。

## 选择注册与发现后端

服务端向 `RpcServer::start` 传入 `Arc<dyn registry::Registry>`，直连时传 `None`。客户端向 `discovery::Discovery::connect` 传入 `&dyn discovery::Resolver` 和服务名。两端独立选择实现，跨语言按名称发现时需要共享兼容的服务目录。

默认启用 Cargo 特性 `etcd`，内置的注册实现是 `EtcdRegistry`，发现客户端可直接传 `&EtcdConfig`。使用其他后端时可以关闭该特性：

```toml
rpc-rs = { path = "../rpc-rs", default-features = false }
```

自定义 `Resolver` 为每个服务返回一个 `DiscoverySubscription`，发布按稳定实例 ID 索引的完整快照，并处理刷新和关闭。框架负责筛选可用实例、维护连接和负载均衡。注册端通过 `Registration::Created` 返回所有权句柄，停机时框架用该句柄注销实例。[内存后端接入测试](tests/custom_backend.rs) 演示了这两个接口。

### 从静态 JSON 文件读取

将服务目录保存为 `services.json`。每个实例填写服务名、对客户端可达的 IPv4 或 IPv6 地址，以及非零端口。同名的多条记录组成实例列表。`weight` 是可选的正整数，默认值为 100，在加权轮询时生效。

```json
{
  "instances": [
    { "service_name": "interop.Echo", "ip": "10.0.0.11", "port": 50051, "weight": 200 },
    { "service_name": "interop.Echo", "ip": "10.0.0.12", "port": 50051 },
    { "service_name": "billing.Invoice", "ip": "10.0.0.13", "port": 50052 }
  ]
}
```

服务端和客户端各自在启动时加载一份。以下示例中第一台服务器监听所有网卡，并通告文件中对应的地址：

```rust
use rpc_rs::{registry::RegistrationOptions, server::RpcServer, static_registry::StaticRegistry};
use std::sync::Arc;

let directory = Arc::new(StaticRegistry::from_file("services.json")?);
let listener = tokio::net::TcpListener::bind("0.0.0.0:50051").await?;
let options = RegistrationOptions { address: "10.0.0.11:50051".into(), ..Default::default() };
let server = RpcServer::default()
    .add_service(EchoServer::new(EchoImpl), options)
    .start(listener, Some(directory));
server.until(async { tokio::signal::ctrl_c().await.ok(); }).await?;
```

客户端按服务名选择实例：

```rust
use rpc_rs::{discovery::{Discovery, DiscoveryOptions, LbPolicy}, static_registry::StaticRegistry};

let directory = StaticRegistry::from_file("services.json")?;
let options = DiscoveryOptions { policy: LbPolicy::WeightedRoundRobin, ..Default::default() };
let mut discovery = Discovery::connect(&directory, "interop.Echo", options).await?;
let client = EchoClient::new(discovery.service());
// 连接异步建立；调用和关闭方式见下文客户端示例。
```

注册时会校验服务名和通告地址。配置文件由部署方维护，实例停机后记录仍在，共用的连接管理器会摘除断开的实例并尝试重连。配置变更在重新加载文件并创建新的注册或发现对象后生效。静态后端支持 `default-features = false`。

## 跑一次双向调用

将 Go 框架放在相邻的 `../rpc`。从本仓库根目录运行：

```sh
interop/run.sh direct
interop/run.sh all
```

`direct` 启动两种语言的测试进程，通过 TCP 直连。 `all` 启动专用 etcd 和真实的框架服务端，用服务名进行发现，运行四种 RPC、标准错误、metadata、超时、取消、限流及权重热更新用例。每个方向使用两个同名实例验证加权分流，脚本退出时关闭子进程并清理临时目录。

需要 Rust/Cargo、Go、protoc、Python 3 和 Unix shell；`all` 还需要 etcd v3 服务端。可通过 `ETCD_BIN=/absolute/path/to/etcd interop/run.sh all` 指定可执行文件。已验证的组合为 Rust 1.95、Go 1.25.6、tonic 0.14.6、protoc 3.20.3 和 etcd 3.6.5。Go 测试桩已随仓库提供，Rust 桩在构建夹具时生成。

业务 proto 的 package、service、方法名、流类型及消息字段号在两端保持一致。修改测试 proto 后，使用已安装的 `protoc-gen-go` 和 `protoc-gen-go-grpc` 更新 Go 桩：

```sh
protoc -I interop/proto --go_out=interop/go --go_opt=module=example.org/rpc-interop \
  --go-grpc_out=interop/go --go-grpc_opt=module=example.org/rpc-interop interop/proto/echo.proto
cargo build -p rpc-interop
```

## 接入 Rust 服务

应用通过路径依赖引用本库，例如 `rpc-rs = { path = "../rpc-rs" }`。在业务 crate 的 build.rs 中用 tonic-prost-build 编译 proto，同时输出 file descriptor set。下面使用测试 proto 生成的 `EchoServer` 和应用自己的 `EchoImpl`。

```rust
use rpc_rs::{
    registry::{EtcdConfig, EtcdRegistry, RegistrationOptions},
    server::RpcServer,
};
use std::sync::Arc;

let config = EtcdConfig::default();
let registry = Arc::new(EtcdRegistry::connect(config).await?);
let listener = tokio::net::TcpListener::bind("127.0.0.1:50051").await?;
let server = RpcServer::default()
    .with_descriptor(tonic::include_file_descriptor_set!("echo_descriptor"))
    .add_service(EchoServer::new(EchoImpl), RegistrationOptions::default())
    .start(listener, Some(registry.clone()));

let result = server.until(async { tokio::signal::ctrl_c().await.ok(); }).await;
registry.close().await?;
result?;
```

默认使用 protobuf 全限定服务名和监听器的实际地址注册。监听 `0.0.0.0` 或位于容器内时，通过 `RegistrationOptions::address` 填入客户端可达的主机名与端口。可多次调用 `add_service`。服务端在停机时注销自己创建的实例并排空在途请求；显式调用 `shutdown().await` 或等待 `until` 可获得清理结果。共享 registry 由其所有者最后关闭。注册状态回调需要调用 registry 时，捕获 `registry.downgrade()` 返回的弱引用，在回调执行期间调用 `upgrade()`。显式借出的 etcd 客户端 clone 由调用方释放。

标准健康检查随服务器启动。反射可用 `with_reflection()` 开启。反射与 `with_limiter(TokenBucket::new(rate, burst))` 均使用随服务桩生成的 descriptor。通过 `with_transport` 传入配置了 TLS 或 HTTP/2 keepalive 的 tonic Server。应用可用 `with_governance` 接入 OpenTelemetry meter 和业务错误日志分类；tracing subscriber、runtime 和 exporter 由应用初始化。

## 客户端发现与容错

```rust
use rpc_rs::{
    discovery::{Discovery, DiscoveryOptions, LbPolicy},
    governance::{CircuitBreaker, RetryPolicy, UnaryPolicy},
    registry::EtcdConfig,
};
use std::time::Duration;

let cfg = EtcdConfig::default();
let options = DiscoveryOptions { policy: LbPolicy::WeightedRoundRobin, ..Default::default() };
let mut discovery = Discovery::connect(&cfg, "interop.Echo", options).await?;
let client = EchoClient::new(discovery.service());
let policy = UnaryPolicy::default()
    .with_retry(RetryPolicy::default())
    .with_breaker(CircuitBreaker::default());
let mut request = tonic::Request::new(message);
request.set_timeout(Duration::from_secs(2));
let response = policy.call(request, None, |request| {
    let mut client = client.clone();
    async move { client.say(request).await }
}).await;
discovery.close().await;
let reply = response?.into_inner();
```

发现连接异步建立，每条新连接通过一次有超时限制的标准 Health/Watch 请求确认对端能返回 HTTP/2 gRPC 响应。第一次调用若需要等待就绪，给等待设置截止时间，并观察 `ready_count()`；无可用连接时生成客户端收到 `UNAVAILABLE`。保留 discovery 与客户端相同的使用寿命，克隆的客户端共享连接。TLS 可在 `DiscoveryOptions::tls` 中配置可信 CA、服务名和客户端证书。默认负载均衡是轮询，另支持优先保持首个可用连接和平滑加权轮询。

重试与熔断由应用在强类型 unary 入口启用。仅对可安全重放的方法启用重试，整个调用共享一个截止时间。流式调用直接使用生成客户端。服务端与客户端示例见 [Rust 夹具](interop/rust/src/main.rs) 和 [Go 夹具](interop/go/main.go)。

## etcd 与错误语义

默认 namespace 为 `grpc`，租约为 5 秒，etcd 操作超时为 2 秒。配置可传入 endpoints、用户名密码和 TLS。每条注册记录使用 `/<namespace>/<service_name>/<instance_id>`。管理端修改状态或权重时应保留原 lease。只有 `up` 且连接可用的实例参与选址。权重热更新复用连接，恢复租约时保留实例 ID 与最近观察到的治理状态。

Rust 只使用标准 gRPC 状态码。Go 客户端能读到 Rust 返回的 `INVALID_ARGUMENT` 和 `RESOURCE_EXHAUSTED`。现有 Go 框架会把 unary handler 的普通错误和内层限流错误转换为 `UNKNOWN`；Go 自定义码在 tonic 中也表现为 `UNKNOWN`。Go 流式 handler 返回的标准码可以直接传递。完整预期见[兼容规格](docs/superpowers/specs/2026-11-26-rust-rpc-design.md)。

## 开发验证

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
interop/run.sh all
```

TLS 测试需要 `openssl`。额外的 etcd 生命周期、watch 与故障切换用例可单独运行。设置 `RPC_ETCD_ENDPOINT` 指向专用测试 etcd，设置 `ETCD_BIN` 为 etcd 可执行文件后运行 `cargo test --workspace -- --ignored`。需要压缩历史的用例会启动自己的 etcd。

若想单独观察两个进程，先构建 Rust 夹具，再在不同终端运行以下两条命令。对调测试时，把第一条换成 Go 的 serve，第二条换成 Rust 的 check。

```sh
cargo run -p rpc-interop -- serve 127.0.0.1:50051 rust
(cd interop/go && GOWORK=off go run . check 127.0.0.1:50051)
```

通过服务名接入时，给两个进程设置相同的 `ETCD_ENDPOINT` 和 `RPC_NAMESPACE`，将客户端的地址参数改为 `discovery`。
