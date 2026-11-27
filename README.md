# rpc-rs

rpc-rs 为基于 tonic 的 Rust gRPC 服务提供注册、发现和负载均衡。服务端挂载从业务 proto 生成的服务，客户端用同一套强类型接口调用实例。部署时可以使用静态服务目录，也可以用 etcd 跟踪实例的上线和下线。支持 unary 和三种流式 RPC，错误使用标准 gRPC 状态码。

[快速开始](#快速开始) · [接入指南](docs/integration.md) · [示例](examples/greeter/README.md) · [参与贡献](#参与贡献)

## 特性

- **服务发现**　从静态 JSON 或 etcd 按服务名解析实例，也可实现 `Registry` 和 `Resolver` 接入自己的注册中心。
- **负载均衡**　在可用连接间轮询，支持加权轮询和优先使用首个可用实例。更新权重时复用已有连接。
- **调用治理**　支持服务端限流，以及客户端按方法启用的重试和熔断。幂等 unary 调用的所有重试共享同一个截止时间。
- **运维与观测**　提供标准健康检查、可选的 gRPC 反射、tracing 日志和 OpenTelemetry 指标。

## 安装

在应用项目中通过 Git 添加依赖。

```sh
cargo add rpc-rs --git https://github.com/goairix/rpc-rs.git
```

etcd 和静态服务目录均为内置后端。应用在创建服务端或客户端时按配置选择，同一进程可以为不同服务使用不同后端。使用自己的 proto 时，生成客户端和服务端所需的依赖及 `build.rs` 配置见[接入指南](docs/integration.md)。

## 快速开始

准备支持 2024 edition 的 Rust 工具链和 Protocol Buffers 编译器 `protoc`。在仓库根目录打开两个终端，运行自带的 Greeter 示例。它从静态目录读取本机地址，无需启动注册中心。

**终端 1** 启动服务端，监听 `127.0.0.1:45051`。

```sh
cargo run --locked -p greeter-example --bin server
```

**终端 2** 按服务名 `example.v1.Greeter` 发起调用。

```sh
cargo run --locked -p greeter-example --bin client -- Ada
```

客户端输出 `Hello, Ada! (Rust)`。在服务端终端按 Ctrl-C 即可优雅退出。[服务端源码](examples/greeter/rust/src/bin/server.rs) · [客户端源码](examples/greeter/rust/src/bin/client.rs) · [服务目录](examples/greeter/services.json)

## 参与贡献

欢迎提交 Issue 和 Pull Request。报告问题时请附上 Rust 版本、复现步骤和预期行为。提交改动前可在仓库根目录运行以下检查。

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```
