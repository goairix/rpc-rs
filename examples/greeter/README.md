# Greeter 跨语言示例

Rust 和 Go 各自实现一个服务端与一个客户端，共用 [greeter.proto](proto/greeter.proto) 中的 `example.v1.Greeter/SayHello`。回复末尾的语言名来自服务端。Rust 客户端从[静态服务目录](services.json)按名称发现实例，Go 客户端直连同一地址。

从**仓库根目录**运行以下命令。Rust 端需要 Cargo 和 `protoc`，Go 端需要 Go 1.25 或更高版本。示例使用本机端口 `45051`，一次启动一个服务端即可。

## Rust 服务端，Go 客户端

在第一个终端启动服务端。

```sh
cargo run --locked -p greeter-example --bin server
```

在第二个终端发起调用，预期输出 `Hello, Ada! (Rust)`。

```sh
(cd examples/greeter/go && GOWORK=off go run ./cmd/client Ada)
```

## Go 服务端，Rust 客户端

按 Ctrl-C 停止上一个服务端，再在第一个终端启动 Go 服务。

```sh
(cd examples/greeter/go && GOWORK=off go run ./cmd/server)
```

在第二个终端按服务名调用，预期输出 `Hello, Ada! (Go)`。

```sh
cargo run --locked -p greeter-example --bin client -- Ada
```

两个客户端都可以传入空字符串 `""` 验证服务端返回的标准状态码 `INVALID_ARGUMENT`。

## 修改协议

Rust 桩由 [build.rs](rust/build.rs) 在构建时生成。Go 桩随示例提交，修改 proto 后，安装 `protoc-gen-go` 和 `protoc-gen-go-grpc` 并重新生成。

```sh
cd examples/greeter
protoc -I proto --go_out=go --go_opt=module=example.org/greeter \
  --go-grpc_out=go --go-grpc_opt=module=example.org/greeter proto/greeter.proto
```
