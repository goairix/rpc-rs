# rpc-rs 开发约定

本文件适用于整个仓库。目标是让 Rust 与 Go 服务使用同一份业务 proto 互调，并可通过双方兼容的后端互相发现。Rust 底座采用 grpc-rust 项目已发布的 tonic。

## 工作依据

- 开始改动前，先查[设计规格](docs/superpowers/specs/2026-11-26-rust-rpc-design.md)中的协议、默认值和状态码表；按[实施计划](docs/superpowers/plans/2026-11-26-rust-rpc.md)找到对应的验收用例。
- 核对 Go 行为时，阅读相邻 `../rpc` 仓库的当前实现、测试与锁定依赖。Go 包用来对齐可观察行为；Rust 的模块、接口和并发模型按 Rust 习惯设计。互通夹具放在本仓库。
- 在当前工作目录开发。开始前检查 Git 状态，保留已有改动。创建 worktree、修改 Go 产品代码、推送或发布需有用户单独指示。
- 小步提交。每完成一个可独立验证的小任务，就运行相关检查、审阅暂存差异并提交，再开始下一项。较大的任务按可验证的行为拆分；同一行为的实现、测试和必要文档一起提交。提交信息沿用 `feat:`、`fix:`、`test:`、`docs:` 等前缀。

## Rust 实现

- 给重要变量、常量、枚举变体及其字段、结构体字段写注释，说明它们代表什么，以及使用时需要知道的单位、取值范围或生命周期。例如，etcd watch 游标要注明它保存的是“下一次订阅的 revision”还是“最后已处理的 revision”。
- 给重要的方法和函数写注释，说明用途，以及调用方需要了解的错误、状态变化或资源清理责任。公开 API 使用 `///` 文档注释；关键局部变量使用相邻的 `//` 注释。
- 在复杂或重要的逻辑旁写 `//` 注释，说明这样处理的原因和必须保持的条件。例如，连接断开时校验连接代号，是为了防止旧连接的迟到事件摘除新连接。行为变更时同步更新注释。

- 直接接入 tonic 生成的强类型服务与客户端，用 Tower 组合中间件。用 enum 表达状态与策略、Duration 表达时间、Result 返回可恢复错误。日志与指标接入应用提供的 tracing subscriber 和 OpenTelemetry meter。
- 明确连接、租约、watch 和后台任务的所有者。后台任务提供取消和等待退出的路径；Drop 释放本地资源，远程撤销和任务回收通过显式异步关闭完成。网络等待和用户回调都在锁外执行。
- 保留流的背压。重试由调用方针对幂等 unary 方法显式开启；每次重建请求、保留 metadata 并使用同一截止时间。流式消息按需处理。日志和指标从响应头或最终 trailers 读取终态，每次调用只记一次。

## 跨语言契约

- Rust 对外只用标准 gRPC 状态码 0–16。Go 端的非标准码在 Rust 端表现为 `UNKNOWN`。变更错误处理时，分别验证调用方向和 unary／streaming，预期以规格中的错误码表为准。
- 注册、名称解析与订阅契约集中在 `crates/naming`；每个后端的注册与解析实现放在同一后端模块。服务端依赖 `naming::Registry`，连接层依赖 `naming::Resolver`。新增后端时复用公共路由与服务生命周期逻辑。下述 key、lease 和 revision 约定适用于 etcd 实现。
- etcd 默认 namespace 为 `grpc`，注册 key 为 `/<namespace>/<service_name>/<instance_id>`。保持 Go JSON 字段名、Unix 秒时间戳及对 `tags`、`properties` 为 null 的兼容。更新治理字段时保留原 key 和 lease。
- 同一 registry 的同名注册应幂等。租约恢复复用实例 ID、key、注册时间与最新治理状态；启动回滚和停机只撤销当前所有者创建的实例。
- 注册端从首次 Put 的 revision + 1 监听自身 key，发现端从线性快照的 revision + 1 监听服务前缀。遇到 compaction 后先重新读取当前值，再续订。
- 仅向状态为 `up` 且连接可用的实例分流。默认策略为 `round_robin`，另外支持 `pick_first` 和 `weighted_round_robin`。只改权重时复用连接。Tower 的 `poll_ready` 表示背压，无节点时由 `call` 路径让生成客户端收到 `UNAVAILABLE`。

## 验证与交付

按变更选择检查。Cargo workspace 和互通夹具建立后，使用以下命令：

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
interop/run.sh all
```

- 涉及协议、路由、错误或流生命周期时，使用真实 Go 和 Rust 进程，从双方框架的正常入口运行互通用例。验证权重热更新需使用两个同名实例，按响应实例 ID 统计交换权重前后的分布。
- etcd 集成测试使用唯一 namespace，清理本次创建的 key、lease 和进程。compaction 在测试专用的临时 etcd 上执行，它会影响整个集群的历史版本。
- 交付时说明实际运行的检查、结果与剩余验证项。仅修改文档时，核对路径、命令和格式即可。
