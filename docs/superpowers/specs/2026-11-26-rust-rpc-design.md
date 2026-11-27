# Rust RPC 框架兼容设计

状态：已实现并通过双向互通验收，结果记录在[实施计划](../plans/2026-11-26-rust-rpc.md)。

## 目标与事实来源

在当前空仓库实现与 `../rpc` Go 框架功能对应的 Rust 库。Go 与 Rust
使用同一份业务 `.proto` 互调。注册和发现后端可替换，服务端依赖 `Registry`，客户端依赖 `Resolver`。静态目录和 etcd 随库提供，由应用在运行时按服务选择。Go 与 Rust 的 etcd 互通使用同一个 namespace。
功能范围和跨语言可观察行为以 Go 当前代码与测试为参照，历史设计文档仅供参考。
Go 包是功能对标，不是 Rust 内部架构、公开 API 或实现算法的模板。

用户明确要求：Rust 版本只使用标准 gRPC 状态码（0–16）。这是与 Go
自定义错误码行为的有意差异，不要求逐字复制 Go API。

## 技术选择

使用用户指定的 `grpc/grpc-rust` 仓库所提供的 tonic 已发布版本，而非
正在进行破坏性变更的 master；使用 Tokio 异步运行时与 prost 代码生成。
通过 tonic/Tower 的扩展点组合服务治理，不修改或 fork gRPC 底座。
新增 Rust 依赖限于完成协议、etcd、异步运行时、TLS、日志和指标所必需的库。
具体版本在实施时依据已发布版本及依赖兼容性锁定到 Cargo.lock。

替代方案：直接跟踪 grpc-rust master 会引入不稳定 API；自行实现 HTTP/2
和 protobuf 会重复底座已有能力。这两种方案均不采用。

不改动 `../rpc` 的产品代码，不创建 worktree，不 push 或发布版本。

## Rust 原生设计原则

用户明确要求实现充分利用 Rust 特性，不能逐项翻译 Go 类型、接口和并发机制。
兼容约束落在业务 proto、gRPC wire protocol、etcd key/JSON 和服务治理能力上；
内部结构、模块组织与调用方式允许采用更合适的 Rust 设计。以下原则用于审阅实现：

- 使用 tonic 生成的强类型服务与客户端，以及 Tower 的 Service/Layer 组合。
  常规组合优先使用泛型与静态分发，仅在异构存储等实际边界使用 trait object
  或类型擦除；不复制 Go 的运行时反射注册。
- 状态和负载均衡策略使用 enum，超时和间隔使用 Duration；使用 Result
  表达可恢复错误、Option 表达可选配置。配置提供合理 Default 与必要校验，
  不机械复制 Go 的函数选项或为每个字段增加 builder 层。
- 通过所有权明确 registry、发现订阅、连接与后台任务的生命周期；仅在需要
  共享所有权时使用 Arc。Send/Sync 约束明确跨任务边界，不依赖全局可变单例。
- 网络与等待使用 Tokio async/await；后台任务必须有取消信号和可等待的
  JoinHandle。使用有界 channel 传递事件，用 watch 等机制发布最新状态；
  不机械地为每条 goroutine 路径生成一个 task，不在持锁期间 await 网络操作。
- 优先用所有权与作用域释放本地资源，通过 Drop/RAII 释放探测名额和发送取消
  信号。远程 lease 撤销及任务 join 使用显式 async close，不能假设 Drop 能
  等待异步清理。future 被取消或 drop 同样必须保持状态机一致性。
- 传输实现遵循 Tower poll_ready/call 的背压约定；流式处理采用 Stream 与
  按需拉取，不为日志、指标或重试把完整消息流收集到内存。
- 熔断和负载均衡按 Rust 的并发模型设计，保证行为与性能目标，不要求复刻 Go
  的原子位编码、sync.Pool 或 endpointsharding 内部结构。共享状态尽可能窄，
  只有经正确性与性能需求证明必要的路径才使用复杂原子算法。
- 库将日志和指标接入交给应用，通过 tracing 与外部 OpenTelemetry meter
  组合；不擅自安装全局 subscriber、runtime 或 exporter。

文档中的模块列表表达职责边界，不要求与 Go 目录一一对应。实现计划应先明确
可编译的 Rust 类型关系和资源所有权，再分解任务。

## 公开模块与接入方式

- `config`：服务监听地址、反射开关、etcd endpoints/namespace，以及显式超时、
  租约、认证和 TLS 配置。etcd 默认 namespace 为 `grpc`、租约 5 秒、超时 2 秒。
- `metadata`：注册入参、持久化服务元数据和服务状态。
- `naming`：注册、解析、订阅契约与服务元数据。静态文件和 etcd 后端各自封装注册与解析实现；连接维护和选址由消费快照的 `discovery` 层负责。
- `balancer`：`pick_first`、`round_robin`、`weighted_round_robin`；默认轮询。
- `server`：组合生成的 tonic 服务、内置治理层、健康检查和可选反射，
  提供异步 serve、显式关闭和优雅退出能力。
- `discovery` 按服务名创建可交给 tonic 生成客户端的传输服务，负责选址、TLS 和
  keepalive。客户端的强类型 unary 调用入口组合可选重试和熔断。
- `middleware`、`limiter`、`retry`、`breaker`：可组合的服务治理组件。
- `errors`、`logger`、`metrics`、`health`：标准状态、tracing 日志、
  OpenTelemetry 指标与标准健康服务。

Rust 使用生成的强类型 Service 接入，无需模拟 Go 反射注册函数。
注册中心允许单独使用；服务端封装需明确拥有或借用注册中心，防止停止一个
服务端误关共享资源。公开示例必须演示所有权、关闭顺序和错误处理。

## 注册与发现协议

etcd key 必须为 `/<namespace>/<service_name>/<instance_id>`。值采用 JSON：

```json
{
  "service_name": "example.Echo",
  "version": "v1.0.0",
  "address": "127.0.0.1:50051",
  "env": "test",
  "weight": 100,
  "tags": [],
  "status": "up",
  "register_time": 0,
  "instance_id": "uuid",
  "properties": {}
}
```

字段名与 Go 完全一致，注册时间为 Unix 秒。解析需兼容 Go 对 nil tags、
properties 编码产生的 JSON null，缺失字段采用兼容默认值，未知字段可忽略。
`version` 取注册入参，不因 DefaultVersion 常量存在就强制补值。
`app_name` 不属于持久化协议，不承诺 Go 当前未实现的运行时行为。

注册生命周期必须保证：

1. 同一 registry 内重复注册同一服务幂等；不同服务可独立注销。
2. 一个进程注册生命周期内 instance ID、key、注册时间不变。
3. Grant → 带 lease 的 Put → KeepAlive；建立过程中失败尽力撤销新 lease。
4. 丢失租约后，以 100ms 起步、最大 5s 的退避恢复同一 key；复用唯一 client。
5. 保存首次带租约 Put 的响应 revision R，从 R + 1 监听自身 key。处理完一批事件后
   推进游标，watch 断开时从游标续订。遇到 compaction 时线性 Get 自身 key，
   若值存在则更新缓存，再从 Get 响应头的 revision + 1 续订。租约恢复沿用同一监听任务。
   外部更新保留本地 service_name、address、instance_id、register_time，其余字段写入
   缓存供租约恢复使用。回调收到独立数据副本。
6. 回调允许重入注册/注销；不持内部锁执行回调，不让回调等待自身任务退出。
7. 注销/关闭先禁止恢复，再取消并等待后台任务，最后撤销 lease；关闭幂等。
8. Drop 提供取消兜底，确定性清理由显式异步 close 完成；不依赖进程退出清理。

发现必须先线性读取完整快照，再从 revision + 1 watch；compaction 后重新
读取快照。watch 断开退避恢复，永久错误对调用方可见。定时全量同步默认 2 小时，
并支持显式刷新。过滤不高于快照 revision 的积压事件，避免旧状态覆盖新状态。
只选取 `status=up` 的实例；down/starting/stopping 不参与选择。
各发现实例独立拥有连接和取消生命周期，不使用全局 resolver 注册表。

## 负载均衡与传输

缺失或非正权重按 100 处理；格式错误的数据应记录并跳过，不能中止整个发现。
权重更新不重建未变更地址的连接，地址新增/删除更新连接集合。
每个 `up` 实例在完成 HTTP/2 连接后进入可选集合。连接驱动报告断开或连接失败时，
摘除对应连接，保存传输错误，并在后台退避重连。重连成功且该实例仍为 `up` 时重新加入；
地址变更或实例下线后，丢弃旧连接的迟到通知。三种策略均从当前可选集合中选址。
Channel 的 `poll_ready` 只用于发送背压。业务 RPC 返回的 `UNAVAILABLE` 仅参与该次
调用的容错判断。连接全部失效时，调用返回 `UNAVAILABLE` 并保留连接失败原因。
加权策略使用平滑加权轮询，小权重可预计算；大权重不得通过粗略整数缩放扭曲比例，
也不得按未经限制的权重总和分配内存。请求选择路径避免全局互斥锁。

支持 unary、客户端流、服务端流、双向流；保留 metadata、deadline/cancellation
和 protobuf payload。支持服务端及客户端 TLS 和 keepalive 配置。
Rust API 不强行模拟 Go 独有的反射、DialOption 或 resolver 类型。

## 状态码与容错

Rust 仅发送和公开标准 `tonic::Code` 状态码，不实现 Go 数字状态码的传输兼容层。
框架生成的错误：限流为 ResourceExhausted，熔断开启/无可用实例为 Unavailable，
panic 恢复为 Internal，调用取消与超时分别保持 Cancelled 和 DeadlineExceeded。
业务处理函数直接返回 tonic::Status，已有标准状态和详情应保持不变。

Go 当前直接把 10000–19999 放进 grpc-status；tonic 将这些非标准码识别为
Unknown。该行为需在 README 与互通测试中明确，不能声称自定义业务码无损互通。
业务错误迁移建议由业务应用映射到 InvalidArgument、FailedPrecondition、
AlreadyExists 等适当标准状态；本库不根据错误消息猜测错误类别。

在 protobuf 消息编码前，由强类型 unary 调用入口执行重试。调用方提供可重建的请求
和生成客户端的 unary 方法。每次尝试从原始消息和 metadata 构造新请求，按同一
绝对截止时间设置剩余超时，再调用该方法。三种流式方法直接使用生成客户端。
默认总尝试 3 次，初始等待 100ms、最大 1s、倍数 2，
可重试码为 Unavailable、DeadlineExceeded、ResourceExhausted、Internal。
总次数为 0 时只尝试一次。退避等待支持取消，deadline 约束整个调用，
不重放已经消费的流。重试为显式启用，调用方负责方法幂等性。

熔断显式启用，包裹整个 unary 重试操作；默认连续失败 5 次开启、10 秒后半开、
半开成功 3 次关闭，同一时刻仅允许一个半开探测。旧世代的并发请求结果不得
改变新世代状态。默认所有非 OK 错误计为失败，提供可配置失败判断以排除
业务应用选定的标准状态码，不宣称能从标准码恢复 Go 的业务码范围语义。
取消、panic 或丢弃请求时应释放半开探测名额。

令牌桶速率最小为每秒 1，支持容量、立即 allow 与可取消 wait。
与 Go 一致，内置自动限流针对 unary，不默认限制流中的每条消息。

## 中间件、可观测性与运维

支持明确顺序的可组合服务层。默认行为包括 panic 恢复、日志、指标与标准错误保留；
流式恢复、日志及指标覆盖流生命周期与最终 trailers，不能在返回响应头时就记为成功。
普通成功日志用 debug，业务可配置的错误用 info，其余失败用 error；没有安装
日志 subscriber 或 meter 时安全工作。

指标名称为 `<name>_rpc_requests_total` 和 `<name>_rpc_request_duration_seconds`，
计数标签为 method/status，耗时标签为 method，耗时单位秒；支持外部 meter。
collector endpoint 不暗示自动初始化 exporter，导出由应用接入配置。

提供标准 grpc.health.v1 服务。为对应 Go 当前 Check 行为，Check 返回 SERVING；
不因 tonic-health 默认的未知服务行为而引入跨语言差异。反射仅在配置开启时
注册，并通过应用提供的 descriptor set 描述实际业务服务。
停止时先注销服务，再优雅等待在途请求；注销失败仍继续执行其余清理并返回错误。

## 验收

- 单元测试覆盖 JSON/null/默认值、状态过滤、权重分布与极端权重、
  重试取消、令牌桶、熔断并发世代与半开探测释放。
- 审阅公开 API 的类型约束、所有权与 Send/Sync 边界；验证 future 被 drop、
  背压和消费者提前结束流时资源正确释放，不产生失控任务或无界消息缓存。
- 真实 etcd 集成测试覆盖幂等注册、租约撤销及恢复、稳定身份、治理更新、
  状态回调重入、发现增删和关闭后不再恢复。使用唯一 namespace，清理仅限测试资源。
- 在首次 Put 已完成、注册端尚未发起 watch 的窗口内，用原 lease 将实例改为
  `down`。分别覆盖可回放历史和起始 revision 已被压缩两种情形。等治理回调
  确认收到 `down` 后撤销租约，验证恢复的 key、instance_id 和 `down` 状态。
  执行 compaction 的用例使用独立临时 etcd，因为 compaction 作用于整个集群。
- 同一份 proto 提供 Go 和 Rust 示例及一键互通命令。Go 端使用 `../rpc`
  框架而非仅裸 grpc-go；测试夹具放在当前 Rust 仓库，不更改 Go 仓库。
- 双向验证 unary、三种 streaming、metadata 和 deadline。错误用例使用下表预期，
  通过两端框架的正常服务入口调用。流式用例读取至流结束并断言最终状态。
- 启用 Rust unary 重试，让服务端第一次返回 `UNAVAILABLE`、第二次成功；
  验证收到两次相同消息和 metadata，且第二次超时预算已扣除退避耗时。
  同一服务的服务端流方法首次返回 `UNAVAILABLE` 时，验证调用次数为一。
- 双向通过 etcd 发现服务；验证 up/down、权重修改及注销后的路由变化。
- 对三种策略分别启动两个可用实例，保持两条 etcd 记录均为 `up`，断开当前被选
  实例的连接并关闭端口。关闭 unary 重试，等待摘除后验证新请求全部到达健康实例。
  重开端口后验证重连成功；轮询策略恢复分流，`pick_first` 在当前连接也断开时可切回。
- 执行 cargo fmt --check、cargo clippy --all-targets -- -D warnings、cargo test，
  以及真实 etcd 与 Go/Rust 互通测试；验证优雅停机后后台任务与监听端口释放。
- README 列明接入、proto 生成、配置、所有权/关闭、验证命令及兼容差异。
  未实际运行的验证不得标记为通过。

| 客户端 → 服务端 | 服务端行为 | 客户端预期状态 |
|---|---|---|
| Go → Rust | unary handler 返回参数错误，或流以参数错误结束 | `INVALID_ARGUMENT` |
| Go → Rust | unary 请求触发 Rust 限流 | `RESOURCE_EXHAUSTED` |
| Rust → Go | unary handler 返回 `status.Error(codes.InvalidArgument, ...)` | `UNKNOWN`，由 Go 默认 unary 错误拦截器转换 |
| Rust → Go | unary 请求触发 Go 内置限流 | `UNKNOWN`，Go 外层错误拦截器转换限流错误 |
| Rust → Go | unary handler 返回 Go 框架业务码 11001 | `UNKNOWN`，tonic 解析非标准码的结果 |
| Rust → Go | 流式 handler 返回 `status.Error(codes.InvalidArgument, ...)` | `INVALID_ARGUMENT`，Go 流式链保留该状态 |

## 参考

- Go 当前实现：`../rpc/server.go`、`discovery.go`、`naming/etcd/`、
  `metadata/`、`errors/`、`breaker/`、`retry/`、`middleware/`。
- 指定底座：https://github.com/grpc/grpc-rust
- 标准状态码：https://docs.rs/tonic/latest/tonic/enum.Code.html
