# Rust RPC 框架兼容设计

状态：待审阅；实现尚未开始。

## 目标与事实来源

在当前空仓库实现与 `../rpc` Go 框架功能对应的 Rust 库。Go 与 Rust
使用同一份业务 `.proto`、同一 etcd namespace，可以互相发现并调用服务。
实现行为以 Go 当前代码与测试为准，历史设计文档仅供参考。

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

## 公开模块与接入方式

- `config`：服务监听地址、反射开关、etcd endpoints/namespace，以及显式超时、
  租约、认证和 TLS 配置。etcd 默认 namespace 为 `grpc`、租约 5 秒、超时 2 秒。
- `metadata`：注册入参、持久化服务元数据和服务状态。
- `naming`：注册中心抽象、etcd 注册实现和服务发现生命周期。
- `balancer`：`pick_first`、`round_robin`、`weighted_round_robin`；默认轮询。
- `server`：组合生成的 tonic 服务、内置治理层、健康检查和可选反射，
  提供异步 serve、显式关闭和优雅退出能力。
- `discovery`：按服务名创建可交给 tonic 生成客户端的传输服务；配置
  负载均衡、TLS、keepalive、可选重试和熔断。
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
5. 监听自身 key 的外部修改，保留 service_name、address、instance_id、
   register_time，其余字段进入本地缓存供恢复使用。回调收到独立数据副本。
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
优先选择可用连接，全部不可用时返回真实传输失败，不伪装成功或就绪。
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

重试仅自动应用于 unary：默认总尝试 3 次，初始等待 100ms、最大 1s、倍数 2，
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
- 真实 etcd 集成测试覆盖幂等注册、租约撤销及恢复、稳定身份、治理更新、
  状态回调重入、发现增删和关闭后不再恢复。使用唯一 namespace，清理仅限测试资源。
- 同一份 proto 提供 Go 和 Rust 示例及一键互通命令。Go 端使用 `../rpc`
  框架而非仅裸 grpc-go；测试夹具放在当前 Rust 仓库，不更改 Go 仓库。
- 双向验证 unary、三种 streaming、metadata、deadline、标准错误码，以及
  Go 非标准码被 Rust 识别为 Unknown 的明确边界。
- 双向通过 etcd 发现服务；验证 up/down、权重修改及注销后的路由变化。
- 执行 cargo fmt --check、cargo clippy --all-targets -- -D warnings、cargo test，
  以及真实 etcd 与 Go/Rust 互通测试；验证优雅停机后后台任务与监听端口释放。
- README 列明接入、proto 生成、配置、所有权/关闭、验证命令及兼容差异。
  未实际运行的验证不得标记为通过。

## 参考

- Go 当前实现：`../rpc/server.go`、`discovery.go`、`naming/etcd/`、
  `metadata/`、`errors/`、`breaker/`、`retry/`、`middleware/`。
- 指定底座：https://github.com/grpc/grpc-rust
- 标准状态码：https://docs.rs/tonic/latest/tonic/enum.Code.html
