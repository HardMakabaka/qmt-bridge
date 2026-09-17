# 可观测性与关联追踪

Bridge、SDK 和 Big QMT 内嵌运行时把诊断事件写为本地 JSONL。它用于定位一次调用走过
HTTP、RPC、数据、任务和交易阶段；不会替代柜台回报、数据覆盖验收或业务对账。

## 启用与文件保留

默认启用。Windows 默认目录为 `%LOCALAPPDATA%\QmtBridge\traces`（没有
`LOCALAPPDATA` 时为 `~\.qmt_bridge\traces`）。文件名带角色、PID 和进程实例 ID；
同一进程的当前文件最多 **5 MiB**，保留 3 个轮转备份，因此该进程最多约 20 MiB。
这不是整个目录上限：不同进程、旧 PID 或旧实例的文件会保留，需由运维按保留策略只读
检查后另行处理。

`.env` 由 server CLI 加载进进程环境，因此下列变量对 `qmt-server` 生效：

```dotenv
# true/false、1/0、yes/no、on/off；默认 true
QMT_BRIDGE_TRACE_ENABLED=true
# 留空使用 Windows 默认目录
# QMT_BRIDGE_TRACE_DIR=C:\trace\qmt-bridge
# 供直接使用 telemetry 模块的进程指定角色；bridge/策略/安装器会设置自己的角色
# QMT_BRIDGE_TRACE_ROLE=research-worker
```

没有额外的 telemetry CLI flag。`scripts/manage-bridge.ps1` 在未设置
`QMT_BRIDGE_TRACE_ENABLED` 时也默认启用 controller telemetry，文件名为
`bridge_controller-<PID>-<instance>.jsonl`，与 bridge 子进程的文件独立。未设置
`QMT_BRIDGE_TRACE_DIR` 时使用 `%LOCALAPPDATA%\QmtBridge\traces`；显式传入
`-LogDirectory` 时使用其 `telemetry` 子目录。后台启动会把解析后的 trace 目录传给子进程，
保证 controller 与 child 可在同一目录关联。

嵌入/集成方可直接调用（配置会关闭并重建当前 writer，不应在热路径反复调用）：

```python
from bigqmt_signal_trader import telemetry

telemetry.configure(enabled=True, directory=r"C:\trace\qmt", role="worker",
                    max_bytes=5 * 1024 * 1024, backup_count=3, queue_size=4096)
```

`flush(timeout)` 只等待队列排空和 Python 文件缓冲写入，**不执行 fsync**，不能当作断电
持久化保证。

## 关联字段与读取规则

每行是一个 JSON 对象，包含 `schema_version`、`timestamp_utc`、
`process_instance_id`、`service_role`、`event_name`、`outcome`，以及可用的
`trace_id`、`span_id`、`parent_span_id`。字段值经过长度限制和敏感键脱敏；不要把它
作为密钥、完整账户信息或原生 payload 的存储位置。

SDK 会从当前上下文发出 `X-QMT-Trace-Id`、`X-QMT-Span-Id`、`X-Request-ID`；服务端
只接受格式合法的值并在 HTTP 响应写入 `X-QMT-Trace-Id`、`X-Request-ID`。这些头用于
关联而非认证。RPC wire envelope 的字段名是 `request_id`；记录事件时它被写为
`rpc_request_id`。trace 会跨 RPC 传递；两者不应混为同一个 ID。

下载任务保存提交时的 trace context；订单/撤单按 `client_submit_id`、`order_sys_id` 和
可能的 `trade_id` 关联；执行事件使用 `event_epoch` / `event_sequence` 游标。持久订单/成交
PUB 与 replay envelope 可选地携带 trace，旧事件仍可用新的 root trace 加其业务 ID 和 cursor
关联。后台任务或全市场采样器可能创建新的 trace，并以 `caused_by_trace_link` 指向触发者，
而不会伪造为同一个同步 span。

### 只读检索示例（PowerShell）

以下命令不修改文件；轮转备份也会被搜索。按需替换 ID，且先确认目录是本次排查目标：

```powershell
$dir = Join-Path $env:LOCALAPPDATA 'QmtBridge\traces'
$traceId = '0123456789abcdef0123456789abcdef'
Get-ChildItem -LiteralPath $dir -File -Filter '*.jsonl*' |
  Select-String -SimpleMatch ('"trace_id":"' + $traceId + '"')

$jobId = 'your-job-id'
Get-ChildItem -LiteralPath $dir -File -Filter '*.jsonl*' |
  Select-String -SimpleMatch ('"job_id":"' + $jobId + '"')

$orderId = 'your-order-sys-id'
Get-ChildItem -LiteralPath $dir -File -Filter '*.jsonl*' |
  Select-String -SimpleMatch ('"order_sys_id":"' + $orderId + '"')
```

## 如何解释结果

- HTTP `outcome=success` 或 2xx 仅证明 ASGI 已完成发送响应；不代表订单已成交、历史数据
  完整或下载已在终端可见。若 HTTP 500 由外层 `ServerErrorMiddleware` 生成，trace middleware
  未必能附加响应头；SDK 已传入的 trace 仍可用于本地 JSONL 关联。
- 下单/撤单的 `unknown` 表示副作用可能已经发生但尚未确认。按原有
  `client_submit_id` 或委托身份查询/对账，**不得因为 unknown 自动重放**。
- `download_job_completed` 是事件，但 `download_invocation_completed` 是任务字段，不是 telemetry
  事件。它、可见性和覆盖验证是不同阶段；`history_visibility_verified` 仅表示每个请求标的
  至少有一条可用数据，`coverage_verified` 当前始终为 `false`，完整窗口仍须业务验收。
- `telemetry.stats()` 提供 `queue_depth`、`dropped`、`write_failures`、
  `critical_dropped`、`critical_write_failures`、`loss_events`、`pending_loss` 等状态；
  `GET /api/meta/health` 公开其中的核心失效/丢失计数。关键事件不做抽样，但队列压力或磁盘
  写失败仍可能造成缺口；随后可写出的 `telemetry.loss` 会记录可见的损失摘要。

## 事件覆盖矩阵

| 链路 | 当前事件前缀或关键事件 | 文件 / 现有聚焦测试 |
|---|---|---|
| HTTP 与 SDK | `sdk.http.*`、`http.request.*`、`http.auth` | `client/base.py`、`server/observability.py`；`test_http_telemetry.py` |
| RPC 与 QMT | `rpc.client.*`、`rpc.transport.zmq.*`、`rpc.server.*`、`qmt.passorder.*`、`qmt.cancel.*` | `rpc_client.py`、`redis_rpc.py`、`transports/zmq_transport.py`；`test_rpc_telemetry.py` |
| 行情、DAT、缓存、分钟订阅 | `market.*`、`market.dat.read`、`market.binary_cache.*`、`market.minute_tail.*`、`market.subscription.*` | market adapters/routers；`test_market_telemetry.py` |
| 下载与恢复 | `download_job_*`、`runtime.*`、`probe.history.*` | download router、runtime controller、meta router；`test_download_recovery.py` 覆盖 worker trace 恢复与 sector 子进程环境传递 |
| 订单、撤单、信号 | `trading.http.*`、`trading.manager.*`、`rpc.trade.*`、`signal.*` | trading router/manager、RPC、signal app；`test_trading_telemetry.py` |
| 执行事件与 WebSocket | `execution_event.*`、`ws.realtime.*`、`ws.whole_quote.*`、`ws.trade.*` | event/WS modules；`test_event_telemetry.py`、`test_ws_telemetry.py` |
| 通知 | `notify.*` | notify modules；`test_notify_telemetry.py` |
| Formula 路由 | `formula.router.*`、`formula.history.lane` | FormulaServer router；`test_formula_telemetry.py` |
| 内嵌策略与安装/进程 | `strategy.init.*`、`adjust.phase*`、`exec_event_*`、`installer.*`、`process.*` | 策略入口、安装器、CLI；`test_strategy_telemetry.py`、`test_bigqmt_installer.py` |
| SDK WebSocket | `sdk.ws.*` | SDK WebSocket client；`test_http_telemetry.py` 的 SDK WS 回调失败场景 |

可选 Redis/MySQL transport 仍有 `rpc.transport.redis.*` / `rpc.transport.mysql.*` 埋点；
SHM 仍只报告 `rpc.transport.shm.unsupported`。FormulaServer 的**读取路由**有上述测试，
但 `/ws/formula` 的原生推送合同仍未验证且没有独立的 Formula WebSocket telemetry 测试。
因此矩阵不表示每一个可选后端或每个失败分支均有独立测试。
