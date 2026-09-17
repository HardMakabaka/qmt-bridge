# 配置参考

## 下载通道锁死与健康检查

`/api/meta/history-readiness` 同时检查共享原生调用协调器的锁死标记。
标记存在时返回 `status=unavailable`、`failure_kind=transport`、
`rpc_status=connection_error` 和 `error.code=xtdata_transport_stuck`，不再用另一条
直连 RPC 探针的成功掩盖下载不可用。既有 keepalive 可按传输失败处理，仍须遵守
活动写请求保护和重启限流，不自动重启 QMT 终端。
`/api/meta/recovery-status` 新增只读 `download_transport`，包含 `status`（`available`
或 `blocked`）及原始 `reason`；不提供绕过存活调用的手动清标记接口。

QMT Bridge 支持通过 `.env` 文件、环境变量或 CLI 参数进行配置。优先级：**CLI 参数 > 环境变量 > .env 文件 > 默认值**。

## 配置项

| 环境变量 | CLI 参数 | 默认值 | 说明 |
|---------|---------|-------|------|
| `QMT_BRIDGE_HOST` | `--host` | `0.0.0.0` | 监听地址（`0.0.0.0` = 允许局域网访问） |
| `QMT_BRIDGE_PORT` | `--port` | `13543` | 监听端口 |
| `QMT_BRIDGE_SINGLETON` | `--no-singleton` | `true` | 启动前非破坏性检查端口；占用则拒绝，不停止任何进程 |
| `QMT_BRIDGE_LOG_LEVEL` | `--log-level` | `info` | 日志级别：`critical` / `error` / `warning` / `info` / `debug` |
| `QMT_BRIDGE_WORKERS` | `--workers` | `1` | Worker 数量（Windows 下建议保持 1） |
| `QMT_BRIDGE_BINARY_CACHE_ENABLED` | — | `true` | 是否启用历史/基础读取的本地二进制缓存 |
| `QMT_BRIDGE_BINARY_CACHE_DIR` | — | `%LOCALAPPDATA%\qmt-bridge\binary-cache` | 缓存目录 |
| `QMT_BRIDGE_BINARY_CACHE_TTL_SECONDS` | — | `86400` | 缓存 TTL 秒数 |
| `QMT_BRIDGE_BINARY_CACHE_MAX_BYTES` | — | `2147483648` | 缓存最大容量，超出后按最旧文件清理 |
| `QMT_BRIDGE_API_KEY` | `--api-key` | _(空)_ | API Key，用于保护交易端点 |
| `QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA` | — | `false` | 数据端点是否也要求认证 |
| `QMT_BRIDGE_RUNTIME` | — | `bigqmt` | 固定 Big QMT 运行时 |
| `QMT_BRIDGE_QMT_ROOT` | `--qmt-root` | _(空)_ | Big QMT 终端根目录；写门禁从其 `userdata/log` 验证当前模型 request id 的 `m_bTrade` |
| `QMT_BRIDGE_RPC_TRANSPORT` | — | `zmq` | 固定 ZMQ RPC |
| `QMT_BRIDGE_ZMQ_ENDPOINT` | `--zmq-endpoint` | `tcp://127.0.0.1:15560` | 仅允许回环地址 |
| `QMT_BRIDGE_EVENT_ZMQ_ENDPOINT` | `--event-zmq-endpoint` | `tcp://127.0.0.1:15561` | 独立委托/成交事件通道；仅允许回环地址且必须与 RPC 端口不同 |
| `QMT_BRIDGE_FORMULA_HISTORY_READ_WORKERS` | — | `2` | FormulaServer 未复权历史 K 线批量读取连接数，只支持 `1` / `2`；设为 `1` 恢复串行 |
| `QMT_BRIDGE_ACCOUNT_ENABLED` | `--account-enabled` | `false` | 启用账户只读查询 |
| `QMT_BRIDGE_TRADING_ACCOUNT_ID` | `--account-id` | _(空)_ | 交易账户 ID |
| `QMT_BRIDGE_ORDER_WRITES_ENABLED` | `--order-writes-enabled` | `true` | API 层委托写入门禁；可显式设为 `false` 冻结写入，终端层仍需同时启用 |

委托写入还要求 `MECOSTOCK_BIGQMT_ZMQ` 在 QMT 中处于“实盘”运行模式。模型 ping 只上报当前 request id，外部 bridge 再从 QMT 原生日志核对该 request id 最近一次 `doRun` / `changeTradeType` 的 `m_bTrade`；缺日志、无法匹配或值为 `0/false` 均 fail closed。

## .env 文件示例

```bash
# QMT Bridge 配置
# 复制此文件为 .env 并按需修改:  cp .env.example .env

# 监听地址 (0.0.0.0 表示允许局域网访问)
QMT_BRIDGE_HOST=0.0.0.0

# 监听端口
QMT_BRIDGE_PORT=13543

# uvicorn 日志级别: critical/error/warning/info/debug
QMT_BRIDGE_LOG_LEVEL=info

# uvicorn worker 数量 (Windows 下建议保持 1)
QMT_BRIDGE_WORKERS=1

# 本地二进制缓存（历史行情/基础读取；实时 snapshot 不缓存）
QMT_BRIDGE_BINARY_CACHE_ENABLED=true
# QMT_BRIDGE_BINARY_CACHE_DIR=C:\Users\YourName\AppData\Local\qmt-bridge\binary-cache
QMT_BRIDGE_BINARY_CACHE_TTL_SECONDS=86400
QMT_BRIDGE_BINARY_CACHE_MAX_BYTES=2147483648

# API Key（用于保护交易端点，留空则交易端点不可用）
# QMT_BRIDGE_API_KEY=your-secret-api-key

# 是否要求数据端点也进行认证（默认否，仅交易端点需要认证）
# QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA=false

# Big QMT ZMQ 运行时
QMT_BRIDGE_RUNTIME=bigqmt
# QMT_BRIDGE_QMT_ROOT=C:\国金证券QMT交易端
QMT_BRIDGE_RPC_TRANSPORT=zmq
QMT_BRIDGE_ZMQ_ENDPOINT=tcp://127.0.0.1:15560
QMT_BRIDGE_EVENT_ZMQ_ENDPOINT=tcp://127.0.0.1:15561
QMT_BRIDGE_FORMULA_ENABLED=true
QMT_BRIDGE_FORMULA_HOST=127.0.0.1
QMT_BRIDGE_FORMULA_PORT=58600
QMT_BRIDGE_FORMULA_HISTORY_READ_WORKERS=2

# 账户只读查询与委托写入分别控制
# QMT_BRIDGE_ACCOUNT_ENABLED=true
# QMT_BRIDGE_TRADING_ACCOUNT_ID=12345678
# QMT_BRIDGE_ORDER_WRITES_ENABLED=true
```

RPC 请求/响应使用 `15560`；委托和成交回报使用独立的 ZMQ PUB/SUB `15561`。
事件带 `{epoch, sequence}` 游标，内嵌运行时默认保留最近 2000 条用于断线回放；
两条通道都必须保持在本机回环地址，不能复用同一端口。

FormulaServer 的两路读取只拆分单次多股票历史 K 线请求；每路使用独立连接，
日期、字段、频率与 `count` 保持一致。两路均完成后才合并返回，任一路失败仍走
原有完整请求的串行 RPC 回退，不交付半批数据。单股、tick、复权行情、ZMQ、
下载任务与缓存写入、交易调用，以及外层 provider 容量限制均不放开并发。

## 历史 RPC 探针与恢复

`GET /api/meta/history-readiness?stock=000300.SH&start_time=20260820145700&end_time=20260820145900&timeout_seconds=8`
只接受一个合法标的、同日且分钟对齐的最多 3 根 1m 窗口，RPC 超时不超过
8 秒。样本应由部署方先验证。调用共享 Big QMT ZMQ 客户端并显式跳过
FormulaServer 和 bridge 缓存，使用原始价格、`fill_data=false`、
`subscribe=false`，不下载、不写库、不调用交易接口。响应使用
`schema_version=history_readiness_v1`，分别给出 `rpc_status`、`data_status`、
`failure_kind`、`source=bigqmt_zmq_rpc`、`cache_used=false`、行数、耗时、
`last_success_at` 和连续失败次数。缺行或非法 OHLC 为数据降级，合法零量
bar 只标记质量提示；固定样本成功不等于当前行情新鲜或全部历史完整。

RPC 客户端以一个固定 I/O owner 线程独占 DEALER，使用同一个 monotonic
deadline 覆盖锁等待、排队、发送及接收。过期未发送请求直接丢弃；已超时
socket 以 `linger=0` 关闭后重建，隔离迟到响应。任何方法（包括 order/cancel）
均不自动重发。`stop()` 回收自身线程/socket，但不终止共享 ZMQ Context。

`GET /api/meta/recovery-status` 不调用 RPC，返回
`schema_version=recovery_status_v1`、`active_write_requests` 和 `restart_safe`。
计数覆盖 trading/credit/fund/smt 四个账户域的 POST 请求；未知计数 fail closed。native `initialized=false` 不等于
计数器无效，可靠的零在途计数仍允许受控 bridge-only 恢复。它不是券商未完成委托清单，也不
冻结后来请求。两个新接口沿用 `QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA` 认证策略。
MeCoStock 的现有 Windows keepalive 负责受限 bridge-only 重启，bridge
本身不新增守护进程、不修改账户/委托开关、不重启 QMT 终端。
分钟尾窗的 8 秒预算同时覆盖共享锁排队和 RPC；取得锁后仅向 RPC 传入剩余秒数，
避免 10 秒 HTTP 调用方超时后，尚未执行的旧请求仍进入 QMT。完整边界见
[自动化审计与恢复](AUTOMATION_AUDIT.md)。

## 认证机制

### Big QMT 历史补数边界

完整 QMT 终端使用官方全局、单股 `download_history_data`，不是 MiniQMT 的
`download_history_data2`。新适配器从 QMT 全局环境注入实际 callable，HTTP 下载任务
逐股有界调用；未加载新模型、函数缺失或原生下载策略禁用时仍明确返回 unsupported，
不以读取缓存冒充下载。能力由运行中原生 ping 确认，不能只凭客户端方法存在判断可用。
`download_invocation_completed` 只表示请求内的函数调用已返回；
`history_visibility_verified` 只表示每股读到了非空数据，`coverage_verified` 保持 false，
直到另有完整窗口/交易日历验收。查询历史任务的接口不依赖当前下载能力。
`get_market_data_ex` 仍可读取并缓存已有行情；`fill_data=False` 会传入
ContextInfo，并过滤旧嵌入适配器返回的 `suspendFlag=1` 且成交量、成交额均为零
的填充行。更新终端内的适配器文件后，需重新加载模型才激活其参数透传修复；
重启 HTTP bridge 只激活外层保护，不代表终端缓存已补齐。

### 分钟短窗 RPC 隔离

`/api/market/minute_tail` 的 `get_market_data_ex_scoped` 使用运行时拥有的独立
RPC 客户端和单请求锁，不再等待普通行情/账户查询客户端的队列。客户端按需创建，
运行时关闭时一并释放；不创建交易 facade，不启用 FormulaServer 或二级历史缓存。
分钟请求仍共享其自身的 8 秒总预算、最多 20 股受控订阅和原生不可用门禁；
这不是扩大订阅额度或允许并行分钟订阅。加载此修复需要重启 HTTP bridge，
不需要重启 QMT 终端。独立通道不能制造原生端缺失的历史数据。

该隔离设计不构成性能或真实 QMT 验证结论；应由部署方在运行时以 readiness 和
自己的业务窗口验证。

### API Key 认证

QMT Bridge 支持可选的 API Key 认证：

- **交易端点** (`/api/trading/*`, `/api/credit/*`, `/api/fund/*`, `/api/smt/*`) — 设置了 `API_KEY` 时强制认证
- **数据端点** — 默认无需认证，可通过 `QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA=true` 开启
- **认证方式** — HTTP Header `X-API-Key: your-secret-key`
- **WebSocket 交易** — 查询参数 `?api_key=your-secret-key`

!!! warning "安全提示"
    本项目设计为**仅在可信局域网内使用**。请勿将服务直接暴露到公网。如确有需要，请通过 VPN 或防火墙规则保护访问。
