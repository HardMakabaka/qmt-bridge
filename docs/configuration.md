# 配置参考

QMT Bridge 支持通过 `.env` 文件、环境变量或 CLI 参数进行配置。优先级：**CLI 参数 > 环境变量 > .env 文件 > 默认值**。

## 配置项

| 环境变量 | CLI 参数 | 默认值 | 说明 |
|---------|---------|-------|------|
| `QMT_BRIDGE_HOST` | `--host` | `0.0.0.0` | 监听地址（`0.0.0.0` = 允许局域网访问） |
| `QMT_BRIDGE_PORT` | `--port` | `13543` | 监听端口 |
| `QMT_BRIDGE_SINGLETON` | `--no-singleton` | `true` | 启动前停止旧 qmt-server 进程 |
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

# 账户只读查询与委托写入分别控制
# QMT_BRIDGE_ACCOUNT_ENABLED=true
# QMT_BRIDGE_TRADING_ACCOUNT_ID=12345678
# QMT_BRIDGE_ORDER_WRITES_ENABLED=true
```

RPC 请求/响应使用 `15560`；委托和成交回报使用独立的 ZMQ PUB/SUB `15561`。
事件带 `{epoch, sequence}` 游标，内嵌运行时默认保留最近 2000 条用于断线回放；
两条通道都必须保持在本机回环地址，不能复用同一端口。

## 认证机制

QMT Bridge 支持可选的 API Key 认证：

- **交易端点** (`/api/trading/*`, `/api/credit/*`, `/api/fund/*`, `/api/bank/*`, `/api/smt/*`) — 设置了 `API_KEY` 时强制认证
- **数据端点** — 默认无需认证，可通过 `QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA=true` 开启
- **认证方式** — HTTP Header `X-API-Key: your-secret-key`
- **WebSocket 交易** — 查询参数 `?api_key=your-secret-key`

!!! warning "安全提示"
    本项目设计为**仅在可信局域网内使用**。请勿将服务直接暴露到公网。如确有需要，请通过 VPN 或防火墙规则保护访问。
