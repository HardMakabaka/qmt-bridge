# QMT Bridge

> 通过 Big QMT 内嵌策略与本机 ZMQ RPC，将行情、账户和受控委托能力暴露为 HTTP/WebSocket；委托写能力默认开启。

**QMT Bridge** 由两个进程组成：Big QMT 终端内的 Python 模型运行固定版本的 ZMQ RPC，外部 Python 3.10+ FastAPI 服务将其转换为稳定的 HTTP/WebSocket 合同。外部服务不安装或导入原生 `xtquant`；委托端点始终要求 API Key、已连接资金账户、双层写能力，以及当前模型确实运行在“实盘”模式的终端日志证明。3.0 的移除项和升级方式见 [MIGRATION_3.md](docs/MIGRATION_3.md)。

```
Mac / Linux (主力机)                    Windows (中转站)
┌──────────────────────┐                ┌─────────────────────────┐
│  你的分析 / 交易代码    │   HTTP/WS     │  Big QMT 客户端 (登录中)   │
│  本地数据库            │ ◄───────────► │  ZMQ 模型 + FastAPI       │
│  可视化仪表盘          │   局域网       │  RPC 15560 / 事件 15561   │
└──────────────────────┘                └─────────────────────────┘
```

## Why

Big QMT 的模型 API 只能在 Windows 终端进程内运行。分析服务直接依赖终端 Python 或原生 `xtquant`，会把部署、版本和进程生命周期绑死在 QMT 上。

QMT Bridge 把终端内能力收敛到只监听回环地址的 ZMQ 边界，再由 Windows 主机上的 API 服务向可信网络提供统一接口。交易账户查询可独立启用；下单和撤单同时受 API 层 `QMT_BRIDGE_ORDER_WRITES_ENABLED`、终端层 `rpc_allow_order_methods` 和 QMT 当前 request id 对应的 `m_bTrade=1` 日志证明约束，任一不满足都拒绝写入。

## Features

- **186 个 HTTP 操作** — 启用账户路由时覆盖行情、板块、财务、账户与受控交易；Python 客户端逐项覆盖
- **4 个 WebSocket 端点** — 实时行情、共享快照、公式和交易回报；公式推送尚未验证，显式返回 `unsupported`
- **账户只读查询** — 资产、持仓、当日委托和成交；不需要开启委托写入
- **受控委托边界** — 下单和撤单默认具备写能力，仍需 API Key、资金账户、两层写门禁和 QMT 实盘模式证明
- **零依赖客户端** — Python 客户端基于 stdlib，无需安装 xtquant 即可在任意平台使用
- **API Key 认证** — 可选的 API Key 保护，交易端点强制认证

## Prerequisites

### Windows 端 (服务端)

- **Python** 3.10+
- **Big QMT 客户端** — 已安装并获得模型运行权限，账户查询时需登录交易账户
- **Big QMT 内嵌 Python** — 安装器部署固定提交的 ZMQ 模型运行时
- 外部 Python 环境不安装原生 `xtquant`

### 网络

- Windows 和你的主力机在同一局域网下（连同一个路由器 / WiFi）
- Windows 防火墙放行本项目使用的端口（默认 13543）

## Quick Start

### 1. 安装

```bash
git clone https://github.com/qmt-bridge/qmt-bridge.git
cd qmt-bridge

# 安装服务端（含 WebSocket 支持）
pip install -e ".[full]"

# 或者只安装服务端（不含 WebSocket）
pip install -e ".[server]"
```

如果只需要在远程机器上使用客户端：

```bash
# 零依赖安装（仅 HTTP）
pip install -e .

# 含 WebSocket 订阅支持
pip install -e ".[client]"
```

在 QMT 关闭时，将固定提交 `40f7275b15843bd167b7ad424a51d3d547be88df`
的内嵌运行时安装到终端：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install-bigqmt-runtime.ps1 `
  -QmtRoot "C:\国金证券QMT交易端" `
  -AccountId "12345678" `
  -EventZmqEndpoint "tcp://127.0.0.1:15561"
```

首次安装若输出 `compiled_model_ready=false`，在 QMT 中新建名为
`MECOSTOCK_BIGQMT_ZMQ` 的 Python 策略，粘贴
`src/MECOSTOCK_BIGQMT_ZMQ.py`，取消勾选“启动本地python”并编译一次。关闭
QMT 后重新运行安装器，确认 `compiled_model_ready=true` 和
`embedded_python_mode=true`。终端启动自动运行由 QMT 自己的策略运行设置
管理，不是安装器中的 `simpleRun` 字段；不要勾选“启动本地python”。

### 2. 配置

```bash
cp .env.example .env
# 按需编辑 .env
```

### 3. 启动 QMT 客户端

打开 Big QMT 并登录交易账户。需要真实委托能力时，把 `MECOSTOCK_BIGQMT_ZMQ` 的运行模式设为“实盘”并确认状态为“运行中”；只读或模拟验收可保留“模拟”。API 会用模型返回的当前 request id 匹配 `userdata/log/XtClient_*.log` 中的 `m_bTrade`，无法证明“实盘”时 fail closed。

### 4. 启动 API 服务

```bash
# 使用 CLI 命令（推荐）
qmt-server

# 自定义参数
qmt-server --port 13543 --log-level debug

# Big QMT ZMQ + 账户查询；写能力默认开启，实际放行仍需终端实盘证明
qmt-server --qmt-root "C:\国金证券QMT交易端" \
  --zmq-endpoint tcp://127.0.0.1:15560 \
  --event-zmq-endpoint tcp://127.0.0.1:15561 \
  --account-enabled --account-id 12345678 \
  --api-key your-secret-key
```

Windows 推荐使用受管理脚本。它只管理本仓库虚拟环境启动的 bridge 进程，校验 PID、
启动时间、可执行文件和命令行；不会启动 QMT、登录账户或重新加载策略模型：

```powershell
.\scripts\manage-bridge.ps1 start
.\scripts\manage-bridge.ps1 status
.\scripts\manage-bridge.ps1 stop

# 前台调试
.\scripts\manage-bridge.ps1 start -Foreground -- --log-level debug
```

### 5. 验证

在你的 Mac/Linux 浏览器中访问：

```
http://<Windows局域网IP>:13543/docs
```

Swagger 只证明 HTTP 进程存活。运行验收必须检查 readiness：

```bash
curl http://<Windows局域网IP>:13543/api/meta/readiness
curl http://<Windows局域网IP>:13543/api/meta/capabilities
```

### 能力与失败合同

`GET /api/meta/capabilities` 是接口适配状态的唯一运行时清单。每条记录包含
`path`、`method`、`mode`（`native` / `derived` / `unsupported`）、
`status`、`reason_code`、`write_effect` 和线程归属。当前代码基线明确标记
44 个 HTTP 操作与 2 个 WebSocket 操作为未适配；不要通过空列表猜测支持状态。

可选 Big QMT 能力统一返回以下失败信封，客户端不会再把它解包成空数据：

```json
{
  "status": "unsupported",
  "data": null,
  "reason_code": "xttrader_credit_order_missing",
  "message": "xttrader_credit_order_missing",
  "capability": "credit_order",
  "provider": "bigqmt",
  "retryable": false,
  "details": {}
}
```

`unavailable` 表示运行时/终端暂时不可用且 `retryable=true`；`unsupported`
表示该精确 Big QMT 合同不存在或未验证，不允许回退到语义不同的接口。

## Configuration

通过 `.env` 文件或环境变量配置，CLI 参数优先级最高。

| 环境变量 | CLI 参数 | 默认值 | 说明 |
|---------|---------|-------|------|
| `QMT_BRIDGE_HOST` | `--host` | `0.0.0.0` | 监听地址（`0.0.0.0` = 允许局域网访问） |
| `QMT_BRIDGE_PORT` | `--port` | `13543` | 监听端口 |
| `QMT_BRIDGE_LOG_LEVEL` | `--log-level` | `info` | 日志级别：critical / error / warning / info / debug |
| `QMT_BRIDGE_WORKERS` | `--workers` | `1` | Worker 数量（Windows 下建议保持 1） |
| `QMT_BRIDGE_BINARY_CACHE_ENABLED` | — | `true` | 是否启用历史/基础读取的本地二进制缓存 |
| `QMT_BRIDGE_BINARY_CACHE_TTL_SECONDS` | — | `86400` | 缓存 TTL 秒数 |
| `QMT_BRIDGE_BINARY_CACHE_MAX_BYTES` | — | `2147483648` | 缓存最大容量 |
| `QMT_BRIDGE_API_KEY` | `--api-key` | _(空)_ | API Key，用于保护交易端点 |
| `QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA` | — | `false` | 数据端点是否也要求认证 |
| `QMT_BRIDGE_RUNTIME` | — | `bigqmt` | 固定 Big QMT 运行时 |
| `QMT_BRIDGE_QMT_ROOT` | `--qmt-root` | _(空)_ | Big QMT 终端根目录；委托写入用它读取终端日志并证明当前模型为实盘模式 |
| `QMT_BRIDGE_RPC_TRANSPORT` | — | `zmq` | 固定 ZMQ RPC |
| `QMT_BRIDGE_ZMQ_ENDPOINT` | `--zmq-endpoint` | `tcp://127.0.0.1:15560` | 仅允许回环地址 |
| `QMT_BRIDGE_EVENT_ZMQ_ENDPOINT` | `--event-zmq-endpoint` | `tcp://127.0.0.1:15561` | 独立委托/成交事件 PUB；仅允许回环地址且不得与 RPC 端口相同 |
| `QMT_BRIDGE_ACCOUNT_ENABLED` | `--account-enabled` | `false` | 启用账户只读查询 |
| `QMT_BRIDGE_TRADING_ACCOUNT_ID` | `--account-id` | _(空)_ | 交易账户 ID |
| `QMT_BRIDGE_ORDER_WRITES_ENABLED` | `--order-writes-enabled` | `true` | API 层委托写入门禁；可显式设为 `false` 冻结写入，终端层门禁仍需同时开启 |

### 全链路诊断

HTTP/SDK、RPC、QMT 原生调用、数据缓存、交易回报、下载与生命周期已接入本地结构化埋点。
配置、按 trace/job/order ID 检索和状态解释见 [可观测性说明](docs/OBSERVABILITY.md)，
实施范围与验收记录见 [实施计划](docs/OBSERVABILITY_PLAN.md)。埋点不替代柜台成交证据或业务对账。

## API Reference

完整 API 文档请访问运行中的服务 `/docs`（Swagger UI）或 `/redoc`（ReDoc）。以下为端点概览。

### Legacy Endpoints（向后兼容）

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/history` | 单只股票历史 K 线 |
| GET | `/api/batch_history` | 批量获取多只股票历史数据 |
| GET | `/api/full_tick` | 最新 tick 快照 |
| GET | `/api/sector_stocks` | 板块成分股列表 |
| GET | `/api/instrument_detail` | 股票基本信息 |
| POST | `/api/download` | 触发历史数据下载 |

### Market — 行情数据 `/api/market/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/market/snapshot` | 实时行情快照（个股 / 指数） |
| GET | `/api/market/indices` | 主要指数行情概览 |
| GET | `/api/market/history_ex` | 增强版 K 线（除权、填充） |
| GET | `/api/market/local_data` | 仅读本地缓存（离线可用） |
| GET | `/api/market/divid_factors` | 除权因子 |
| GET | `/api/market/market_data` | 通用行情数据查询 |

### Tick & L2 — 逐笔数据 `/api/tick/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tick/l2_quote` | L2 行情快照 |
| GET | `/api/tick/l2_order` | L2 逐笔委托 |
| GET | `/api/tick/l2_transaction` | L2 逐笔成交 |

### Sector — 板块数据 `/api/sector/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/sector/list` | 所有板块列表 |
| GET | `/api/sector/stocks` | 板块成分股（支持历史日期） |
| GET | `/api/sector/info` | 板块元数据 |
| POST | `/api/sector/create` | 创建自定义板块 |
| POST | `/api/sector/add_stocks` | 添加成分股 |
| DELETE | `/api/sector/remove` | 删除板块 |

### Calendar — 交易日历 `/api/calendar/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/calendar/trading_dates` | 交易日列表 |
| GET | `/api/calendar/holidays` | 节假日列表 |
| GET | `/api/calendar/trading_calendar` | 完整日历 |
| GET | `/api/calendar/trading_period` | 交易时段 |
| GET | `/api/calendar/is_trading_date` | 日期校验 |
| GET | `/api/calendar/prev_trading_date` | 上一个交易日 |
| GET | `/api/calendar/next_trading_date` | 下一个交易日 |

### Financial — 财务数据 `/api/financial/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/financial/data` | 财务报表数据（资产负债表 / 利润表等） |

### Instrument — 合约信息 `/api/instrument/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/instrument/batch_detail` | 批量合约详情 |
| GET | `/api/instrument/type` | 代码类型判断 |
| GET | `/api/instrument/ipo_info` | IPO 信息 |
| GET | `/api/instrument/index_weight` | 指数成分股权重 |
| GET | `/api/instrument/st_history` | ST 历史 |

### Option — 期权数据 `/api/option/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/option/detail` | 期权合约详情 |
| GET | `/api/option/chain` | 标的期权链 |
| GET | `/api/option/list` | 按到期日 / 类型筛选 |
| GET | `/api/option/history_list` | 历史期权列表 |

### ETF & Convertible Bond — `/api/etf/*` & `/api/cb/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/etf/list` | ETF 代码列表 |
| GET | `/api/etf/info` | ETF 申赎清单 |
| GET | `/api/cb/list` | 可转债列表 |
| GET | `/api/cb/detail` | 可转债详情 |
| GET | `/api/cb/conversion_price` | 转股价信息 |
| GET | `/api/cb/bond_info` | 债券信息 |

### Futures — 期货数据 `/api/futures/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/futures/main_contract` | 主力合约 |
| GET | `/api/futures/sec_main_contract` | 次主力合约 |

### HK — 港股通 `/api/hk/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/hk/stock_list` | 港股通标的列表 |
| GET | `/api/hk/connect_stocks` | 按方向筛选（沪港通 / 深港通） |

### Meta — 系统元数据 `/api/meta/*`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/meta/health` | HTTP 进程存活检查 |
| GET | `/api/meta/readiness` | Big QMT ZMQ、账户和固定上游版本运行就绪检查 |
| GET | `/api/meta/history-readiness` | 固定单标的短窗 1m 原生 ZMQ 数据探针，绕过 FormulaServer/bridge 缓存，不订阅、不下载 |
| GET | `/api/meta/recovery-status` | 无 RPC 的在途委托/撤单 HTTP 请求计数，供宿主机恢复前检查 |
| GET | `/api/meta/capabilities` | 全量 HTTP/WS 能力、适配模式、运行状态和原因码 |
| GET | `/api/meta/version` | 服务版本 |
| GET | `/api/meta/connection_status` | Big QMT RPC 连接与恢复状态 |
| GET | `/api/meta/markets` | 可用市场列表 |
| GET | `/api/meta/periods` | K 线周期列表 |
| GET | `/api/meta/stock_list` | 按类别获取证券列表 |
| GET | `/api/meta/last_trade_date` | 最近交易日 |

### Download — 数据下载 `/api/download/*`

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/download/jobs` | 创建历史数据下载任务 |
| GET | `/api/download/jobs/{job_id}` | 查询下载任务进度 |
| POST | `/api/download/jobs/{job_id}/cancel` | 取消下载任务 |
| POST | `/api/download/financial` | 下载财务数据 |
| POST | `/api/download/sector_data` | 下载板块数据；支持 `timeout_seconds`，返回 `ok` / `timeout` / `busy` / `error` |
| POST | `/api/download/index_weight` | 下载指数权重 |
| POST | `/api/download/etf_info` | 下载 ETF 信息 |
| POST | `/api/download/cb_data` | 下载可转债数据 |
| POST | `/api/download/history_contracts` | 下载过期合约 |

### Trading — 交易 `/api/trading/*` (需要 API Key)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/trading/order` | 下单 |
| POST | `/api/trading/cancel` | 撤单 |
| POST | `/api/trading/batch_order` | 批量下单 |
| GET | `/api/trading/orders` | 查询委托 |
| GET | `/api/trading/trades` | 查询成交 |
| GET | `/api/trading/positions` | 查询持仓 |
| GET | `/api/trading/asset` | 查询资产 |
| GET | `/api/trading/order_detail` | 查询单笔委托 |

### Credit — 融资融券 `/api/credit/*` (需要 API Key)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/credit/order` | 信用交易下单（当前精确 Big QMT 方法未适配） |
| GET | `/api/credit/available_amount` | 额度查询（当前未适配） |
| GET | `/api/credit/positions` | 信用持仓 |
| GET | `/api/credit/asset` | 信用资产 |
| GET | `/api/credit/debt` | 信用负债 |

### Fund — 资金查询 (需要 API Key)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/fund/available` | 可用资金（由资产查询派生） |

### WebSocket

| Path | Description |
|------|-------------|
| `/ws/realtime` | 实时行情推送 |
| `/ws/whole_quote` | 全市场行情订阅 |
| `/ws/formula` | 公式推送；当前 Big QMT 合同未验证，返回 `unsupported` |
| `/ws/trade` | 交易回报推送 (需要 API Key) |

WebSocket 连接后发送 JSON 订阅请求：

```jsonc
// /ws/realtime
{ "stocks": ["000001.SZ", "600519.SH"], "period": "tick" }

// /ws/whole_quote
{ "codes": ["SH", "SZ"] }

```

## Python Client

项目附带零依赖 Python 客户端，可在任意平台使用（无需安装 xtquant）。

### 基本用法

```python
from qmt_bridge import QMTClient

client = QMTClient(host="192.168.1.100", port=13543)

# 历史 K 线
df = client.get_history("000001.SZ", period="1d", count=60)

# 增强版 K 线，前复权
dfs = client.get_history_ex(["000001.SZ", "600519.SH"], dividend_type="front", count=60)

# 大盘行情一览
indices = client.get_major_indices()

# 实时快照
snapshot = client.get_market_snapshot(["000001.SZ", "600519.SH"])

# 板块
sectors = client.get_sector_list()
stocks = client.get_sector_stocks("沪深A股")

# 财务数据
fin = client.get_financial_data(["000001.SZ"], tables=["Balance"])

# ETF / 期权 / 期货
etfs = client.get_etf_list()
options = client.get_option_list("000300.SH", "20250321")
main_contract = client.get_main_contract("IF.CFE")

# 元数据
markets = client.get_markets()
periods = client.get_periods()
last_date = client.get_last_trade_date("SH")
```

### 交易 (需要 API Key)

```python
client = QMTClient(host="192.168.1.100", api_key="your-secret-key")

# 下单
order_id = client.place_order(
    stock_code="000001.SZ",
    order_type=23,        # 买入
    order_volume=100,
    client_submit_id="manual-20260719-0001",
    price_type=5,         # 最新价
)

# 查询
orders = client.query_orders(client_submit_id="manual-20260719-0001")
positions = client.query_positions()
asset = client.query_asset()

# 撤单
client.cancel_order(order_id)
```

### WebSocket 实时订阅

```python
import asyncio

def on_tick(data):
    print(data)

# 实时行情
asyncio.run(client.subscribe_realtime(
    stocks=["000001.SZ", "600519.SH"],
    callback=on_tick,
))

# 全市场行情
asyncio.run(client.subscribe_whole_quote(
    codes=["SH", "SZ"],
    callback=on_tick,
))
```

## Examples

```bash
# 运行就绪检查
curl http://192.168.1.100:13543/api/meta/readiness

# 平安银行最近 60 根日线
curl "http://192.168.1.100:13543/api/history?stock=000001.SZ&period=1d&count=60"

# 增强版 K 线，前复权
curl "http://192.168.1.100:13543/api/market/history_ex?stocks=000001.SZ&period=1d&count=5&dividend_type=front"

# 大盘行情
curl http://192.168.1.100:13543/api/market/indices

# 个股 / 指数快照
curl "http://192.168.1.100:13543/api/market/snapshot?stocks=000001.SH,000001.SZ"

# 板块列表
curl http://192.168.1.100:13543/api/sector/list

# 沪深 A 股成分股
curl "http://192.168.1.100:13543/api/sector/stocks?sector=沪深A股"

# ETF 代码列表
curl http://192.168.1.100:13543/api/etf/list

# 交易日列表
curl "http://192.168.1.100:13543/api/calendar/trading_dates?market=SH"

# 指数成分股权重
curl "http://192.168.1.100:13543/api/instrument/index_weight?index_code=000300.SH"

# 财务数据
curl "http://192.168.1.100:13543/api/financial/data?stocks=000001.SZ&tables=Balance"

# 创建历史下载任务
curl -X POST http://192.168.1.100:13543/api/download/jobs \
  -H "Content-Type: application/json" \
  -d '{"stocks": ["000001.SZ", "600519.SH"], "period": "1d", "batch_size": 10, "max_attempts": 2}'

# 查询历史下载任务
curl http://192.168.1.100:13543/api/download/jobs/<job_id>

# 下单（需要 API Key）
curl -X POST http://192.168.1.100:13543/api/trading/order \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-key" \
  -d '{"stock_code": "000001.SZ", "order_type": 23, "order_volume": 100}'
```

## Project Structure

```
qmt-bridge/
├── pyproject.toml                  # 项目元数据与依赖
├── .env.example                    # 配置模板
├── scripts/                        # 启动 / 停止脚本
│   └── manage-bridge.ps1           # Windows 受管理的启动、状态和停止
├── src/qmt_bridge/
│   ├── _version.py                 # 版本号
│   ├── server/                     # FastAPI 服务端
│   │   ├── app.py                  # 应用工厂 & 生命周期管理
│   │   ├── cli.py                  # qmt-server CLI 入口
│   │   ├── config.py               # 配置加载
│   │   ├── security.py             # API Key 认证
│   │   ├── helpers.py              # 数据转换工具
│   │   ├── models.py               # Pydantic 请求 / 响应模型
│   │   ├── deps.py                 # 依赖注入
│   │   ├── routers/                # REST API 路由 (21 个模块)
│   │   ├── ws/                     # WebSocket 端点
│   │   └── trading/                # 交易模块
│   │       ├── manager.py          # XtTraderManager 生命周期
│   │       └── callbacks.py        # 交易回调
│   └── client/                     # Python 客户端 (22 个 Mixin 模块)
│       ├── __init__.py             # QMTClient 聚合类
│       ├── base.py                 # HTTP 传输层 (stdlib)
│       ├── websocket.py            # WebSocket 订阅
│       └── [feature].py            # 各功能域客户端方法
└── tests/                          # 测试
```

## Authentication

QMT Bridge 支持可选的 API Key 认证机制：

- **交易端点** (`/api/trading/*`, `/api/credit/*`, `/api/fund/*`, `/api/smt/*`) — 设置了 `API_KEY` 时强制认证
- **数据端点** — 默认无需认证，可通过 `QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA=true` 开启
- **认证方式** — HTTP Header `X-API-Key: your-secret-key`

## Security Notice

本项目设计为**仅在可信局域网内使用**。请勿将服务直接暴露到公网。如确有需要，请通过 VPN 或防火墙规则保护访问。

## FAQ

**Q: QMT 客户端必须一直开着吗？**

是的。ZMQ 模型运行在 Big QMT 进程内；终端关闭、未登录或模型未运行时 `/api/meta/readiness` 会失败，API 不会把 HTTP 存活误报为通道可用。

**Q: 支持自动下单吗？**

默认具备受控写入能力。账户查询与委托写入仍然分离；只有 API 层
`QMT_BRIDGE_ORDER_WRITES_ENABLED=true`、终端层
`rpc_allow_order_methods=True`、API Key、资金账户，以及当前 request id 在 QMT 原生日志中对应 `m_bTrade=1` 同时满足时，下单/撤单端点才可能放行。切到“模拟”、缺失日志证明或任一写门禁显式为 false 都会立即冻结新写入。

**Q: 非交易时间能用吗？**

可以。历史 K 线、板块成分股等静态数据在非交易时间也能正常获取。实时 tick 和 WebSocket 推送在非交易时间没有数据。

**Q: 数据延迟大吗？**

未对当前部署做性能承诺。实时行情由 Big QMT 回调转发；延迟和可用性应以运行中的
`/api/meta/readiness`、业务端超时与调用方自己的测量为准。

**Q: 客户端需要安装什么依赖吗？**

基础客户端 (HTTP) 零依赖，仅使用 Python 标准库。如需 WebSocket 订阅功能，安装 `pip install qmt-bridge[client]` 即可。如安装了 pandas，返回结果会自动转为 DataFrame。

## License

[MIT](LICENSE)
