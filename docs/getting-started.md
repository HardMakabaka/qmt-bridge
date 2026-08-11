# 快速开始

## 前提条件

### Windows 端（服务端）

- **Python** 3.10+
- **Big QMT 客户端** — 已安装并获得模型运行权限；账户查询时需登录交易账户
- **Big QMT 内嵌 Python** — 由安装器部署固定版本的 ZMQ 运行时
- 外部 Python 环境不安装原生 `xtquant`

### 网络

- Windows 和你的主力机在同一局域网下（连同一个路由器 / WiFi）
- Windows 防火墙放行本项目使用的端口（默认 13543）

## 1. 安装

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

QMT 关闭时安装终端内运行时：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\install-bigqmt-runtime.ps1 `
  -QmtRoot "C:\国金证券QMT交易端" `
  -AccountId "12345678"
```

首次输出 `compiled_model_ready=false` 时，在 QMT 中创建
`MECOSTOCK_BIGQMT_ZMQ` Python 策略，粘贴 `src/MECOSTOCK_BIGQMT_ZMQ.py`，
取消勾选“启动本地python”并编译。关闭 QMT 后重新运行安装器，确认
`compiled_model_ready=true` 和 `embedded_python_mode=true`。终端启动自动
运行由 QMT 自己的策略运行设置管理，不要把 `simpleRun` 当作自动运行开关。

## 2. 配置

```bash
cp .env.example .env
# 按需编辑 .env
```

详细配置项请参考 [配置参考](configuration.md)。

## 3. 启动 QMT 客户端

打开 Big QMT 并登录交易账户。需要真实委托能力时，把 `MECOSTOCK_BIGQMT_ZMQ` 的运行模式设为“实盘”并确认状态为“运行中”。API 会将模型返回的当前 request id 与 `userdata/log/XtClient_*.log` 匹配；日志不能证明 `m_bTrade=1` 时写请求会被拒绝。

## 4. 启动 API 服务

```bash
# 使用 CLI 命令（推荐）
qmt-server

# 自定义参数
qmt-server --port 8080 --log-level debug

# Big QMT ZMQ + 账户查询；写能力默认开启，真实委托还需终端实盘证明
qmt-server --qmt-root "C:\国金证券QMT交易端" \
    --zmq-endpoint tcp://127.0.0.1:15560 \
    --account-enabled --account-id 12345678 \
    --api-key your-secret-key
```

也可以使用脚本：

```bash
# 前台运行（Ctrl+C 停止）
bash scripts/start.sh

# 后台运行
bash scripts/start-nohup.sh
bash scripts/stop.sh

# Windows
scripts\start.bat
scripts\stop.bat
```

## 5. 验证

在你的 Mac/Linux 浏览器中访问：

```
http://<Windows局域网IP>:13543/docs
```

Swagger 只证明 HTTP 进程存活。运行验收必须检查 readiness：

```bash
curl http://<Windows局域网IP>:13543/api/meta/readiness
```

## Python 客户端用法

```python
from qmt_bridge import QMTClient

client = QMTClient(host="192.168.1.100", port=13543)

# 历史 K 线
df = client.get_history("000001.SZ", period="1d", count=60)

# 增强版 K 线，前复权
dfs = client.get_history_ex(
    ["000001.SZ", "600519.SH"],
    dividend_type="front",
    count=60,
)

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

### 交易（需要 API Key）

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
