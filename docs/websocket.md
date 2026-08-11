# WebSocket 使用指南

QMT Bridge 提供 WebSocket 端点用于实时数据推送。历史下载进度通过
`GET /api/download/jobs/{job_id}` 查询，不再提供下载进度 WebSocket。

## 端点列表

| 路径 | 说明 | 认证 |
|------|------|------|
| `/ws/realtime` | 实时行情推送 | 无 |
| `/ws/whole_quote` | 全市场行情订阅 | 无 |
| `/ws/l2_thousand` | 当前 Big QMT 推送合同未验证，连接后返回 `unsupported` | 无 |
| `/ws/formula` | 当前 Big QMT 推送合同未验证，连接后返回 `unsupported` | 无 |
| `/ws/trade` | 交易回报推送 | 需要 API Key |

`/ws/formula` 和 `/ws/l2_thousand` 保留兼容路径，但不会伪造订阅成功。服务端发送
统一失败信封后关闭连接；以 `/api/meta/capabilities` 的运行时结果为准。

## 实时行情 `/ws/realtime`

连接后发送 JSON 订阅请求，服务端持续推送行情更新。

```jsonc
// 订阅请求
{ "stocks": ["000001.SZ", "600519.SH"], "period": "tick" }
```

**Python 客户端用法：**

```python
import asyncio
from qmt_bridge import QMTClient

client = QMTClient(host="192.168.1.100")

def on_tick(data):
    print(f"收到行情: {data}")

asyncio.run(client.subscribe_realtime(
    stocks=["000001.SZ", "600519.SH"],
    callback=on_tick,
))
```

## 全市场行情 `/ws/whole_quote`

订阅整个市场的行情更新。

```jsonc
// 订阅请求
{ "codes": ["SH", "SZ"] }
```

**Python 客户端用法：**

```python
asyncio.run(client.subscribe_whole_quote(
    codes=["SH", "SZ"],
    callback=on_tick,
))
```

## L2 千档行情 `/ws/l2_thousand`

当前 Big QMT 内嵌运行时没有经过验证的千档推送合同。以下请求会收到
`status=unsupported`、`reason_code=bigqmt_l2_push_not_verified`，不会启动轮询或
回退到普通 L2 数据。

```jsonc
// 订阅请求
{ "stocks": ["000001.SZ"] }
```

**Python 客户端用法：**

```python
asyncio.run(client.subscribe_l2_thousand(
    stocks=["000001.SZ"],
    callback=on_tick,
))
```

## 公式/指标 `/ws/formula`

当前 Big QMT 内嵌运行时没有经过验证的公式推送合同。以下请求会收到
`status=unsupported`、`reason_code=bigqmt_formula_push_not_verified`。

```jsonc
// 订阅
{
    "action": "subscribe",
    "formula_name": "MA",
    "stock_code": "000001.SZ",
    "period": "1d",
    "count": -1,
    "dividend_type": "none",
    "params": {}
}

// 取消订阅
{ "action": "unsubscribe", "seq_id": 123 }
```

## 交易回报 `/ws/trade`

!!! note "需要认证"
    交易回报 WebSocket 需要通过查询参数传递 API Key：`ws://<host>:13543/ws/trade?api_key=your-secret-key`

推送交易事件（委托回报、成交回报、错误信息等）。

Big QMT 模型先通过独立回环 ZMQ `tcp://127.0.0.1:15561` 发布委托/成交事件，
服务端再转发到本 WebSocket。每条事件包含 `{epoch, sequence}` 游标；客户端
重连时通过 RPC 回放保留窗口并对实时重复事件去重。默认保留最近 2000 条，
游标跨进程 epoch 或早于保留窗口时 readiness 会报告 replay gap。

**Python 客户端用法：**

```python
client = QMTClient(host="192.168.1.100", api_key="your-secret-key")

def on_trade_event(data):
    print(f"交易事件: {data}")

asyncio.run(client.subscribe_trade_events(callback=on_trade_event))
```

## JavaScript / 浏览器使用

```javascript
const ws = new WebSocket("ws://192.168.1.100:13543/ws/realtime");

ws.onopen = () => {
    ws.send(JSON.stringify({
        stocks: ["000001.SZ", "600519.SH"],
        period: "tick"
    }));
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log("行情更新:", data);
};
```
