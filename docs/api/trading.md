# 交易

下单、撤单、批量委托、持仓/资产/成交查询等交易功能。

!!! note "需要认证"
    交易方法需要在创建客户端时传入 `api_key` 参数。

```python
client = QMTClient(host="192.168.1.100", api_key="your-secret-key")
```

## 写入回执与重连对账

- 每个下单请求必须携带稳定且唯一的 `client_submit_id`。同一逻辑委托重试时复用该值，新委托必须生成新值。
- 服务在写入前按 `client_submit_id` 查询已有委托或成交。已存在时直接返回原券商单号，不再次下单；参数不一致时返回 `CLIENT_SUBMIT_ID_CONFLICT`。
- `status="submitted"` 且带 `broker_order_id` 表示已通过回执或查询确认；`submit_unknown` 表示可能已经写入，调用方只能按原 `client_submit_id` 查询或重试，不能换新提交号。
- 撤单以 `order_sysid` 或 `order_id` 作为稳定身份。重试会先检查终态；已撤或已成交时不会再次调用券商撤单。
- `cancel_unknown` 表示撤单结果暂时无法确认。调用方应查询订单状态，或用同一订单身份重试，不能据此假定撤单失败。

::: qmt_bridge.client.trading.TradingMixin
    options:
      show_root_heading: false
      heading_level: 2
