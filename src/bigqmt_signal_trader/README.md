# bigqmt_signal_trader

Big QMT 内嵌运行时包。它由策略进程加载，接收本机 ZMQ RPC 请求，并只调用
Big QMT 已注入的 `ContextInfo` / 全局交易函数；不是 MiniQMT 或 XtQuant 兼容包。

## 已完成

- `TradeSignal`、`OrderRequest`、`PositionSnapshot`、`AccountSnapshot` 等核心数据模型。
- `SignalSource`、`MarketDataProvider`、`PositionProvider`、`OrderGateway`、`PositionSyncSink`、`StateStore` 等替换接口。
- `SignalTradingApp.tick()` 编排流程：
  1. 读取信号。
  2. 原子 claim。
  3. 读取持仓。
  4. 计算买卖数量。
  5. 生成价格。
  6. 调用可替换 `OrderGateway`。
  7. 写回状态。
  8. 同步持仓快照。
- `bigqmt_signal_trader_strategy.py`：内嵌入口，响应 `init`、`adjust`、`handlebar`、订单和成交回调。
- 运行时注入的 `passorder`、`cancel`、`get_trade_detail_data`，以及单股
  `download_history_data`（若当前 QMT 终端实际提供）。

## 当前安全状态

订单 RPC 默认不允许；即使启用，外层 HTTP 服务仍要求 API Key、账户、写门禁和
终端实盘证明。`passorder` 没有同步成交回执，调用方必须按 `userOrderId` 对账。

## 后续接入顺序

外部程序使用 HTTP `QMTClient`；不要导入已移除的 `xtquant_compat` 或 Xt 风格对象。
完整迁移见仓库根文档 [MIGRATION_3.md](../../docs/MIGRATION_3.md)。

## 测试

```powershell
cd <REPO_ROOT>
python -m unittest discover -s tests\bigqmt_signal_trader
```


