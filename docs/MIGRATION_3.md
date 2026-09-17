# 3.0 迁移：仅 Big QMT

3.0 移除了 MiniQMT/`xtquant` 兼容层。外部服务只经本机回环 ZMQ 调用正在运行的
Big QMT 内嵌模型；它不导入原生 `xtquant`，也不提供模拟的异步交易回调。

## 已移除

| 旧入口或路径 | 处理方式 |
|---|---|
| `bigqmt_signal_trader.xtquant_compat`、`BigQmtXtData`、`BigQmtXtTrader` 等 Xt 对象 | 删除；改用 HTTP `QMTClient` 或 REST API。没有替代 shim。 |
| MiniQMT 连接、`download_history_data2`、xtdata 自动重连 | 删除；Big QMT 仅使用运行时注入的单股 `download_history_data(stock, period, start_time, end_time)`。 |
| fake async 下单/撤单接口 | 删除；使用 `/api/trading/order`、`/api/trading/cancel`，并按稳定 `client_submit_id` / 委托身份查询对账。 |
| `/api/bank/*` 与 Python `BankMixin` | 删除；不要把银行划转请求发送到 bridge。 |
| `/api/tick/l2_thousand_*`、`/ws/l2_thousand`、`subscribe_l2_thousand` | 删除；没有以普通 L2 或轮询伪装的替代。 |
| `/api/meta/xtdata_version`、`get_xtdata_version()` | 改为 `/api/meta/runtime_version`、`get_runtime_version()`；返回大 QMT 适配器上游版本，不再假称 MiniQMT SDK 版本。 |

以下仅有旧兼容占位的接口也已移除，对应 `QMTClient` 方法不再导出：

- `/api/credit/order`、`/api/credit/available_amount`。
- `/api/fund/transfer`、`transfer_records`、`ctp_transfer_in`、`ctp_transfer_out`、`ctp_balance`、`ctp_option_to_future`、`ctp_future_to_option`。
- `/api/smt/order`、`negotiate_order_async`、`cancel`、`quoter`、`compact`。
- `/api/trading/order_async`、`cancel_async`、`com_fund`、`com_position`、`export_data`、`query_data`。
- `/api/sector/create_folder`、`remove_stocks`、`reset`；`/api/futures/sec_main_contract`。
- `/api/formula/generate_index`、`/api/financial/field`。
- `/api/market/fullspeed_orderbook`、`transactioncount`；`/api/download/ipo_data`、`option_data`。

内部 RPC 也不再接受 `query_stock_*`、`order_stock*`、`cancel_order_stock*` 兼容别名；
原生客户端使用 `get_asset/get_positions/query_orders/query_trades/submit_order/submit_orders_batch/cancel_order`。

`QMTClient` 仍保留真正的旧 HTTP 路由（例如 `/api/history`、`/api/full_tick`）的客户端方法；
这些是兼容 HTTP 路由，不是 XtQuant 对象或 MiniQMT fallback。

## 升级步骤

1. 更新外部调用：删除 Xt 对象导入和 L2 千档/银证转账调用，改用 `QMTClient`。
2. 在 Big QMT 中重新部署并加载本仓库生成的内嵌运行时；确认 `/api/meta/readiness` 和
   `/api/meta/capabilities` 的运行时结果，而不是只看 Python 包版本。
3. 历史下载改为创建 `/api/download/jobs` 任务。`completed` 只表示 bridge 的任务完成；
   数据可见性和覆盖范围须由调用方按自己的窗口验收。
4. HTTP 客户端默认超时为 30 秒；WebSocket 客户端在握手中使用 `X-API-Key` 请求头。
5. 所有写操作要求已配置 API Key；普通数据读取遵守 `QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA`。
   实时原生订阅消耗 QMT 额度，始终要求 API Key。
6. 下载进度默认保存在 `%LOCALAPPDATA%/qmt-bridge/download-jobs`，可通过
   `QMT_BRIDGE_DOWNLOAD_STATE_DIR` 改变；目录只有一个进程所有者。
   原生调用结果未知时保持待对账，不自动重发。盘中默认延后重型下载；确有需求时才显式设置
   `QMT_BRIDGE_ALLOW_MARKET_HOURS_DOWNLOAD=true`。

源码 provenance 的 `hash_normalization=lf` 表示校验文本源文件时统一 CRLF/LF；
这避免 Windows checkout 与 Linux 构建结果不一致，不改变上游 ancestry SHA。

## 回滚边界

代码仓库与部署目录不同。修改或回退本仓库 **不会** 自动切换
`D:\AIWORK\qmt-bridge` 中已经部署的运行时，也不会重启 HTTP bridge 或重新加载 QMT 模型。
若需回滚，先停止受管理的 bridge，再部署已验证版本并重新加载模型；本迁移不执行任何
终端切换、下载或交易操作。
