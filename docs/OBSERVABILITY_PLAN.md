# Bridge 全链路埋点实施计划

状态：已完成源码实施和隔离验证（2026-09-17）。未部署到真实 QMT 实例；现有业务改动全部保留。

## 1. 目标与边界

让请求、订单、下载任务、订阅和事件都能通过关联 ID 还原执行路径、分段耗时、状态变化及证据边界。覆盖已盘点的 HTTP、WebSocket、RPC、原生调用、缓存/DAT/Formula 分支、后台任务、生命周期和可选适配器。

- 先完成本计划，再持续实施、集成和验证；不是仅交付计划。
- 不改变接口业务结果、交易门禁、幂等/对账、重试或取消语义。
- 不触发真实下单、撤单、下载、Redis/MySQL 或其他外部服务写入；验证使用隔离临时目录、模拟原生 API 和本地测试传输。
- 不添加数据库、远程观测平台、自动下单重试或客户端重连。
- 埋点不等于业务账本。关键事件不抽样，但有界队列/磁盘故障仍须明确暴露审计缺口；本轮不新增“日志故障阻断交易”门禁。
- 嵌入 QMT 的代码保持 Python 3.6 语法及标准库兼容；客户端安装仍不增加依赖。

## 2. 统一契约

共享实现放在 `src/bigqmt_signal_trader/telemetry.py`，Bridge、SDK 与安装到 QMT 的运行资产复用。

- 事件基础字段：schema_version、timestamp_utc、process_instance_id、service_role、event_name、trace_id、span_id、parent_span_id、outcome。
- 按需附加：http_request_id、rpc_request_id、runtime_generation、client_submit_id、order_sys_id、trade_id、batch_id/item_index、job_id/attempt、signal_id、subscription_id、ws_session_id、event_epoch/event_sequence。
- trace 与 RPC request_id 分离；跨线程显式传递、跨进程通过 RPC envelope 的可选 trace 字段传递。后台工作和回报通过业务 ID/trace link 关联，不延长已结束 HTTP span。
- 同进程耗时用 monotonic；UTC 用于关联，不用跨进程时间差伪造精确网络耗时。
- outcome 区分 success、empty、partial、unsupported、rejected、overloaded、timeout、unknown、gap、canceled。原生调用开始/返回、柜台委托观察、成交观察、消息发送和客户端消费是不同事件。
- 统一 API：`configure(...)`、`emit(event_name, **fields)`、`span(event_name, **fields)`（yield 可追加结果字段的 dict）、`bind_context(**fields)`、`current_context()`、`inject_trace(payload)`、`extract_trace(payload)`、`traced(event_name, **fields)`、`flush(timeout)`、`shutdown(timeout)`、`stats()`。
- 关键事件使用 `critical=True`，不抽样；高频行情保存生命周期、错误和周期汇总，不全量落盘行情 payload。
- 单进程单写入者、有界队列、JSONL 轮转；队列丢弃/写盘失败可查询并在恢复时记录缺口。不记录密钥、完整敏感账户信息、大响应或任意对象 repr。

## 3. 实施分工与覆盖

| 工作包 | 主要文件/责任 | 必须覆盖 |
| --- | --- | --- |
| A 观测基础 | telemetry.py、基础测试 | 上下文、同步/异步 span、JSONL、轮转、脱敏、有界队列、失败可见、关闭与隔离 |
| B RPC 与 QMT | rpc_client.py、transports/*、redis_rpc.py 公共处理、formula_server.py、request_budget.py（仅需要时） | client queue/wire/server queue/deadline/handler/response、Formula fallback、可选传输 |
| C 数据路径 | data_client.py、market_bigqmt.py、market router、DAT/分钟/缓存、其余数据 routers | 路径选择、覆盖/新鲜度/结果摘要、原生分支、订阅创建与 finally 释放、显式公式/板块写入 |
| D 交易和信号 | trading routers/manager、trading_client.py、order gateway、redis_rpc.py 交易 handler、SignalTradingApp/状态与信号 adapters | 门禁、身份预查、实际调用、回执/未知/严格对账、逐项批量、撤单、外部同步、durable intent/ACK |
| E 推送与通知 | exec_events.py、server/ws/*、callbacks.py、notify/*、RPC quote listener | callback/publish/receive/queue/send、replay/gap、hub 引用与关闭、通知过滤/发送/丢弃 |
| F 下载与调度 | download router/state、download_jobs.py、full_tick_cache.py、position sync、strategy.py | 任务状态/恢复/取消/在途/验证、同步下载、adjust/cadence、可选缓存与同步 no-op 原因 |
| G 入口、运维、SDK | app.py、helpers.py、security/deps、runtime_controller.py、meta/cli、client/*、安装/进程脚本、文档 | HTTP/WS 接入、拒绝和响应、线程上下文、恢复/关闭、SDK 网络/解析/callback、安装/启动/停止 |

实现时各工作包有明确文件所有权；共享文件按函数分区并协调，不覆盖其他修改。公共节点覆盖全部路由，特有分支只补业务事件，不给 149 个 handler 机械复制日志。

## 4. 执行顺序

1. [x] 冻结源码链路清单和执行边界，写本计划。
2. [x] 实现 A，并发布稳定埋点 API；其他工作包按契约并行。
3. [x] 完成 B/C/D/E/F/G，补与分支对应的聚焦测试。
4. [x] 集成验证 trace 连通性、关键失败语义和副作用不变；修复有证据的遗漏。
5. [x] 完成配置、字段/事件、检索示例、限制、验证结果和覆盖表文档。

## 5. 验收标准

- HTTP 成功、拒绝、异常均可关联；至少一个 HTTP→RPC→模拟 QMT→响应场景证明跨层 trace 和不同 RPC ID。
- 队列满、执行前过期明确没有原生调用；执行后超时保留 unknown，不引起重放。
- 单笔/批量下单、撤单、外部同步和信号恢复记录各自真实阶段，埋点不改变结果或调用次数。
- DAT/缓存/Formula fallback 和分钟临时订阅可解释数据来源、覆盖与释放结果。
- WS/notify 慢消费、gap、callback 异常、关闭和 runtime 更换可观察；不宣称 send 等于消费 ACK。
- 下载恢复、市场时段暂停、单项重试、取消在途和数据可见性验证可观察。
- writer 轮转、脱敏、队列压力、磁盘写失败、flush/shutdown 测试；埋点自身异常不能改变业务结果。
- 执行相关 focused tests，随后完整 pytest、Ruff、嵌入 Python 3.6 检查、必要的打包资产检查。性能使用隔离微基准记录实际开销，不预设无证据指标。
- 不做真实券商交易或部署切换；最终明确代码验证与真实 QMT 运行验证的边界。

## 6. 收尾记录

使用、事件覆盖与检索说明见 [OBSERVABILITY.md](OBSERVABILITY.md)，README 和 `.env.example` 已添加入口。

### 实际实现

- 共享标准库 telemetry：同步/异步 context/span、跨线程与 RPC/持久事件 envelope 传递、JSONL 单 writer/轮转/脱敏、损失统计、退出 drain。
- HTTP/SDK：全局 ASGI 入口覆盖原有 149 个 HTTP 操作及 4 个 WS 路由；受限关联头、拒绝/异常/发送终态、最多 4 KiB 业务状态检查，不缓存大行情响应。
- 数据与传输：DAT/缓存/Formula/RPC 分支、覆盖摘要、分钟独立通道与订阅 finally 释放；真实 queue/wire/handler 阶段与剩余预算。
- 交易与事件：下单/撤单/批量/外部同步的确认边界，signal durable intent/恢复；PUB 与回放保存可选 trace，listener→callback→WS/notify 继续关联；旧事件建立独立 trace 并保留 cursor。
- 下载与运维：任务持久关联、worker/native/subprocess 上下文、取消/在途/恢复、QMT 调度摘要、runtime generation/重连、安装和受控启停。
- 高频 quote 不逐 tick 落盘；共享采样组不继承已断开的首个 WS 会话；通知关闭丢弃与 sender 失败可观察。

### 验证记录

| 验证 | 结果 |
| --- | --- |
| 完整 pytest（全新临时 basetemp） | **402 passed，2 subtests passed，11.92 s** |
| Ruff（src/tests） | 通过 |
| 嵌入 Python 3.6 grammar | **43 个文件通过**；不是实际 Python 3.6/QMT 运行证明 |
| git diff --check | 通过；保留原有换行风格和无关改动 |
| wheel + sdist | 隔离构建成功；两份 wheel telemetry 资产与源码字节一致，sdist 源码一致；HTTP middleware/SDK trace headers 均包含 |
| HTTP→RPC→独立模拟原生线程 | 同一 trace、两个不同 RPC request ID；没有启动真实 lifespan/QMT |
| 拒绝与故障 | 401/404/405/422/500、provider 失败、SDK callback 异常、批量未知/false 撤单、signal 恢复等反例保持返回与调用次数 |
| 最终隔离启停 | 随机回环端口 58688；无效 runtime、空账户、写入禁用；health 返回，随后 `stopped gracefully`；PID/state/shutdown 文件无残留 |

隔离启停的 `process.started/identity_verified/stop_requested/stopped/server_start/server_exit` 共享 trace
`a634340aa2c74380ac86795f92b913af`；health 请求另建请求级 trace。没有接入真实 QMT。

本机隔离微基准（仅观测开销，不代表真实券商链路性能）：

| 操作 | 样本数 | 中位数 | P95 |
| --- | ---: | ---: | ---: |
| disabled emit | 2000 | 0.4 µs | 0.4 µs |
| enabled emit 入队 | 2000 | 6.5 µs | 9.0 µs |
| span 起止一对 | 1000 | 40.1 µs | 61.3 µs |

该次共写入 4001 行、1,307,037 bytes，队列丢弃和写盘失败均为 0；写盘异步执行，表中不是 fsync 耗时。

### 边界

- 未触发真实下单、撤单、下载、Redis/MySQL 写入或生产部署；真实 QMT 端到端运行仍待部署后只读验证。
- 日志是观测证据，不是交易幂等状态或柜台账本；`flush` 只表示队列排空，不保证断电持久化。
- 单文件默认 5 MiB、3 个备份、有界队列 4096；这是单进程实例的轮转策略，不是整个目录或跨重启保留期上限。
- 初次非隔离构建因本地缺少 hatchling 未通过，改用标准隔离构建后成功；没有为此改变项目依赖。
