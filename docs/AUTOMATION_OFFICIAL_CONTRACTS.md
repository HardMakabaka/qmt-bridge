# QMT 通道自动化：官方能力边界与实现合同

**研究日期：2026-09-11；范围：Big QMT 终端内嵌模型 -> 本机 ZMQ RPC ->
qmt-bridge -> 分钟线采集。** 本文只核实启动、检测、恢复与行情采集；不授权
自动交易、账户动作或绕过登录。链接均为第一方迅投、libzmq 或 Microsoft 文档。

本仓库当前标注为 `qmt-bridge 2.1.3`，且 `third_party/xtquant_big_convert.UPSTREAM.json`
记录了转换项目版本/提交；这只是本地版本线索，**不是**对迅投接口能力的证明。

## 可改变实现的已核实事实

| # | 官方事实与证据 | 自动化实现合同 |
|---|---|---|
| 1 | Big QMT 内嵌模型的 `ContextInfo.get_market_data_ex` 原型含 `subscribe`；`subscribe=False` 的官方定义是“不做数据订阅，只读取本地已有数据”。该函数也不建议在 `init` 调用，因为那时只能读本地数据。[迅投：内嵌 ContextInfo 行情函数](https://dict.thinktrader.net/innerApi/data_function.html) | 原生历史探针必须在模型已运行后的回调阶段执行，固定 `subscribe=False, fill_data=False`，并把“本地已有的 1m 数据”与“终端/账户已恢复”分开报告。没有行不是许可自动下载或把 HTTP 存活报为行情可用。 |
| 2 | 同一 Big QMT 文档明确 `1m` 是可选周期、时间格式为 `%Y%m%d` / `%Y%m%d%H%M%S`，结果是每代码一个 `DataFrame`；`fill_data` 的语义是“是否填充数据”。[同上](https://dict.thinktrader.net/innerApi/data_function.html) | 分钟采集/探针应验证每个 code 的真实时间戳、严格请求窗口和 OHLC；不得因 `fill_data=True` 的前值填充把缺分钟当成功。零成交量本身不能等同传输故障，需作为单独数据质量分类。 |
| 3 | Big QMT 官方示例说明：`get_market_data_ex(subscribe=False)` 仅从本地行情文件取数、不受订阅数限制、需要预先下载；`subscribe=True` 才能取动态行情且受订阅上限约束，并建议盘后增量补齐。[迅投：内嵌 Python 完整示例](https://dict.thinktrader.net/innerApi/code_examples.html) | 全 A 轮询历史读走 `subscribe=False`，不得把它当盘中实时拉取；盘中增量只能使用受控订阅/回调路径，须有订阅数容量账本与明确释放/重建逻辑。检测用小窗口、单标的、无订阅的历史探针，采集的“实时成功”必须另行证据化。 |
| 4 | 迅投 FAQ：订阅可用于当日数据，早于当日的数据要下载；订阅有数量上限，超限时返回的数据会用前值填充，属于“不正确行情”。该页还指出 `get_market_data_ex(subscribe=True)` 自动订阅的品种没有订阅号，不能手动反订阅，只能停止策略释放订阅数。[迅投：行情常见问题](https://dict.thinktrader.net/innerApi/question_answer.html) | 不允许将每轮分钟采集写成 `get_market_data_ex(..., subscribe=True)` 的无限扩张调用。对于需要可控取消的实时订阅，显式 `subscribe_quote` 并保存订阅号；检出容量耗尽/重复时间戳后判数据失败，不进入“重启 bridge 即可修复”的分支。 |
| 5 | Big QMT 内嵌 Python 官方页**单独公开了全局** `download_history_data(stockcode, period, startTime, endTime)`：支持 `tick/1m/5m/1d` 基础周期；`incrementally` 是部分客户端版本可选的增量参数；示例直接在内嵌模型中调用。官方完整示例还展示了对股票列表逐只循环调用它。 [Big QMT 数据下载](https://dict.thinktrader.net/innerApi/data_function.html) [Big QMT 循环下载示例](https://dict.thinktrader.net/innerApi/code_examples.html) | Big QMT **有已文档化的单标的历史下载能力**，不能把“当前 bridge 尚未正确接通/没有实机验证”误报成厂商不支持。它是运行时全局函数而非 `ContextInfo` 方法：bridge 应显式捕获/注入该全局函数，并把调用限制为单股、基础周期、受预算的主策略线程切片。必须先实机验证当前终端版本和权限，才可把能力从 `implemented` 提升为 `validated/deployed`。 |
| 5a | 迅投的 **XtData 原生 Python** 文档说其本质是与 **MiniQMT** 建连，读取前应确保 MiniQMT 已有数据，不足时用补充接口；其中 `download_history_data2` 是批量版本，`reconnect` 也列在 XtData 的版本更新中。 [XtData 运行逻辑/下载接口](https://dict.thinktrader.net/nativeApi/xtdata.html) | `download_history_data2` 批量接口和 `reconnect` 是 MiniQMT/`xtquant` 文档能力，**不能搬运为 Big QMT 内嵌模型已支持的接口**。Big QMT 自动补数可从已文档化的“单股全局 download”开始；批量、自动重连和全 A 速度/限流均仍须本桥接实机验证，失败必须留下可重试证据而非声称厂商无能力。 |
| 6 | QMT 官方 FAQ 明确：所有策略在同一个线程调用；一个策略阻塞（死循环、sleep、锁等）会阻塞全部策略，若需多线程/多进程应使用极简模式配合 xtquant。[迅投：策略线程限制](https://dict.thinktrader.net/innerApi/question_answer.html) | 内嵌 ZMQ ROUTER、周期检查和单次 `ContextInfo` 调用必须短、非阻塞；不能在模型线程实现 sleep-retry、批量全 A 下载或等待 HTTP。恢复判定放 bridge/watchdog 外层；若模型心跳/探针失败，只可请求受限 bridge 进程重启，不能从模型线程自我重启 QMT。 |
| 7 | libzmq 文档说明单个 0MQ socket 不线程安全；实务上创建后只能在初始化时迁移给一个线程，而不能被多线程并发使用。[libzmq `zmq(7)`](https://libzmq.readthedocs.io/en/zeromq4-x/zmq.html) | 每个 DEALER 只能由一个 I/O owner 线程创建、`poll/send/recv/close`。HTTP 请求通过队列进入 owner；超时不让调用线程碰 socket。重建也由该 owner 串行执行，避免并发 close/send 造成二次故障。 |
| 8 | ØMQ 官方 Guide 的可靠请求-应答模式：超时后应轮询、有限次重试/放弃；对 REQ 的恢复做法是 close/reopen socket。它也指出 DEALER/SUB 接收方不能仅凭静默区分“没有数据”和“对端已死”，需要显式心跳或请求/响应探针。[ØMQ Guide：可靠请求应答](https://zguide.zeromq.org/docs/chapter4/) | 对本桥接 DEALER：采用单一 monotonic deadline 覆盖排队、发送、接收；超时后 `linger=0` 关闭并由 owner 重建，关联 request id 隔离迟到回复。**不自动重发任何 RPC**：原生请求已发出后不能撤销，也不能证明服务端没有执行；读请求可由上层下一周期重新探测，任何 order/cancel 永不自动重放。 |
| 9 | Windows `ReplaceFile` 能以一个函数替换文件并可保留旧文件属性；替换、备份与新文件必须同卷。`MoveFileEx(..., MOVEFILE_REPLACE_EXISTING)` 可以替换现有文件，`MOVEFILE_WRITE_THROUGH` 只保证 copy-and-delete 路径在返回前冲盘。 [ReplaceFile](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-replacefilea) [MoveFileEx](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexa) | recovery-state 不能直接覆盖目标文件。写入同目录唯一临时文件，完成 JSON、`flush`/`fsync` 后使用同卷 replace，并保留上一有效副本。读取时校验 schema、必填字段、类型、大小上限，以及持久化日期/时间戳和请求窗口格式是否有效；进程内的 monotonic 时间不能持久化后比较。主文件损坏时尝试备份，二者都无效则创建**保守默认状态**（0 次失败、无恢复资格、记录 `state_reset_reason`），而不是卡死或把全零当可信状态。 |
| 10 | 本次核对的迅投内嵌与 XtData 文档中，未找到由官方公开承诺的 Big QMT headless 启动、自动重新登录、验证码绕过、自动 reload 内嵌模型，或从外部进程安全附着已登录 Big QMT 的 API。XtData 的 `reconnect` 更新记录属于其 MiniQMT 接口页，不能迁移解释为 Big QMT 终端控制能力。[XtData 版本/接口页](https://dict.thinktrader.net/nativeApi/xtdata.html) | 可以全自动化 **bridge** 检测/受限重启/重新附着尝试与分钟采集恢复。短暂的 `runtime not initialized` 首先进入有限次数、限频的 `recovering`，并用后续原生探针验收；只有终端关闭、登录/验证码明确需要人、模型持续未运行或恢复预算耗尽时，才进入 `operator_required`。不得伪造“QMT 已自动登录/自动启动”成功。 |

## 推荐状态机（非交易）

1. **ready**：短窗原生 `ContextInfo` 1m probe 返回完整真实时间戳；bridge HTTP、ZMQ RPC、数据三层分别记录。
2. **degraded_data**：RPC 通、但行数/时间戳/OHLC 不合约；零量、停牌、订阅容量异常细分为质量原因，不能触发 QMT 重启。
3. **transport_failed**：仅当同一非订阅小窗探针连续 `N` 次发生明确连接/超时/协议失败，才达到 bridge-only restart 候选；固定指数 + ETF 两个标的可降低单证券异常误报。
4. **restart_deferred**：有在途写请求、读不到安全状态、锁被占用、仍在冷却/限频，立即停止恢复；不得杀 QMT 或重发请求。
5. **bridge_restarted**：只替换受身份/父进程/监听端口证实的 bridge；随后等待模型侧重新连接，重复原生探针。
6. **recovering**：短暂 runtime 未初始化或 transport failed 时，先在锁、冷却、在途写保护下受限重启 bridge 并重新附着；不重启 QMT 终端。
7. **operator_required**：终端未登录、验证码/登录明确需要人、模型持续未运行/待 reload、或恢复预算耗尽。自动化应发出证据，不继续循环杀进程。
8. **collector_healthy**：仅当连续采集周期同时出现 `valid_rows > 0`、`written > 0`、覆盖/延迟在门槛内才成立；历史 probe 通过本身不足以宣称该状态。

## 本轮官方查阅覆盖与未证实项

- 已查：Big QMT 内嵌 `ContextInfo` 行情/订阅语义、官方 FAQ 的订阅限制和策略单线程限制；MiniQMT XtData 的本地缓存/下载/订阅语义（明确隔离）；libzmq 的线程与可靠请求-应答边界；Windows 文件替换 API。
- 未证实且不得写进实现承诺：Big QMT 终端命令行 headless launch、自动登录/验证码、外部 reload 内嵌模型、Big QMT 对应的 `download_history_data2` 或 XtData `reconnect` 自动恢复接口、以及“终端重启后所有订阅/模型自动恢复”的保证。**这不否定**官方已文档化的 Big QMT 全局单股 `download_history_data`；未证实的是当前 bridge 接入、当前客户端版本/权限和实机完成性。
- 因此自动化验收必须区分：`bridge process alive`、`ZMQ RPC replied`、`真实 1m 数据合约`、`collector wrote rows`、`QMT terminal/operator state`。任一上游层失败不由下游 HTTP 健康掩盖。

## 本地静态接入点（仅代码阅读，未调用 QMT）

- `src/bigqmt_signal_trader_strategy.py::_resolve_runtime_name()` 已能从 QMT
  运行时 globals/builtins 取函数；`_build_config()` 目前仅把交易类 globals
  和 `_EXTRA_QMT_GLOBAL_FUNCS` 注入 `qmt_api`，该 tuple **未包含**
  `download_history_data`。
- `src/bigqmt_signal_trader_strategy.py::_build_rpc_service()` 和
  `::_pump_download_jobs()` 都创建 `BigQmtMarketDataProvider(context_info)`；
  `src/bigqmt_signal_trader/adapters/market_bigqmt.py::download_history_data()`
  当前先走 `xtquant.xtdata`，再错误地尝试 `ContextInfo.download_history_data`。
  这解释了“官方 Big QMT 有全局函数、当前 bridge 却不可用”的接入缺口。
- 最小正确改动位置是：在策略入口用 `_resolve_runtime_name("download_history_data")`
  捕获全局函数，显式传给 provider；provider 在 Big QMT 分支优先调用该函数
  （保持官方 `stockcode, period, startTime, endTime[, incrementally]` 形状），
  而不是把它绑定成 `ContextInfo` 方法。`download_jobs.py::_download_chunk()`
  对 Big QMT 只能选单股 `download_history_data`，不能默认 `download_history_data2`。
  调用仍必须由现有 `adjust()` 的有界切片驱动，实机先以 1 个代码/1m 小窗验收。
