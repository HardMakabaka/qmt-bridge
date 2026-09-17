# QMT 通道自动化审计与运行合同

日期：2026-09-11。范围是 **Big QMT -> 本机 bridge -> MeCoStock 分钟采集** 的启动、检测、受控恢复和补数，不是自动交易。官方依据见 [官方能力合同](AUTOMATION_OFFICIAL_CONTRACTS.md)。

## 扫描范围与证据边界

起始清单覆盖 `src / tests / scripts / dashboard / docs / policy / .github / third_party` 共 191 个文件，其中 153 个 Python 文件、约 3.04 万行；所有 Python 文件完成静态解析。另检查根目录依赖、构建、版本和文档配置。重点逐调用链核对：

- 原生模型入口、RPC 派发、ZMQ 生命周期、缓存、行情/下载/订阅、分钟尾窗；
- FastAPI 生命周期、全部交易写域、客户端与 OpenAPI 操作对齐、只读/写入边界；
- Windows 启动脚本、看门狗、进程身份、重启预算及状态持久化；
- MeCoStock 采集批次、事务提交、连续游标、覆盖/缺口及盘后验证；
- 项目测试、安装器完整性校验、Python 3.6 内嵌语法与发行构建。

这是全项目静态扫描加关键路径深入复核，不代表每个金融字段、所有可选 dashboard/通知功能都做了实机验收。源目录存在本轮之前的大量本地补丁；保留这些补丁，区分源码、隔离验证与当前已加载的 QMT 模型。

## 已复现并修复的自动恢复断点

| 断点 | 修复合同 | 验证 |
|---|---|---|
| recovery state 429 字节全零后永久卡死 | 主备校验与恢复；主备均损坏时建立一次性保守窗口，随后可自动恢复，不每轮重置等待时间 | 实际 PS5 假进程 E2E 与 PS5/7 文件测试 |
| 未初始化却被禁止重启 | `initialized=false` 是需要恢复的状态；安全条件只看可靠的 HTTP 写计数、版本化 guard、身份与限频 | 启动失败后原生端稍后就绪的 E2E |
| 先杀进程、后保存重启次数 | 停止前先将预算预留写入并验证主备；保存失败则不停止 | stop stub 检查主备均已包含事件 |
| 只保护主交易的 6 条 POST | 覆盖 trading/credit/fund/bank/smt 五个域的 POST，包含当前 17 条真实交易写路由 | 实际 ASGI 在途/异常清零测试 |
| CLI 按命令行字样杀其他 qmt-server | CLI 改为非破坏性端口检查；只有 host controller 可按严格身份和 guard 替换 bridge | 跨端口误杀红测、占用端口拒绝测试 |
| 分钟 HTTP 调用超时后仍可能排队进入 RPC | 分钟热路径 8 秒总预算覆盖共享锁等待与 RPC；取得锁后仅传递剩余预算；过期不进入 native | 锁竞争、剩余预算、路由参数与无调用测试 |
| 新增运维接口缺客户端方法 | 补 history-readiness、recovery-status、minute_tail；保留状态和血缘，不只抽取 data | 全 OpenAPI/client surface 对齐测试 |
| collector 在 commit 前累计 written | 只有 commit 成功后才累加写入计数；失败回滚不冒充有效写入 | 无数据库的实际 batch 函数红绿回归 |
| 本地 vendored 补丁与安装器校验清单不一致 | 保留 upstream ancestry pin，校验值明确描述含本地补丁的运行树；不关闭校验 | 隔离临时 QMT 根安装器测试 |

## 唯一恢复控制面

Windows `stockFilter/deploy/windows_qmt_bridge_keepalive.ps1` 是恢复 owner。bridge 不另起一套与之竞争的 runtime 换代线程，不自动重连或重放交易。

1. HTTP/runtime 初始化与原生历史 RPC 分层检测。默认 HTTP 30 秒、原生样本 60 秒。
2. 连续 3 次传输失败或初始化健康失败才考虑恢复；数据缺口不触发重启。
3. guard 必须有 `schema_version=recovery_status_v1`、`counter_status=ok`、整数 `active_write_requests=0`、布尔 `restart_safe=true`。
4. 检查 PID、创建时间、命令、可执行文件、监听端口；锁保护、60 秒冷却、10 分钟最多 3 次。
5. 主备状态以同目录临时文件、`Flush(true)`、原子替换保存；单调序号用于选择最新副本。磁盘无法写入时明确阻止停止进程，不假称已保存。
6. 两份状态均损坏且没有可信内存副本时，先恢复合法状态并等待一个重启窗口，以保护丢失的历史预算。该期限落盘，不随每次循环后移。
7. 重启后同一原生样本通过，才能称行情探针恢复；HTTP-only 模式不得更新历史行情成功时间。

在途计数是 **HTTP 请求**，不是券商未完成委托库存，也不冻结随后到达的请求。未知 guard、正在写入或进程身份变化仍然必须推迟恢复。

## 原生样本必须有持久数据依据

原先 `000300.SH / 2026-08-20` 的参考在终端重新启动后返回空，现场也未发现该代码的本地分钟 DAT。不能靠换一次 bridge 重启“补出”数据。

本次另核验了 `SZ/60/000001.DAT`：严格完整日解析得到 5,302 行，最后完整日为 2026-09-09；其 14:58—15:00 三根同时通过了 **原生 ZMQ、subscribe=false、fill_data=false、cache_used=false** 验证。参考窗口应以这种持久来源建立，而不是依赖某次临时加载到内存的数据。

参考正常只说明这个小窗口可读，不说明全 A 分钟线完整、实时新鲜或可成交。

## 采集验收不能只看 written

本次第一阶段 bridge 恢复后，11:25—11:30 的五轮日志分别记录 written=2,600 / 3,800 / 2,800 / 1,400 / 3,800；这是当时采集程序的 UPSERT 计数，不是本次人工 SQL，也不保证全部为新增行。扫描进一步发现旧计数在 commit 前累加；源码已修正，是否已加载该修正由最终部署报告单独说明。

这些轮次仍为 `partial`，存在超时，`covered=0/5200`；因此**不能称全 A 采集已健康或历史已追齐**。最终采集状态必须同时看实际返回数据、提交成功、逐股连续游标、covered/gap、目标分钟与延迟。在旧计数实现中，仅有 `written>0` 甚至不能证明事务已提交。

## A 股分钟成交量来源与单位合同

同一 `1m` 响应可以混合本地 DAT 与原生 scoped RPC。捕获的 2026-09-11 09:30 行显示：`600000.SH` 原生值 `volume=3778`、`close=9.35`、`amount=3532430`，与 `3778 × 100 × 9.35` 对齐；`000001.SZ` DAT 值 `volume=283600`、`close=11.82`、`amount=3352152`，与 `283600 × 11.82` 对齐。DAT 解码器本身已经将其底层 lot 字段乘以 100。

因此 bridge 在 `history_ex` 和 `minute_tail` 的 A 股 `1m` metadata 中逐股票返回准确 `source_by_stock` 和 `volume_unit_by_stock`：`qmt_local_dat.1m=shares`，`qmt_rpc_fallback.1m=lots`。端点的公开数值保持原样，兼容既有调用方；只有 MeCoStock 全 A 分钟归一化把**声明为** `lots` 的值转为 canonical `shares`，且一次转换后保留 canonical/raw unit/conversion lineage。来源混合而缺逐股票单位的 QMT 行在全 A 分钟边界 fail closed；不使用 amount/close 启发式推断，不对 ETF、日线或通用历史接口做全局乘 100。QMT/AmazingData 回退合并会保留这些字段，避免第二次归一化重复乘数。

该合同修复分钟数据正确性边界，不等于全天覆盖验收。HTTP bridge、collector、QMT worker 已 scoped 部署；2026-09-11 16:51 readiness 为 `ready/bigqmt`，但 `/api/download` 与 `/api/download/jobs` 仍为 `unsupported(native_history_download_unavailable)`。native adapter/function injection 已安装，但内嵌模型尚未 reload。16:47:43 collector 周期为 `requested=120`、`empty_symbols=120`、`persistence_failed_symbols=0`、`covered=0/5200`；全 5,200 当天 coverage 尚未验收。

## 官方下载接口接入

Big QMT 官方有内嵌 **全局、单股** `download_history_data(stockcode,period,startTime,endTime)`。本轮定位到旧实现未正确注入这个函数、默认走 MiniQMT 批量方法，这是本项目接入缺口，不是厂商完全没有能力。相关源码与入口传递已修复并安装；上述 16:51 实测仍未完成内嵌模型 reload，不能把安装状态当作运行中下载能力。

接入修复必须：从 QMT globals/builtins 显式绑定 callable；缺失时报 unsupported；逐股有界执行；不以一次读取缓存冒充下载；不把函数返回 None 等同于数据覆盖已验证。源码与隔离测试完成后仍需要重新加载内嵌模型及真实小窗下载/读取验证，未经这一步不得称已部署。

## 其余审计发现与限制

以下发现保留为明确风险，不在当前已部署恢复修复中冒充解决：

- 原生 pending 队列满时，部分派发异常只写日志，客户端表现为超时；待做结构化背压与原生端过期请求治理。原生模型有独立安装/加载边界。
- `subscribe_warmup` 批量请求只处理第一个代码；当前分钟 collector 明确关闭该旧 warmup，不能把它用于新自动化流程。
- `whole_quote` WebSocket 的认证/代码数/连接资源约束与 realtime 不一致，不能无限新增轮询消费者与分钟采集争抢 RPC。
- `minute_tail` 的现有 `ok` 只证明结束分钟覆盖，不证明中间没有孔洞；全量完成仍以 collector 连续游标和严格日验收为准。
- 原生短订阅中存在同步读取与等待；官方 QMT 策略线程不能被无限阻塞。5200 标的的可持续吞吐与订阅容量必须实测，不能仅凭单标的探针承诺全市场每分钟无缺口。
- 旧手工 `scripts/stop.bat` 只依据 PID 文件停止进程，不属于受控恢复入口；自动化不调用它。
- 未找到官方承诺的无人登录、验证码处理或外部模型热重载接口。需要登录或重新加载模型时明确提示操作员，不模拟点击绕过，也不自动改变交易开关。

## 本轮证据位置

外部审计目录：`D:/AIWORK/data/stockFilter/audits/20260911-1055-qmt-full-automation`。

- `full-project-inventory.json`：初始逐文件静态清单。
- `red-feedback.log`：状态损坏与初始化门禁红测。
- `host-recovery-unit-final.log`、`e2e/E2E_REPORT.md`：Windows 27 项专项及 3 个真实假进程场景。
- `qmt-full-tests-final.log/xml`：第一阶段项目全量 306 项测试；后续原生下载/预算增量以最终汇总为准。
- `state-automatically-repaired.json`、`live-deployment.json`、`live-native-probe.json`：首轮实机恢复和旧样本空数据，失败状态保留，不覆盖成成功。
- `durable-reference-native-probe.json`：文件支撑的新原生参考验证。
- `final-validation-*.log`：终版 333 项测试、2 项子测试、静态检查及发行构建；原生 35 文件 Python 3.6 语法、37 项 vendored 校验通过。
- `final-live-deployment.json`、`final-runtime-summary.json`、`final-real-minute-tail.json`：最终 bridge-only 加载、运行中能力与当天实际分钟数据；未加载内嵌下载模型时，两个下载写入口均不得宣传可用。

最终上线/实采验收由同目录最终报告记录；上述源码完成、测试完成、bridge 已加载、原生模型已加载、全 A 数据已追齐是五种不同状态。
