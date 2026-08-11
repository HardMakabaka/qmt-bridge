# QMT Bridge

> 通过 Big QMT 内嵌策略与本机 ZMQ RPC，将行情、账户和受控委托能力暴露为 HTTP/WebSocket；委托写能力默认开启。

**QMT Bridge** 由 Big QMT 终端内的 ZMQ 模型和外部 FastAPI 服务组成。外部 Python 不安装原生 `xtquant`，行情与账户查询通过固定版本 RPC 提供；委托写门禁默认开启，但只有当前模型 request id 的 QMT 原生日志证明 `m_bTrade=1` 时才会放行写入。

```
Mac / Linux (主力机)                    Windows (中转站)
┌──────────────────────┐                ┌─────────────────────────┐
│  你的分析 / 交易代码    │   HTTP/WS     │  Big QMT 客户端 (登录中)   │
│  本地数据库            │ ◄───────────► │  ZMQ 模型 + FastAPI       │
│  可视化仪表盘          │   局域网       │  127.0.0.1:15560         │
└──────────────────────┘                └─────────────────────────┘
```

## 核心特性

- **100+ REST API 端点** — 历史 K 线、实时行情、L2 逐笔、板块管理、财务数据、指数权重、期权链、可转债、ETF、港股通、期货主力合约等
- **5 个 WebSocket 端点** — 实时行情推送、全市场行情、L2 千档、下载进度、交易回报
- **程序化交易** (可选) — 下单、撤单、批量委托、融资融券、银证转账、智能交易
- **零依赖客户端** — Python 客户端基于 stdlib，无需安装 xtquant 即可在任意平台使用
- **API Key 认证** — 可选的 API Key 保护，交易端点强制认证

## 快速导航

| 文档 | 说明 |
|------|------|
| [快速开始](getting-started.md) | 安装、配置、启动服务 |
| [配置参考](configuration.md) | 所有配置项详解 |
| [REST API 速查](rest-api.md) | 全部 HTTP 端点列表 |
| [WebSocket](websocket.md) | WebSocket 端点使用指南 |
| [Python 客户端 API](api/index.md) | `QMTClient` 完整 API 参考 |

## 安装

```bash
git clone https://github.com/qmt-bridge/qmt-bridge.git
cd qmt-bridge

# 安装服务端（含 WebSocket 支持）
pip install -e ".[full]"

# 或者只安装客户端（零依赖）
pip install -e .

# 含 WebSocket 订阅支持
pip install -e ".[client]"
```

## 许可

[MIT](https://github.com/qmt-bridge/qmt-bridge/blob/main/LICENSE)
