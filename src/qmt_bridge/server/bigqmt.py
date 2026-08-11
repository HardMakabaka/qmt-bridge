from __future__ import annotations

import importlib
import threading
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import urlparse

from .config import Settings

BIGQMT_UPSTREAM_SHA = "40f7275b15843bd167b7ad424a51d3d547be88df"
BIGQMT_UPSTREAM_VERSION = "0.2.0"
_QMT_LOG_SCAN_LIMIT = 64 * 1024 * 1024
_QMT_LOG_FILE_LIMIT = 8


class BigQmtRuntimeUnavailable(RuntimeError):
    pass


def _require_loopback_endpoint(endpoint: str) -> None:
    parsed = urlparse(endpoint)
    if parsed.scheme != "tcp" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("QMT_BRIDGE_ZMQ_ENDPOINT must use tcp loopback")
    if not parsed.port:
        raise ValueError("QMT_BRIDGE_ZMQ_ENDPOINT must include a port")


def _read_log_tail(path: Path, byte_limit: int) -> bytes:
    with path.open("rb") as log_file:
        size = log_file.seek(0, 2)
        read_size = min(size, byte_limit)
        log_file.seek(size - read_size)
        content = log_file.read(read_size)
    if size > read_size:
        _, _, content = content.partition(b"\n")
    return content


def _attest_terminal_trade_mode(qmt_root: str, request_id: str) -> str:
    if not qmt_root or not request_id:
        return "unknown"
    log_root = Path(qmt_root) / "userdata" / "log"
    try:
        log_paths = sorted(log_root.glob("XtClient_2*.log"), reverse=True)
    except OSError:
        return "unknown"
    remaining_bytes = _QMT_LOG_SCAN_LIMIT
    for log_path in log_paths[:_QMT_LOG_FILE_LIMIT]:
        if remaining_bytes <= 0:
            break
        try:
            content = _read_log_tail(log_path, remaining_bytes)
        except OSError:
            return "unknown"
        remaining_bytes -= len(content)
        for line in reversed(content.decode("utf-8", errors="replace").splitlines()):
            if request_id.lower() not in line.lower():
                continue
            compact = line.replace(" ", "").lower()
            if "changetradetype" in compact:
                if "btrade:true" in compact:
                    return "trading"
                if "btrade:false" in compact:
                    return "simulation"
            if "ctradestrategydata::dorun" in compact:
                if "m_btrade:1" in compact:
                    return "trading"
                if "m_btrade:0" in compact:
                    return "simulation"
    return "unknown"


class BigQmtRuntime:
    def __init__(self, settings: Settings, compat_module: ModuleType | Any | None = None):
        self.settings = settings
        self._compat_module = compat_module
        self.client = None
        self.xtdata = None
        self.ping_payload: dict[str, Any] = {}
        self.last_error = ""

    @property
    def compat_module(self):
        if self._compat_module is None:
            try:
                self._compat_module = importlib.import_module(
                    "bigqmt_signal_trader.xtquant_compat"
                )
            except Exception as exc:
                raise BigQmtRuntimeUnavailable(
                    "xtquant-big-convert is not installed"
                ) from exc
        return self._compat_module

    def _client_config(self) -> dict[str, Any]:
        return {
            "transport": "zmq",
            "zmq": {
                "connect_address": self.settings.zmq_endpoint,
                "redis_discovery_enabled": False,
            },
            "formula_server": {
                "enabled": self.settings.formula_enabled,
                "host": self.settings.formula_host,
                "port": self.settings.formula_port,
            },
            "full_tick_cache_enabled": False,
            "local_cache_enabled": False,
        }

    def connect(self) -> dict[str, Any]:
        if self.settings.runtime != "bigqmt":
            raise ValueError("QMT_BRIDGE_RUNTIME must be bigqmt")
        if self.settings.rpc_transport != "zmq":
            raise ValueError("QMT_BRIDGE_RPC_TRANSPORT must be zmq")
        _require_loopback_endpoint(self.settings.zmq_endpoint)
        if self.settings.formula_host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("QMT_BRIDGE_FORMULA_HOST must be loopback")
        account_id = self.settings.trading_account_id.strip()
        if not account_id:
            raise ValueError("QMT_BRIDGE_TRADING_ACCOUNT_ID is required")

        compat = self.compat_module
        config = self._client_config()
        self.client = compat.BigQmtRpcClient(
            account_id=account_id,
            redis_config=config,
            timeout_seconds=self.settings.rpc_timeout_seconds,
        )
        self.xtdata = compat.BigQmtXtData(self.client)
        return self.probe()

    def probe(self) -> dict[str, Any]:
        if self.client is None:
            raise BigQmtRuntimeUnavailable("Big QMT RPC client is not initialized")
        try:
            payload = self.client.call("ping")
            if not isinstance(payload, dict) or payload.get("pong") is not True:
                raise BigQmtRuntimeUnavailable("Big QMT ping did not return pong=true")
            expected = self.settings.trading_account_id.strip()
            actual = str(payload.get("account_id") or "").strip()
            if expected and actual and actual != expected:
                raise BigQmtRuntimeUnavailable(
                    "Big QMT ping account does not match configured account"
                )
            self.ping_payload = dict(payload)
            request_id = str(self.ping_payload.get("qmt_request_id") or "").strip()
            trade_mode = _attest_terminal_trade_mode(
                self.settings.qmt_root,
                request_id,
            )
            self.ping_payload["qmt_trade_mode"] = trade_mode
            self.ping_payload["terminal_real_mode"] = trade_mode == "trading"
            self.ping_payload["terminal_mode_source"] = (
                "qmt_terminal_log" if trade_mode != "unknown" else "unavailable"
            )
            self.last_error = ""
            return self.ping_payload
        except Exception as exc:
            self.last_error = f"{exc.__class__.__name__}: {exc}"
            raise

    def new_account(self, account_id: str = ""):
        target = str(account_id or self.settings.trading_account_id).strip()
        return self.compat_module.StockAccount(target)

    def new_trader(self):
        if self.client is None:
            raise BigQmtRuntimeUnavailable("Big QMT RPC client is not initialized")
        trader = self.compat_module.BigQmtXtTrader(
            account_id=self.settings.trading_account_id,
            redis_config=self._client_config(),
            timeout_seconds=self.settings.rpc_timeout_seconds,
        )
        trader.client = self.client
        return trader

    def readiness(self) -> dict[str, Any]:
        ping = dict(self.ping_payload)
        account_id = str(ping.get("account_id") or "")
        return {
            "ready": bool(self.client is not None and ping.get("pong") is True),
            "runtime": "bigqmt",
            "transport": "zmq",
            "zmq_endpoint": self.settings.zmq_endpoint,
            "formula_endpoint": f"{self.settings.formula_host}:{self.settings.formula_port}",
            "formula_enabled": self.settings.formula_enabled,
            "qmt_root": self.settings.qmt_root,
            "account_id": account_id,
            "rpc_revision": ping.get("rpc_revision"),
            "qmt_trade_mode": str(ping.get("qmt_trade_mode") or "unknown"),
            "terminal_real_mode": ping.get("terminal_real_mode") is True,
            "terminal_mode_source": ping.get("terminal_mode_source"),
            "upstream_order_writes_enabled": bool(
                ping.get("allow_order_methods", False)
            ),
            "upstream_sha": BIGQMT_UPSTREAM_SHA,
            "upstream_version": BIGQMT_UPSTREAM_VERSION,
            "last_error": self.last_error or None,
        }

    def close(self) -> None:
        client = self.client
        transport = getattr(client, "_transport_instance", None)
        if transport is not None:
            stop = getattr(transport, "stop", None)
            close = getattr(transport, "close", None)
            if callable(stop):
                stop()
            elif callable(close):
                close()
        self.client = None
        self.xtdata = None
        self.ping_payload = {}


_runtime: BigQmtRuntime | None = None
_runtime_lock = threading.Lock()


def initialize_bigqmt_runtime(
    settings: Settings,
    compat_module: ModuleType | Any | None = None,
) -> BigQmtRuntime:
    global _runtime
    runtime = BigQmtRuntime(settings, compat_module=compat_module)
    runtime.connect()
    with _runtime_lock:
        previous = _runtime
        _runtime = runtime
    if previous is not None and previous is not runtime:
        previous.close()
    return runtime


def get_bigqmt_runtime() -> BigQmtRuntime:
    runtime = _runtime
    if runtime is None:
        raise BigQmtRuntimeUnavailable("Big QMT runtime is not initialized")
    return runtime


def reset_bigqmt_runtime() -> None:
    global _runtime
    with _runtime_lock:
        runtime = _runtime
        _runtime = None
    if runtime is not None:
        runtime.close()


class _XtDataProxy:
    def __getattr__(self, name: str):
        runtime = get_bigqmt_runtime()
        if runtime.xtdata is None:
            raise BigQmtRuntimeUnavailable("Big QMT data facade is not initialized")
        return getattr(runtime.xtdata, name)


xtdata = _XtDataProxy()
