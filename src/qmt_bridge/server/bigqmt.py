from __future__ import annotations

import threading
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from bigqmt_signal_trader.data_client import BigQmtDataClient
from bigqmt_signal_trader.rpc_client import BigQmtRpcClient
from bigqmt_signal_trader.trading_client import BigQmtTradingClient

from .config import Settings

BIGQMT_UPSTREAM_SHA = "40f7275b15843bd167b7ad424a51d3d547be88df"
BIGQMT_UPSTREAM_VERSION = "0.2.0"
_QMT_LOG_SCAN_LIMIT = 64 * 1024 * 1024
_QMT_LOG_FILE_LIMIT = 8


class BigQmtRuntimeUnavailable(RuntimeError):
    pass


class BigQmtConfigurationError(ValueError):
    """A static bridge configuration error; retrying cannot repair it."""


def _require_loopback_endpoint(endpoint: str) -> None:
    parsed = urlparse(endpoint)
    if parsed.scheme != "tcp" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise BigQmtConfigurationError("QMT_BRIDGE_ZMQ_ENDPOINT must use tcp loopback")
    if not parsed.port:
        raise BigQmtConfigurationError("QMT_BRIDGE_ZMQ_ENDPOINT must include a port")


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
    def __init__(
        self,
        settings: Settings,
        *,
        rpc_client_factory=BigQmtRpcClient,
        data_client_factory=BigQmtDataClient,
        trading_client_factory=BigQmtTradingClient,
    ):
        self.settings = settings
        self._rpc_client_factory = rpc_client_factory
        self._data_client_factory = data_client_factory
        self._trading_client_factory = trading_client_factory
        self.client = None
        self.market_data = None
        self._minute_client = None
        self._minute_data_client = None
        self._minute_client_lock = threading.Lock()
        self._transport_failure_callback = None
        self.ping_payload: dict[str, Any] = {}
        self.last_error = ""

    def _client_config(self) -> dict[str, Any]:
        config = {
            "transport": "zmq",
            "zmq": {
                "connect_address": self.settings.zmq_endpoint,
                "redis_discovery_enabled": False,
            },
            "exec_events": {
                "transport": "zmq",
                "zmq": {
                    "connect_address": self.settings.event_zmq_endpoint,
                },
            },
            "formula_server": {
                "enabled": self.settings.formula_enabled,
                "host": self.settings.formula_host,
                "port": self.settings.formula_port,
                "history_read_workers": self.settings.formula_history_read_workers,
            },
            "full_tick_cache_enabled": False,
            "local_cache_enabled": False,
        }
        if self.settings.local_cache_enabled:
            cache_dir = self.settings.local_cache_dir.strip()
            if not cache_dir:
                raise BigQmtConfigurationError(
                    "QMT_BRIDGE_LOCAL_CACHE_DIR is required when local cache is enabled"
                )
            config.update(
                {
                    "local_cache_enabled": True,
                    "local_cache_dir": cache_dir,
                    "local_cache_fallback_rpc": False,
                    "local_cache_format": self.settings.local_cache_format,
                }
            )
        return config

    def connect(self) -> dict[str, Any]:
        if self.settings.runtime != "bigqmt":
            raise BigQmtConfigurationError("QMT_BRIDGE_RUNTIME must be bigqmt")
        if self.settings.rpc_transport != "zmq":
            raise BigQmtConfigurationError("QMT_BRIDGE_RPC_TRANSPORT must be zmq")
        _require_loopback_endpoint(self.settings.zmq_endpoint)
        _require_loopback_endpoint(self.settings.event_zmq_endpoint)
        if self.settings.zmq_endpoint.strip().lower() == self.settings.event_zmq_endpoint.strip().lower():
            raise BigQmtConfigurationError(
                "QMT_BRIDGE_EVENT_ZMQ_ENDPOINT must differ from QMT_BRIDGE_ZMQ_ENDPOINT"
            )
        if self.settings.formula_host not in {"127.0.0.1", "localhost", "::1"}:
            raise BigQmtConfigurationError("QMT_BRIDGE_FORMULA_HOST must be loopback")
        if self.settings.formula_history_read_workers not in (1, 2):
            raise BigQmtConfigurationError("QMT_BRIDGE_FORMULA_HISTORY_READ_WORKERS must be 1 or 2")
        account_id = self.settings.trading_account_id.strip()
        if not account_id:
            raise BigQmtConfigurationError("QMT_BRIDGE_TRADING_ACCOUNT_ID is required")

        config = self._client_config()
        client = self._rpc_client_factory(
            account_id=account_id,
            redis_config=config,
            timeout_seconds=self.settings.rpc_timeout_seconds,
        )
        self.client = client
        self.market_data = self._data_client_factory(client)
        try:
            return self.probe()
        except Exception:
            # A client created before ping failure owns sockets/threads too.
            self.close()
            raise

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
        return str(account_id or self.settings.trading_account_id).strip()

    def new_trader(self):
        if self.client is None:
            raise BigQmtRuntimeUnavailable("Big QMT RPC client is not initialized")
        trader = self._trading_client_factory(
            account_id=self.settings.trading_account_id,
            redis_config=self._client_config(),
            timeout_seconds=self.settings.rpc_timeout_seconds,
        )
        trader.client = self.client
        return trader

    def set_transport_failure_callback(self, callback) -> None:
        """Bind lifecycle recovery to genuine native transport failures."""
        self._transport_failure_callback = callback
        for client in (self.client, self._minute_client):
            setter = getattr(client, "set_transport_failure_callback", None)
            if callable(setter):
                setter(callback)

    def readiness(self) -> dict[str, Any]:
        ping = dict(self.ping_payload)
        account_id = str(ping.get("account_id") or "")
        return {
            "ready": bool(self.client is not None and ping.get("pong") is True),
            "runtime": "bigqmt",
            "transport": "zmq",
            "zmq_endpoint": self.settings.zmq_endpoint,
            "event_zmq_endpoint": self.settings.event_zmq_endpoint,
            "formula_endpoint": f"{self.settings.formula_host}:{self.settings.formula_port}",
            "formula_enabled": self.settings.formula_enabled,
            "formula_history_read_workers": self.settings.formula_history_read_workers,
            "local_cache_enabled": self.settings.local_cache_enabled,
            "local_cache_dir": self.settings.local_cache_dir or None,
            "local_cache_format": (
                self.settings.local_cache_format
                if self.settings.local_cache_enabled
                else None
            ),
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

    def minute_data_client(self):
        """One owned read-only RPC lane, independent of account/bulk queues."""
        with self._minute_client_lock:
            if self.client is None:
                raise BigQmtRuntimeUnavailable("Big QMT runtime is not initialized")
            if self._minute_client is None:
                config = self._client_config()
                config["formula_server"] = {"enabled": False}
                config["local_cache_enabled"] = False
                self._minute_client = self._rpc_client_factory(
                    account_id=self.settings.trading_account_id.strip(),
                    redis_config=config,
                    timeout_seconds=self.settings.rpc_timeout_seconds,
                )
                self._minute_data_client = self._data_client_factory(self._minute_client)
                setter = getattr(self._minute_client, "set_transport_failure_callback", None)
                if callable(setter):
                    setter(self._transport_failure_callback)
            return self._minute_data_client

    @staticmethod
    def _close_client(client) -> None:
        formula_router = getattr(client, "_formula_router_instance", None)
        close_formula_router = getattr(formula_router, "close", None)
        if callable(close_formula_router):
            close_formula_router()
        stop_quote_listener = getattr(client, "stop_quote_event_listener", None)
        if callable(stop_quote_listener):
            stop_quote_listener()
        transport = getattr(client, "_transport_instance", None)
        if transport is not None:
            stop = getattr(transport, "stop", None)
            close = getattr(transport, "close", None)
            if callable(stop):
                stop()
            elif callable(close):
                close()

    def close(self) -> None:
        with self._minute_client_lock:
            for client in (self._minute_client, self.client):
                setter = getattr(client, "set_transport_failure_callback", None)
                if callable(setter):
                    setter(None)
            self._close_client(self._minute_client)
            self._minute_client = None
            self._minute_data_client = None
            self._close_client(self.client)
            self.client = None
            self.market_data = None
            self._transport_failure_callback = None
        self.ping_payload = {}


_runtime: BigQmtRuntime | None = None
_runtime_lock = threading.Lock()


def initialize_bigqmt_runtime(
    settings: Settings,
    *,
    rpc_client_factory=BigQmtRpcClient,
    data_client_factory=BigQmtDataClient,
    trading_client_factory=BigQmtTradingClient,
) -> BigQmtRuntime:
    global _runtime
    runtime = BigQmtRuntime(
        settings,
        rpc_client_factory=rpc_client_factory,
        data_client_factory=data_client_factory,
        trading_client_factory=trading_client_factory,
    )
    try:
        runtime.connect()
    except Exception:
        runtime.close()
        raise
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


def discard_bigqmt_runtime(runtime: BigQmtRuntime) -> bool:
    """Close a runtime only if it is still the active module-proxy owner."""
    global _runtime
    with _runtime_lock:
        if _runtime is not runtime:
            return False
        _runtime = None
    runtime.close()
    return True


class _MarketDataProxy:
    def __getattr__(self, name: str):
        runtime = get_bigqmt_runtime()
        if runtime.market_data is None:
            raise BigQmtRuntimeUnavailable("Big QMT data facade is not initialized")
        if name == "get_market_data_ex_scoped":
            return getattr(runtime.minute_data_client(), name)
        return getattr(runtime.market_data, name)


market_data = _MarketDataProxy()
