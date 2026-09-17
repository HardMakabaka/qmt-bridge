import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from qmt_bridge.server.config import Settings


class FakeClient:
    def __init__(self, *, account_id, redis_config, timeout_seconds):
        self.account_id = account_id
        self.redis_config = redis_config
        self.timeout_seconds = timeout_seconds
        self.calls = []

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append((method, params, account_id, timeout_seconds))
        if method == "ping":
            return {
                "pong": True,
                "account_id": self.account_id,
                "allow_order_methods": False,
                "qmt_trade_mode": "simulation",
                "terminal_real_mode": False,
                "rpc_revision": "test-rpc",
            }
        return {"method": method, "params": params or {}}


class CurrentRequestFakeClient(FakeClient):
    request_id = "qmt-request-17"

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        payload = super().call(method, params, account_id, timeout_seconds)
        if method == "ping":
            payload["qmt_request_id"] = self.request_id
            payload["allow_order_methods"] = True
            payload["qmt_trade_mode"] = "unknown"
            payload["terminal_real_mode"] = False
        return payload


class FakeXtData:
    def __init__(self, client):
        self.client = client

    def get_markets(self):
        return ["SH", "SZ"]


class FakeTrader:
    def __init__(self, **_kwargs):
        self.callback = None
        self.calls = []

    def register_callback(self, callback):
        self.callback = callback
        self.calls.append(("register_callback",))
        return 0

    def start(self):
        self.calls.append(("start",))
        return 0

    def connect(self):
        self.calls.append(("connect",))
        return 0

    def stop(self):
        self.calls.append(("stop",))
        return 0

    def query_orders(self, *, account_id, cancelable_only=False, client_submit_id=""):
        self.calls.append(("query_orders", account_id, cancelable_only, client_submit_id))
        return []

    def query_trades(self, *, account_id):
        self.calls.append(("query_trades", account_id))
        return []

    def submit_order(self, **kwargs):
        self.calls.append(("submit_order", kwargs))
        return "123"

    def cancel_order(self, *args, **kwargs):
        self.calls.append(("cancel_order",) + args)
        return True


def _runtime(settings, client_class=FakeClient, data_class=FakeXtData):
    from qmt_bridge.server.bigqmt import BigQmtRuntime

    return BigQmtRuntime(
        settings,
        rpc_client_factory=client_class,
        data_client_factory=data_class,
        trading_client_factory=FakeTrader,
    )


def test_settings_load_bigqmt_zmq_defaults_with_write_gate_enabled(monkeypatch):
    monkeypatch.setenv("QMT_BRIDGE_ACCOUNT_ENABLED", "true")
    monkeypatch.setenv("QMT_BRIDGE_TRADING_ACCOUNT_ID", "acct-1")
    monkeypatch.setenv("QMT_BRIDGE_ZMQ_ENDPOINT", "tcp://127.0.0.1:15560")
    monkeypatch.delenv("QMT_BRIDGE_ORDER_WRITES_ENABLED", raising=False)

    settings = Settings.from_env()

    assert settings.runtime == "bigqmt"
    assert settings.rpc_transport == "zmq"
    assert settings.zmq_endpoint == "tcp://127.0.0.1:15560"
    assert settings.formula_host == "127.0.0.1"
    assert settings.formula_port == 58600
    assert settings.formula_history_read_workers == 2
    assert settings.account_enabled is True
    assert settings.order_writes_enabled is True


def test_settings_explicit_false_freezes_write_gate(monkeypatch):
    monkeypatch.setenv("QMT_BRIDGE_ORDER_WRITES_ENABLED", "false")

    settings = Settings.from_env()

    assert settings.order_writes_enabled is False


def test_runtime_builds_explicit_zmq_client_without_redis_discovery():
    from qmt_bridge.server.bigqmt import BigQmtRuntime

    settings = Settings(
        trading_account_id="acct-1",
        zmq_endpoint="tcp://127.0.0.1:15560",
        formula_host="127.0.0.1",
        formula_port=58600,
        rpc_timeout_seconds=4.5,
    )
    runtime = _runtime(settings)

    ping = runtime.connect()

    assert ping["pong"] is True
    assert runtime.client.redis_config == {
        "transport": "zmq",
        "zmq": {
            "connect_address": "tcp://127.0.0.1:15560",
            "redis_discovery_enabled": False,
        },
        "exec_events": {
            "transport": "zmq",
            "zmq": {
                "connect_address": "tcp://127.0.0.1:15561",
            },
        },
        "formula_server": {
            "enabled": True,
            "host": "127.0.0.1",
            "port": 58600,
            "history_read_workers": 2,
        },
        "full_tick_cache_enabled": False,
        "local_cache_enabled": False,
    }
    assert runtime.client.timeout_seconds == 4.5
    assert runtime.readiness()["ready"] is True
    assert runtime.readiness()["upstream_order_writes_enabled"] is False
    assert runtime.readiness()["qmt_trade_mode"] == "unknown"
    assert runtime.readiness()["terminal_real_mode"] is False
    assert runtime.readiness()["terminal_mode_source"] == "unavailable"


def test_runtime_enables_explicit_history_download_cache(tmp_path):
    from qmt_bridge.server.bigqmt import BigQmtRuntime

    settings = Settings(
        trading_account_id="acct-1",
        local_cache_enabled=True,
        local_cache_dir=str(tmp_path),
        local_cache_format="pkl",
    )
    runtime = _runtime(settings)

    runtime.connect()

    assert runtime.client.redis_config["local_cache_enabled"] is True
    assert runtime.client.redis_config["local_cache_dir"] == str(tmp_path)
    assert runtime.client.redis_config["local_cache_fallback_rpc"] is False
    assert runtime.client.redis_config["local_cache_format"] == "pkl"
    assert runtime.readiness()["local_cache_enabled"] is True


def test_runtime_rejects_shared_rpc_and_event_zmq_endpoint():
    from qmt_bridge.server.bigqmt import BigQmtRuntime

    endpoint = "tcp://127.0.0.1:15560"
    runtime = _runtime(
        Settings(
            trading_account_id="acct-1",
            zmq_endpoint=endpoint,
            event_zmq_endpoint=endpoint,
        ),
    )

    with pytest.raises(ValueError, match="must differ"):
        runtime.connect()


def test_runtime_attests_real_mode_from_qmt_terminal_log(tmp_path):
    from qmt_bridge.server.bigqmt import BigQmtRuntime

    log_root = tmp_path / "userdata" / "log"
    log_root.mkdir(parents=True)
    (log_root / "XtClient_20260811.log").write_text(
        "2026-08-11 03:35:05,030 [INFO] [TC::CTradeStrategyData::doRun] "
        "m_requestID:qmt-request-17, m_updateTime:0, m_bTrade:1, m_runMode:1\n",
        encoding="utf-8",
    )
    runtime = _runtime(
        Settings(qmt_root=str(tmp_path), trading_account_id="acct-1"),
        CurrentRequestFakeClient,
    )

    ping = runtime.connect()

    assert ping["qmt_trade_mode"] == "trading"
    assert ping["terminal_real_mode"] is True
    assert ping["terminal_mode_source"] == "qmt_terminal_log"


def test_runtime_uses_latest_trade_mode_change_for_current_request(tmp_path):
    from qmt_bridge.server.bigqmt import BigQmtRuntime

    log_root = tmp_path / "userdata" / "log"
    log_root.mkdir(parents=True)
    (log_root / "XtClient_20260811.log").write_text(
        "2026-08-11 03:35:05,030 [INFO] [TC::CTradeStrategyData::doRun] "
        "m_requestID:qmt-request-17, m_updateTime:0, m_bTrade:1, m_runMode:1\n"
        "2026-08-11 03:36:05,030 [INFO] changeTradeType "
        "requestID:qmt-request-17, bTrade:false\n",
        encoding="utf-8",
    )
    runtime = _runtime(
        Settings(qmt_root=str(tmp_path), trading_account_id="acct-1"),
        CurrentRequestFakeClient,
    )

    ping = runtime.connect()

    assert ping["qmt_trade_mode"] == "simulation"
    assert ping["terminal_real_mode"] is False


def test_manager_refreshes_terminal_mode_before_order(tmp_path):
    from qmt_bridge.server.bigqmt import BigQmtRuntime
    from qmt_bridge.server.trading.manager import BigQmtTradingManager, TradingWriteDisabled

    log_root = tmp_path / "userdata" / "log"
    log_root.mkdir(parents=True)
    log_path = log_root / "XtClient_20260811.log"
    log_path.write_text(
        "2026-08-11 03:35:05,030 [INFO] [TC::CTradeStrategyData::doRun] "
        "m_requestID:qmt-request-17, m_updateTime:0, m_bTrade:1, m_runMode:1\n",
        encoding="utf-8",
    )
    runtime = _runtime(
        Settings(qmt_root=str(tmp_path), trading_account_id="acct-1"),
        CurrentRequestFakeClient,
    )
    runtime.connect()
    assert runtime.ping_payload["terminal_real_mode"] is True
    manager = BigQmtTradingManager(runtime=runtime, account_id="acct-1", order_writes_enabled=True)
    manager.connect()
    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(
            "2026-08-11 03:36:05,030 [INFO] changeTradeType "
            "requestID:qmt-request-17, bTrade:false\n"
        )

    with pytest.raises(TradingWriteDisabled, match="bigqmt_terminal_real_mode_required"):
        manager.order("000001.SZ", 23, 100)

    assert not any(call[0] == "submit_order" for call in manager._trader.calls)


def test_manager_starts_zmq_callback_listener_and_queries_account_wide():
    from qmt_bridge.server.bigqmt import BigQmtRuntime
    from qmt_bridge.server.trading.manager import BigQmtTradingManager

    runtime = _runtime(Settings(trading_account_id="acct-1"))
    runtime.connect()
    manager = BigQmtTradingManager(
        runtime=runtime,
        account_id="acct-1",
        order_writes_enabled=False,
    )

    manager.connect()
    manager.query_orders()
    manager.query_trades()

    assert ("connect",) in manager._trader.calls
    assert ("start",) in manager._trader.calls
    assert ("query_orders", "acct-1", False, "") in manager._trader.calls
    assert ("query_trades", "acct-1") in manager._trader.calls


def test_manager_blocks_orders_and_cancels_when_bridge_gate_is_off():
    from qmt_bridge.server.bigqmt import BigQmtRuntime
    from qmt_bridge.server.trading.manager import BigQmtTradingManager, TradingWriteDisabled

    runtime = _runtime(Settings(trading_account_id="acct-1"))
    runtime.connect()
    manager = BigQmtTradingManager(runtime=runtime, account_id="acct-1", order_writes_enabled=False)
    manager.connect()

    with pytest.raises(TradingWriteDisabled, match="bridge_order_writes_disabled"):
        manager.order("000001.SZ", 23, 100)
    with pytest.raises(TradingWriteDisabled, match="bridge_order_writes_disabled"):
        manager.cancel_order(123)
    assert not any(call[0] in {"submit_order", "cancel_order"} for call in manager._trader.calls)


def test_manager_blocks_writes_when_qmt_runtime_gate_is_off():
    from qmt_bridge.server.bigqmt import BigQmtRuntime
    from qmt_bridge.server.trading.manager import BigQmtTradingManager, TradingWriteDisabled

    runtime = _runtime(Settings(trading_account_id="acct-1"))
    runtime.connect()
    manager = BigQmtTradingManager(runtime=runtime, account_id="acct-1", order_writes_enabled=True)
    manager.connect()

    with pytest.raises(TradingWriteDisabled, match="bigqmt_order_methods_disabled"):
        manager.order("000001.SZ", 23, 100)


def test_manager_blocks_writes_when_terminal_is_not_in_real_mode(tmp_path):
    from qmt_bridge.server.bigqmt import BigQmtRuntime
    from qmt_bridge.server.trading.manager import BigQmtTradingManager, TradingWriteDisabled

    log_root = tmp_path / "userdata" / "log"
    log_root.mkdir(parents=True)
    (log_root / "XtClient_20260811.log").write_text(
        "2026-08-11 03:35:05,030 [INFO] [TC::CTradeStrategyData::doRun] "
        "m_requestID:qmt-request-17, m_updateTime:0, m_bTrade:0, m_runMode:1\n",
        encoding="utf-8",
    )
    runtime = _runtime(
        Settings(qmt_root=str(tmp_path), trading_account_id="acct-1"),
        CurrentRequestFakeClient,
    )
    runtime.connect()
    manager = BigQmtTradingManager(runtime=runtime, account_id="acct-1", order_writes_enabled=True)
    manager.connect()

    with pytest.raises(TradingWriteDisabled, match="bigqmt_terminal_real_mode_required"):
        manager.order("000001.SZ", 23, 100)
    assert not any(call[0] == "submit_order" for call in manager._trader.calls)


def test_server_source_has_no_native_xtquant_import_fallback():
    server_root = Path(__file__).parents[1] / "src" / "qmt_bridge" / "server"
    offenders = []
    for path in server_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name == "xtquant" or alias.name.startswith("xtquant.") for alias in node.names):
                    offenders.append(str(path.relative_to(server_root)))
            elif isinstance(node, ast.ImportFrom):
                if node.module and (node.module == "xtquant" or node.module.startswith("xtquant.")):
                    offenders.append(str(path.relative_to(server_root)))
    assert offenders == []


def test_scoped_minute_reads_use_one_owned_client_and_close_it(monkeypatch):
    from qmt_bridge.server import bigqmt

    created = []
    stopped = []

    class Client(FakeClient):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            created.append(self)
            self._transport_instance = SimpleNamespace(stop=lambda: stopped.append(self))

    class XtData(FakeXtData):
        def get_market_data_ex_scoped(self, **kwargs):
            return self.client.call("get_market_data_ex_scoped", kwargs)

    runtime = bigqmt.BigQmtRuntime(
        Settings(trading_account_id="acct-1"),
        rpc_client_factory=Client,
        data_client_factory=XtData,
        trading_client_factory=FakeTrader,
    )
    runtime.connect()
    monkeypatch.setattr(bigqmt, "_runtime", runtime)
    try:
        assert len(created) == 1
        for _ in range(2):
            bigqmt.market_data.get_market_data_ex_scoped(stock_list=["000001.SZ"], count=3)
        assert len(created) == 2
        assert created[0].calls == [("ping", None, None, None)]
        assert [call[0] for call in created[1].calls] == ["get_market_data_ex_scoped"] * 2
        assert created[1].redis_config["formula_server"]["enabled"] is False
        assert created[1].redis_config["local_cache_enabled"] is False
        assert bigqmt.market_data.get_markets() == ["SH", "SZ"]
    finally:
        runtime.close()
    assert len(stopped) == 2 and set(stopped) == set(created)
