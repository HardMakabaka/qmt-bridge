from types import SimpleNamespace
import threading

import pytest

from bigqmt_signal_trader.models import OrderSubmitResult
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.trading_client import BigQmtTradingClient
from bigqmt_signal_trader.telemetry import emit
from qmt_bridge.server.trading.manager import BigQmtTradingManager


class _Gateway:
    def __init__(self):
        self.submits = []

    def query_submission_identities_strict(self, _account_id, _strategy_name):
        return [], []

    def submit(self, request):
        self.submits.append(request)
        return OrderSubmitResult("SUBMITTED", request.remark, None, "accepted")


def _handlers(gateway):
    return BigQmtRpcHandlers(
        account_id="acct-telemetry", market_data=SimpleNamespace(),
        position_provider=SimpleNamespace(get_asset=lambda _a: {}, get_positions=lambda _a: []),
        order_gateway=gateway, allow_order_methods=True,
    )


def _names(events):
    return [event["event_name"] for event in events]


def test_rpc_trading_telemetry_covers_rejection_unknown_and_batch(trace_events, monkeypatch):
    gateway = _Gateway()
    handlers = _handlers(gateway)
    monkeypatch.setattr("bigqmt_signal_trader.redis_rpc.time.sleep", lambda _v: None)

    with pytest.raises(ValueError, match="volume must be positive"):
        handlers.handle("submit_order", {
            "account_id": "acct-telemetry", "remark": "reject-1", "stock_code": "000001.SZ",
            "action": "BUY", "volume": 0, "price": 10.0,
        })
    unknown = handlers.handle("submit_order", {
        "account_id": "acct-telemetry", "remark": "unknown-1", "stock_code": "000001.SZ",
        "action": "BUY", "volume": 100, "price": 10.0,
    })
    batch = handlers.handle("submit_orders_batch", {
        "account_id": "acct-telemetry", "batch_id": "batch-1", "orders": [{
            "remark": "batch-1-item", "stock_code": "000002.SZ", "action": "BUY",
            "volume": 100, "price": 9.0,
        }],
    })

    assert unknown.status == "SUBMITTED_UNCONFIRMED"
    assert batch[0]["success"] is True
    assert len(gateway.submits) == 2
    names = _names(trace_events())
    assert "rpc.trade.submit.gate" in names
    assert "rpc.trade.submit.receipt" in names
    assert "rpc.trade.batch_item" in names


class _Client:
    def __init__(self):
        self.account_id = "acct-telemetry"

    def call(self, method, _params=None, **_kwargs):
        if method == "submit_orders_batch":
            return [{"success": False, "accepted": False}]
        if method == "cancel_order":
            return {"success": False}
        if method == "sync_transaction_from_external":
            raise TimeoutError("simulated transport loss")
        return {}


def _event(events, name):
    return [item for item in events if item["event_name"] == name][-1]


def test_client_telemetry_does_not_promote_false_or_exceptional_receipts(trace_events):
    client = BigQmtTradingClient(account_id="acct-telemetry", client=_Client())
    assert client.submit_batch([{"stock_code": "000001.SZ"}], batch_id="b-1") == [{"success": False, "accepted": False}]
    assert client.cancel_order_sysid("sys-1") is False
    with pytest.raises(TimeoutError):
        client.sync_transaction("op", "type", [{"id": 1}])

    events = trace_events()
    assert _event(events, "trading.client.batch.return")["outcome"] == "rejected"
    assert _event(events, "trading.client.cancel.return")["outcome"] == "rejected"
    assert _event(events, "trading.client.external_sync.return")["outcome"] == "unknown"


class _Runtime:
    ping_payload = {"allow_order_methods": True, "terminal_real_mode": True}

    def probe(self):
        return None


class _Trader:
    def submit_batch(self, *_args, **_kwargs):
        return [{"success": False, "accepted": False}]

    def cancel_order(self, *_args, **_kwargs):
        return False

    def cancel_order_sysid(self, *_args, **_kwargs):
        return False


def test_manager_telemetry_summarizes_false_batch_and_cancel(trace_events):
    manager = BigQmtTradingManager(runtime=_Runtime(), account_id="acct-telemetry")
    manager._trader = _Trader()
    assert manager.submit_batch([{}], batch_id="b-manager") == [{"success": False, "accepted": False}]
    assert manager.cancel_order("sys-1") is False
    assert manager.cancel_order_sysid("sys-2", "SZ") is False

    events = trace_events()
    assert _event(events, "trading.manager.batch.return")["outcome"] == "rejected"
    returns = [item for item in events if item["event_name"] == "trading.manager.cancel.return"]
    assert [item["outcome"] for item in returns] == ["rejected", "rejected"]


class _EventCallback:
    def on_trade(self, _trade):
        # Models the downstream WS/notifier handoff: it must inherit the
        # producer's wire trace even on the listener's independent thread.
        emit("test.notify.trade", outcome="success")


def test_execution_wire_trace_crosses_listener_thread_without_leaking_to_legacy_event(trace_events):
    client = BigQmtTradingClient(account_id="acct-telemetry", client=_Client())
    client.register_callback(_EventCallback())
    traced = {
        "account_id": "acct-telemetry", "event_type": "trade", "trade_id": "trade-1",
        "cursor": {"epoch": "e-1", "sequence": 1},
        "trace": {"trace_id": "producer-trace", "span_id": "producer-span"},
    }
    worker = threading.Thread(target=client._dispatch_event, args=(traced,))
    worker.start()
    worker.join(1.0)
    assert not worker.is_alive()
    # An old payload has no trace. It must create its own root instead of
    # inheriting the previous persistent-listener context.
    client._dispatch_event({
        "account_id": "acct-telemetry", "event_type": "trade", "trade_id": "trade-2",
        "cursor": {"epoch": "e-1", "sequence": 2},
    })

    events = trace_events()
    notify = [item for item in events if item["event_name"] == "test.notify.trade"][0]
    dispatches = [item for item in events if item["event_name"] == "trading.execution.dispatch.start"]
    assert notify["trace_id"] == "producer-trace"
    assert dispatches[0]["trace_id"] == "producer-trace"
    assert dispatches[1]["trace_id"] != "producer-trace"
