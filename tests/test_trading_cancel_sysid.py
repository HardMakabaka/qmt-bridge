from types import SimpleNamespace

from qmt_bridge.server.config import Settings
from qmt_bridge.server.helpers import _market_data_to_records, _numpy_to_python
from qmt_bridge.server.models import CancelRequest
from qmt_bridge.server.routers import trading


class DummyManager:
    def __init__(self, account_id="acct-1", writes_enabled=False, write_blockers=None):
        self.account_id = account_id
        self.calls = []
        self.writes_enabled = writes_enabled
        self.write_blockers = list(write_blockers or ["bridge_order_writes_disabled"])

    def cancel_order(self, *, order_id, account_id):
        self.calls.append(("order_id", order_id, account_id))
        return 0

    def cancel_order_sysid(self, *, order_sysid, market, account_id):
        self.calls.append(("order_sysid", order_sysid, market, account_id))
        return 0

    def query_asset(self, *, account_id):
        self.calls.append(("query_asset", account_id))
        return {"account_id": account_id or self.account_id, "cash": 1000}


class ReconcilingCancelManager(DummyManager):
    def __init__(self, order_states, cancel_result=None):
        super().__init__()
        self.order_states = iter(order_states)
        self.cancel_result = cancel_result

    def query_order_detail(self, *, order_id, account_id):
        self.calls.append(("query_order_detail", order_id, account_id))
        result = next(self.order_states)
        if isinstance(result, Exception):
            raise result
        return result

    def cancel_order(self, *, order_id, account_id):
        self.calls.append(("order_id", order_id, account_id))
        if isinstance(self.cancel_result, Exception):
            raise self.cancel_result
        return self.cancel_result


class SysidCancelManager(DummyManager):
    def __init__(self, order_states, cancel_result=None):
        super().__init__()
        self.order_states = iter(order_states)
        self.cancel_result = cancel_result

    def query_orders(self, **payload):
        self.calls.append(("query_orders", payload))
        return next(self.order_states)

    def cancel_order_sysid(self, *, order_sysid, market, account_id):
        self.calls.append(("order_sysid", order_sysid, market, account_id))
        if isinstance(self.cancel_result, Exception):
            raise self.cancel_result
        return self.cancel_result


def _request(settings, manager):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(settings=settings, trader_manager=manager)))


class SlotAsset:
    __slots__ = ("account_id", "cash", "frozen_cash", "market_value", "total_asset")

    def __init__(self):
        self.account_id = "8890450365"
        self.cash = 1000.5
        self.frozen_cash = 10
        self.market_value = 2000
        self.total_asset = 3000.5


def test_cancel_order_uses_order_sysid_when_present():
    manager = DummyManager()

    payload = trading.cancel_order(
        CancelRequest(account_id="acct-1", order_id=42, order_sysid="168", market=0),
        manager=manager,
    )

    assert manager.calls == [("order_sysid", "168", 0, "acct-1")]
    assert payload["cancel_method"] == "order_sysid"
    assert payload["order_sysid"] == "168"
    assert payload["market"] == 0


def test_cancel_order_keeps_order_id_fallback():
    manager = DummyManager()

    payload = trading.cancel_order(CancelRequest(account_id="acct-1", order_id=42), manager=manager)

    assert manager.calls == [("order_id", 42, "acct-1")]
    assert payload["cancel_method"] == "order_id"


def test_cancel_retry_returns_an_existing_cancel_terminal_state():
    # Given: the original cancel succeeded before its HTTP response was lost.
    manager = ReconcilingCancelManager(
        [SimpleNamespace(order_id=42, order_status=54)],
    )

    # When: the caller retries the same cancel request after service recovery.
    payload = trading.cancel_order(
        CancelRequest(account_id="acct-1", order_id=42),
        manager=manager,
    )

    # Then: the terminal state is returned without sending a second cancel.
    assert manager.calls == [("query_order_detail", 42, "acct-1")]
    assert payload["status"] == "ok"
    assert payload["order_status"] == 54
    assert payload["idempotent"] is True
    assert payload["reconciled"] is True


def test_cancel_retry_does_not_touch_an_already_filled_order():
    # Given: the order filled while the original cancel response was unavailable.
    manager = ReconcilingCancelManager(
        [SimpleNamespace(order_id=42, order_status=56)],
    )

    # When: the caller retries the cancel after reconnecting.
    payload = trading.cancel_order(
        CancelRequest(account_id="acct-1", order_id=42),
        manager=manager,
    )

    # Then: the terminal fill is reported and no invalid cancel is replayed.
    assert manager.calls == [("query_order_detail", 42, "acct-1")]
    assert payload["status"] == "not_cancelable"
    assert payload["order_status"] == 56
    assert payload["idempotent"] is True
    assert payload["reconciled"] is True


def test_cancel_still_sends_once_when_preflight_reconciliation_is_unavailable():
    # Given: current broker state cannot be queried but the cancel receipt is valid.
    manager = ReconcilingCancelManager(
        [TimeoutError("query offline")],
        cancel_result=True,
    )

    # When: a cancel arrives while reconciliation is unavailable.
    payload = trading.cancel_order(
        CancelRequest(account_id="acct-1", order_id=42),
        manager=manager,
    )

    # Then: the risk-reducing cancel is sent exactly once without a blind retry.
    assert manager.calls == [
        ("query_order_detail", 42, "acct-1"),
        ("order_id", 42, "acct-1"),
    ]
    assert payload["status"] == "ok"


def test_cancel_transport_error_is_reconciled_without_retrying_provider():
    # Given: the provider changes the order to canceled but loses the response.
    manager = ReconcilingCancelManager(
        [
            SimpleNamespace(order_id=42, order_status=50),
            SimpleNamespace(order_id=42, order_status=54),
        ],
        cancel_result=ConnectionError("response lost"),
    )

    # When: the route reconciles after the transport exception.
    payload = trading.cancel_order(
        CancelRequest(account_id="acct-1", order_id=42),
        manager=manager,
    )

    # Then: one cancel was attempted and the observed terminal state is returned.
    assert manager.calls == [
        ("query_order_detail", 42, "acct-1"),
        ("order_id", 42, "acct-1"),
        ("query_order_detail", 42, "acct-1"),
    ]
    assert payload["status"] == "ok"
    assert payload["order_status"] == 54
    assert payload["idempotent"] is False
    assert payload["reconciled"] is True


def test_cancel_false_receipt_is_reconciled_before_reporting_success():
    # Given: the provider returns a false receipt while the order remains reported.
    manager = ReconcilingCancelManager(
        [
            SimpleNamespace(order_id=42, order_status=50),
            SimpleNamespace(order_id=42, order_status=50),
        ],
        cancel_result=False,
    )

    # When: the route receives the unusable cancel receipt.
    payload = trading.cancel_order(
        CancelRequest(account_id="acct-1", order_id=42),
        manager=manager,
    )

    # Then: it reports uncertainty rather than a false success.
    assert payload["status"] == "cancel_unknown"
    assert payload["reason"] == "broker_receipt_missing"
    assert manager.calls.count(("order_id", 42, "acct-1")) == 1


def test_cancel_by_sysid_is_idempotent_after_service_restart():
    # Given: the order identified by its counter sysid is already canceled.
    manager = SysidCancelManager(
        [[SimpleNamespace(order_sysid="168", order_status=54)]],
    )

    # When: the caller retries after the service restarts.
    payload = trading.cancel_order(
        CancelRequest(account_id="acct-1", order_sysid="168", market=0),
        manager=manager,
    )

    # Then: no second provider cancel is sent.
    assert manager.calls == [(
        "query_orders",
        {
            "account_id": "acct-1",
            "cancelable_only": False,
            "client_submit_id": "",
        },
    )]
    assert payload["status"] == "ok"
    assert payload["cancel_method"] == "order_sysid"
    assert payload["idempotent"] is True
    assert payload["reconciled"] is True


def test_trading_health_reports_ready_write_contract():
    manager = DummyManager(
        account_id="8890450365",
        writes_enabled=False,
        write_blockers=["bridge_order_writes_disabled"],
    )
    request = _request(
        Settings(account_enabled=True, trading_account_id="8890450365"),
        manager,
    )

    payload = trading.trading_health(request)

    assert payload["status"] == "ok"
    assert payload["enabled"] is True
    assert payload["authenticated"] is True
    assert payload["account_authenticated"] is True
    assert payload["order_supported"] is True
    assert payload["cancel_supported"] is True
    assert payload["write_enabled"] is False
    assert payload["write_blockers"] == ["bridge_order_writes_disabled"]
    assert payload["account_id"] == "8890450365"
    assert payload["supports"]["submit_order"] is True
    assert payload["supports"]["cancel_order"] is True


def test_trading_health_reports_connect_failure_without_manager():
    request = _request(
        Settings(account_enabled=True, trading_account_id="8890450365"),
        None,
    )

    payload = trading.trading_health(request)

    assert payload["status"] == "unavailable"
    assert payload["enabled"] is False
    assert payload["authenticated"] is False
    assert payload["account_authenticated"] is False
    assert payload["write_enabled"] is False
    assert payload["code"] == "QMT_TRADING_CONNECT_FAILED"
    assert payload["write_blockers"] == ["bigqmt_rpc_connect_failed"]


def test_assets_plural_alias_uses_existing_asset_query():
    manager = DummyManager(account_id="8890450365")

    payload = trading.query_assets(account_id="8890450365", manager=manager)

    assert manager.calls == [("query_asset", "8890450365")]
    assert payload == {"data": {"account_id": "8890450365", "cash": 1000}}


def test_numpy_to_python_converts_slot_based_xtquant_objects():
    payload = _numpy_to_python(SlotAsset())

    assert payload == {
        "account_id": "8890450365",
        "cash": 1000.5,
        "frozen_cash": 10,
        "market_value": 2000,
        "total_asset": 3000.5,
    }


def test_numpy_to_python_converts_non_finite_float_to_none():
    payload = _numpy_to_python({"nan": float("nan"), "inf": float("inf")})

    assert payload == {"nan": None, "inf": None}


def test_market_data_records_convert_non_finite_values_to_none():
    import pandas as pd

    raw = {
        "open": pd.DataFrame({"20260623": [float("nan")]}, index=["000001.SZ"]),
        "close": pd.DataFrame({"20260623": [float("inf")]}, index=["000001.SZ"]),
    }

    payload = _market_data_to_records(raw, ["000001.SZ"], ["open", "close"])

    assert payload == {"000001.SZ": [{"date": "20260623", "open": None, "close": None}]}
