from types import SimpleNamespace

from qmt_bridge.server.config import Settings
from qmt_bridge.server.helpers import _numpy_to_python
from qmt_bridge.server.models import AsyncCancelRequest, CancelRequest
from qmt_bridge.server.routers import trading


class DummyManager:
    def __init__(self, account_id="acct-1"):
        self.account_id = account_id
        self.calls = []

    def cancel_order(self, *, order_id, account_id):
        self.calls.append(("order_id", order_id, account_id))
        return 0

    def cancel_order_sysid(self, *, order_sysid, market, account_id):
        self.calls.append(("order_sysid", order_sysid, market, account_id))
        return 0

    def cancel_order_async(self, *, order_id, account_id):
        self.calls.append(("order_id_async", order_id, account_id))
        return 101

    def cancel_order_sysid_async(self, *, order_sysid, market, account_id):
        self.calls.append(("order_sysid_async", order_sysid, market, account_id))
        return 102

    def query_asset(self, *, account_id):
        self.calls.append(("query_asset", account_id))
        return {"account_id": account_id or self.account_id, "cash": 1000}


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


def test_cancel_async_uses_order_sysid_when_present():
    manager = DummyManager()

    payload = trading.cancel_order_async(
        AsyncCancelRequest(account_id="acct-1", order_id=42, order_sysid="168", market=0),
        manager=manager,
    )

    assert manager.calls == [("order_sysid_async", "168", 0, "acct-1")]
    assert payload["seq"] == 102
    assert payload["cancel_method"] == "order_sysid"


def test_trading_health_reports_ready_write_contract():
    manager = DummyManager(account_id="8890450365")
    request = _request(
        Settings(trading_enabled=True, trading_account_id="8890450365"),
        manager,
    )

    payload = trading.trading_health(request)

    assert payload["status"] == "ok"
    assert payload["enabled"] is True
    assert payload["authenticated"] is True
    assert payload["account_authenticated"] is True
    assert payload["order_supported"] is True
    assert payload["cancel_supported"] is True
    assert payload["write_enabled"] is True
    assert payload["write_blockers"] == []
    assert payload["account_id"] == "8890450365"
    assert payload["supports"]["order_stock"] is True
    assert payload["supports"]["cancel_order_stock"] is True


def test_trading_health_reports_connect_failure_without_manager():
    request = _request(
        Settings(trading_enabled=True, trading_account_id="8890450365"),
        None,
    )

    payload = trading.trading_health(request)

    assert payload["status"] == "unavailable"
    assert payload["enabled"] is False
    assert payload["authenticated"] is False
    assert payload["account_authenticated"] is False
    assert payload["write_enabled"] is False
    assert payload["code"] == "QMT_TRADING_CONNECT_FAILED"
    assert payload["write_blockers"] == ["xttrader_connect_failed"]


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
