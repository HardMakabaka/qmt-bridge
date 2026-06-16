from qmt_bridge.server.models import AsyncCancelRequest, CancelRequest
from qmt_bridge.server.routers import trading


class DummyManager:
    def __init__(self):
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
