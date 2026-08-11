from types import SimpleNamespace

from qmt_bridge.client.trading import TradingMixin
from qmt_bridge.server.models import OrderRequest
from qmt_bridge.server.routers import trading
from qmt_bridge.server.trading.manager import XtTraderManager


CLIENT_ID = "meco00000000000000000001"


class _RouteManager:
    def __init__(self, receipts):
        self.receipts = iter(receipts)
        self.calls = []

    def order(self, **payload):
        self.calls.append(payload)
        result = next(self.receipts)
        if isinstance(result, Exception):
            raise result
        return result


class _Trader:
    def __init__(self):
        self.orders = [
            SimpleNamespace(order_id=9001, order_remark=CLIENT_ID),
            SimpleNamespace(order_id=9002, order_remark="another-client-id"),
        ]

    def query_stock_orders(self, _account, _cancelable_only):
        return self.orders


class _Client(TradingMixin):
    def __init__(self):
        self.calls = []

    def _post(self, path, payload):
        self.calls.append(("post", path, payload))
        return {"status": "ok"}

    def _get(self, path, payload=None):
        self.calls.append(("get", path, payload))
        return {"status": "ok"}


def _request(client_submit_id: str = CLIENT_ID) -> OrderRequest:
    return OrderRequest(
        account_id="acct-1",
        stock_code="300308.SZ",
        order_type=23,
        order_volume=100,
        price_type=11,
        price=10.0,
        strategy_name="MeCoStock",
        client_submit_id=client_submit_id,
    )


def test_order_route_maps_client_id_to_remark_and_returns_receipt():
    # Given: one valid client identity and one positive QMT receipt.
    manager = _RouteManager([9001])

    # When: the HTTP route submits the order.
    result = trading.place_order(_request(), manager=manager)

    # Then: the identity reaches QMT remark and is echoed with the broker id.
    assert manager.calls[0]["order_remark"] == CLIENT_ID
    assert result == {
        "client_submit_id": CLIENT_ID,
        "order_remark": CLIENT_ID,
        "broker_order_id": 9001,
        "order_id": 9001,
        "status": "submitted",
    }


def test_order_route_marks_missing_receipt_unknown():
    # Given: QMT returns no usable broker receipt.
    manager = _RouteManager([-1])

    # When: the route parses that result.
    result = trading.place_order(_request(), manager=manager)

    # Then: it echoes the client id but does not claim submission success.
    assert result["client_submit_id"] == CLIENT_ID
    assert result["status"] == "submit_unknown"
    assert result["reason"] == "broker_receipt_missing"
    assert "broker_order_id" not in result


def test_manager_filters_orders_by_client_submit_id():
    # Given: QMT reports two orders with different remarks.
    manager = XtTraderManager(account_id="acct-1")
    manager._account = SimpleNamespace(account_id="acct-1")
    manager._trader = _Trader()

    # When: reconciliation queries one stable client identity.
    result = manager.query_orders(account_id="acct-1", client_submit_id=CLIENT_ID)

    # Then: only the matching broker order is returned.
    assert [row.order_id for row in result] == [9001]


def test_batch_order_reports_partial_results_without_replaying_success():
    # Given: the first order succeeds and the second loses its response.
    manager = _RouteManager([9001, TimeoutError("response lost")])
    second_id = "meco00000000000000000002"

    # When: both logical orders are processed once.
    result = trading.batch_order([_request(), _request(second_id)], manager=manager)

    # Then: success and uncertainty remain isolated per item.
    assert len(manager.calls) == 2
    assert result["status"] == "partial"
    assert result["results"][0]["status"] == "submitted"
    assert result["results"][0]["client_submit_id"] == CLIENT_ID
    assert result["results"][1]["status"] == "submit_unknown"
    assert result["results"][1]["client_submit_id"] == second_id


def test_client_sends_identity_on_submit_and_query():
    # Given: a client facade with captured HTTP calls.
    client = _Client()

    # When: it submits and then queries by the same identity.
    client.place_order(
        "300308.SZ",
        23,
        100,
        11,
        10.25,
        "MeCoStock",
        "legacy-remark",
        "acct-1",
        client_submit_id=CLIENT_ID,
    )
    client.query_orders(account_id="acct-1", client_submit_id=CLIENT_ID)

    # Then: both request boundaries carry the identity.
    assert client.calls[0][2]["client_submit_id"] == CLIENT_ID
    assert client.calls[0][2]["order_remark"] == CLIENT_ID
    assert client.calls[0][2]["price_type"] == 11
    assert client.calls[0][2]["price"] == 10.25
    assert client.calls[0][2]["account_id"] == "acct-1"
    assert client.calls[1] == (
        "get",
        "/api/trading/orders",
        {
            "account_id": "acct-1",
            "cancelable_only": False,
            "client_submit_id": CLIENT_ID,
        },
    )
