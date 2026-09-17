from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from qmt_bridge.client.trading import TradingMixin
from qmt_bridge.server.models import OrderRequest
from qmt_bridge.server.routers import trading
from qmt_bridge.server.trading.manager import BigQmtTradingManager


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


class _ReconcilingRouteManager(_RouteManager):
    def __init__(self, receipts, query_results):
        super().__init__(receipts)
        self.query_results = iter(query_results)
        self.query_calls = []

    def query_orders(self, **payload):
        self.query_calls.append(payload)
        result = next(self.query_results)
        if isinstance(result, Exception):
            raise result
        return result


class _Trader:
    def __init__(self):
        self.orders = [
            SimpleNamespace(order_id=9001, order_remark=CLIENT_ID),
            SimpleNamespace(order_id=9002, order_remark="another-client-id"),
        ]

    def query_orders(self, account_id, cancelable_only=False, strategy_name=""):
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


def _matching_order(order_id: int = 9001, stock_code: str = "300308.SZ"):
    return SimpleNamespace(
        order_id=order_id,
        order_sysid=str(order_id),
        order_remark=CLIENT_ID,
        stock_code=stock_code,
        order_type=23,
        order_volume=100,
        price=10.0,
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


def test_order_route_reconciles_a_missing_receipt_by_client_submit_id():
    # Given: the provider accepts one order but omits its broker receipt.
    manager = _ReconcilingRouteManager([-1], [[], [_matching_order()]])

    # When: the route performs its post-submit identity query.
    result = trading.place_order(_request(), manager=manager)

    # Then: the exact logical order is confirmed without a second provider write.
    assert len(manager.calls) == 1
    assert result["broker_order_id"] == 9001
    assert result["status"] == "submitted"
    assert result["reconciled"] is True
    assert result["idempotent"] is False


def test_order_route_replays_an_existing_client_submit_id_idempotently():
    # Given: a previous process already submitted this logical order.
    manager = _ReconcilingRouteManager([], [[_matching_order()]])

    # When: the caller retries after losing the original HTTP response.
    result = trading.place_order(_request(), manager=manager)

    # Then: the stored broker order is returned and no new order is sent.
    assert manager.calls == []
    assert result["broker_order_id"] == 9001
    assert result["status"] == "submitted"
    assert result["reconciled"] is True
    assert result["idempotent"] is True


def test_order_route_reconciles_all_matches_before_declaring_conflict():
    # Given: an old conflicting row and the correct broker row share the identity.
    manager = _ReconcilingRouteManager(
        [],
        [[
            _matching_order(order_id=8001, stock_code="510300.SH"),
            _matching_order(order_id=9001),
        ]],
    )

    # When: the route reconciles this logical submission.
    result = trading.place_order(_request(), manager=manager)

    # Then: the exact matching row wins and no write is replayed.
    assert result["broker_order_id"] == 9001
    assert result["idempotent"] is True
    assert manager.calls == []


def test_order_route_rejects_reused_identity_with_different_order_terms():
    # Given: the stable identity already belongs to a different security.
    manager = _ReconcilingRouteManager(
        [],
        [[_matching_order(stock_code="510300.SH")]],
    )

    # When/Then: the conflicting retry is rejected before another write.
    with pytest.raises(HTTPException, match="CLIENT_SUBMIT_ID_CONFLICT") as exc_info:
        trading.place_order(_request(), manager=manager)

    assert exc_info.value.status_code == 409
    assert manager.calls == []


def test_order_route_reconciles_a_transport_error_without_replaying():
    # Given: the write response is lost after the provider accepted the order.
    manager = _ReconcilingRouteManager(
        [TimeoutError("response lost")],
        [[], [_matching_order()]],
    )

    # When: the route queries by the stable logical identity.
    result = trading.place_order(_request(), manager=manager)

    # Then: it returns the original broker order after exactly one write attempt.
    assert len(manager.calls) == 1
    assert result["broker_order_id"] == 9001
    assert result["status"] == "submitted"
    assert result["reconciled"] is True
    assert result["idempotent"] is False


def test_order_route_fails_closed_when_preflight_reconciliation_is_unavailable():
    # Given: the identity lookup is unavailable before any provider write.
    manager = _ReconcilingRouteManager([9001], [TimeoutError("query offline")])

    # When: the route cannot prove whether the logical order already exists.
    result = trading.place_order(_request(), manager=manager)

    # Then: it reports a blocked submission and never calls the write provider.
    assert manager.calls == []
    assert result["status"] == "not_submitted"
    assert result["reason"] == "idempotency_check_unavailable"


def test_order_route_reports_unknown_when_post_submit_reconciliation_is_unavailable():
    # Given: preflight succeeds, the write loses its response, and recovery query fails.
    manager = _ReconcilingRouteManager(
        [TimeoutError("response lost")],
        [[], TimeoutError("query offline")],
    )

    # When: the route has already attempted exactly one provider write.
    result = trading.place_order(_request(), manager=manager)

    # Then: uncertainty is explicit and the write is never replayed.
    assert len(manager.calls) == 1
    assert result["status"] == "submit_unknown"
    assert result["reason"] == "transport_error_unreconciled"


def test_manager_filters_orders_by_client_submit_id():
    # Given: QMT reports two orders with different remarks.
    manager = BigQmtTradingManager(account_id="acct-1")
    from bigqmt_signal_trader.trading_client import BigQmtTradingClient

    client = SimpleNamespace(account_id="acct-1", call=lambda *args, **kwargs: [
        {"order_sys_id": "9001", "user_order_id": CLIENT_ID},
        {"order_sys_id": "9002", "user_order_id": "another-client-id"},
    ])
    manager._trader = BigQmtTradingClient(account_id="acct-1", client=client)

    # When: reconciliation queries one stable client identity.
    result = manager.query_orders(account_id="acct-1", client_submit_id=CLIENT_ID)

    # Then: only the matching broker order is returned.
    assert [row["order_id"] for row in result] == ["9001"]


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
