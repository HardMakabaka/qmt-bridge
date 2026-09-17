from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.trading_client import BigQmtTradingClient
from qmt_bridge.server.models import OrderRequest
from qmt_bridge.server.routers.trading import _reconciled_order_payload


def test_native_buy_action_reconciles_with_http_numeric_order_type():
    def call(method, params=None, **kwargs):
        assert method == "query_orders"
        return [{"order_sys_id": "9001", "user_order_id": "same", "action": "BUY",
                 "stock_code": "000001.SZ", "volume": 100, "price": 10.0}]

    client = BigQmtTradingClient(account_id="fixture", client=SimpleNamespace(call=call))
    orders = client.query_orders(client_submit_id="same")
    request = OrderRequest(client_submit_id="same", stock_code="000001.SZ", order_type=23,
                           order_volume=100, price_type=11, price=10.0)
    result = _reconciled_order_payload(request, orders, idempotent=True)
    assert result["idempotent"] is True
    assert str(result["broker_order_id"]) == "9001"


@pytest.mark.parametrize("frozen", [None, "unknown", float("nan")])
def test_missing_or_invalid_balance_is_not_fabricated(frozen):
    payload = {"cash": 100, "total_asset": 500, "frozen_cash": frozen}
    client = BigQmtTradingClient(account_id="fixture", client=SimpleNamespace(call=lambda *a, **k: payload))
    asset = client.query_asset()
    assert asset["frozen_cash"] is None
    assert asset["market_value"] is None


@pytest.mark.parametrize("method", ["submit_order", "cancel_order", "sync_transaction_from_external"])
def test_readonly_extension_cannot_invoke_a_write(method):
    calls = []
    client = BigQmtTradingClient(account_id="fixture", client=SimpleNamespace(call=lambda *a, **k: calls.append(a)))
    with pytest.raises(ValueError, match="read-only"):
        client.query_extension(method)
    assert not calls


@pytest.mark.parametrize("code", ["OVERLOADED", "DEADLINE_EXCEEDED"])
def test_queue_rejection_is_not_a_transport_failure_or_an_unknown_submission(code):
    from bigqmt_signal_trader.rpc_client import BigQmtRpcClient, RpcRequestRejected

    failures = []
    transport = SimpleNamespace(send_request=lambda request, timeout: {
        "ok": False, "error_type": code, "request_id": "fixture-request", "error": "not executed",
    })
    client = BigQmtRpcClient(account_id="fixture", transport="zmq",
                             transport_failure_callback=failures.append)
    client._transport_instance = transport
    with pytest.raises(RpcRequestRejected) as error:
        client.call("submit_order", {}, force_rpc=True)
    assert error.value.code == code
    assert error.value.request_id == "fixture-request"
    assert not failures
