from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.models import OrderSubmitResult
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers, RedisPubSubRpcService


class _Gateway:
    def __init__(self):
        self.submits = []

    def query_submission_identities_strict(self, _account_id, _strategy_name):
        return [], []

    def submit(self, request):
        self.submits.append(request)
        return OrderSubmitResult("SUBMITTED", request.remark, "broker-1")


class _Transport:
    def __init__(self):
        self.responses = []
        self.on_raw_payload = None

    def send_response(self, request, response):
        self.responses.append((request, response))


def _handlers(gateway=None):
    return BigQmtRpcHandlers(
        account_id="acct-1",
        market_data=SimpleNamespace(),
        position_provider=SimpleNamespace(get_asset=lambda _a: {}, get_positions=lambda _a: []),
        order_gateway=gateway,
        allow_order_methods=True,
    )


def _order(price=10.0, strategy="strategy-1"):
    return {
        "stock_code": "000001.SZ", "action": "BUY", "volume": 100,
        "price": price, "price_type": "LIMIT", "strategy_name": strategy,
        "remark": "same-client-tag",
    }


def test_batch_reuses_single_order_journal_and_rejects_changed_identity(monkeypatch):
    gateway = _Gateway()
    handlers = _handlers(gateway)
    monkeypatch.setattr("bigqmt_signal_trader.redis_rpc.time.sleep", lambda _value: None)

    first = handlers.handle("submit_orders_batch", {
        "account_id": "acct-1", "strategy_name": "strategy-1", "orders": [_order()],
    })
    changed = handlers.handle("submit_orders_batch", {
        "account_id": "acct-1", "strategy_name": "strategy-1", "orders": [_order(price=11.0)],
    })

    assert first[0]["success"] is True
    assert changed[0]["success"] is False
    assert "CLIENT_SUBMIT_ID_CONFLICT" in changed[0]["error"]
    assert len(gateway.submits) == 1


def test_batch_rejects_item_account_or_strategy_conflicts():
    handlers = _handlers(_Gateway())
    account = handlers.handle("submit_orders_batch", {
        "account_id": "acct-1", "strategy_name": "strategy-1",
        "orders": [dict(_order(), account_id="acct-2")],
    })
    strategy = handlers.handle("submit_orders_batch", {
        "account_id": "acct-1", "strategy_name": "strategy-1",
        "orders": [_order(strategy="strategy-2")],
    })
    assert "BATCH_ACCOUNT_ID_CONFLICT" in account[0]["error"]
    assert "BATCH_STRATEGY_NAME_CONFLICT" in strategy[0]["error"]


def test_deferred_overload_replies_with_original_identity_and_expired_request_never_runs():
    transport = _Transport()
    calls = []

    class _Handlers:
        def handle(self, method, _params):
            calls.append(method)
            return {"ok": True}

    service = RedisPubSubRpcService(
        redis_client=object(), handlers=_Handlers(), account_id="acct-1",
        transport=transport, max_queue_size=1, process_in_listener=False,
    )
    first = {"request_id": "queued", "method": "slow", "account_id": "acct-1"}
    full = {"request_id": "full", "method": "slow", "account_id": "acct-1"}
    service.enqueue_payload(first)
    service.enqueue_payload(full)
    assert transport.responses[-1][1]["request_id"] == "full"
    assert transport.responses[-1][1]["error_type"] == "OVERLOADED"

    expired = {
        "request_id": "expired", "method": "must-not-run", "account_id": "acct-1",
        "deadline_epoch_ms": 1,
    }
    response = service.process_request(expired)
    assert response["error_type"] == "DEADLINE_EXCEEDED"
    assert calls == []
