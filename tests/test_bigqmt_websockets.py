from types import SimpleNamespace
import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from qmt_bridge.server.config import Settings
from qmt_bridge.server.ws import formula, realtime, whole_quote
from qmt_bridge.client.websocket import WebSocketMixin


def _app_with(router, *, api_key="test-key"):
    app = FastAPI()
    app.state.settings = Settings(api_key=api_key)
    app.include_router(router)
    return app


class CallbackXtData:
    def __init__(self, *, fail_code=""):
        self.fail_code = fail_code
        self.callbacks = {}
        self.subscribe_calls = []
        self.unsubscribe_calls = []

    def subscribe_quote(self, stock_code, *, period, callback):
        self.subscribe_calls.append((stock_code, period))
        if stock_code == self.fail_code:
            raise RuntimeError("native quote subscription unavailable")
        seq = len(self.subscribe_calls)
        self.callbacks[stock_code] = callback
        return seq

    def unsubscribe_quote(self, seq):
        self.unsubscribe_calls.append(seq)
        return 0


def test_realtime_websocket_forwards_callback_without_polling(monkeypatch):
    # Given: a callback-capable data facade (deliberately no get_full_tick method).
    facade = CallbackXtData()
    monkeypatch.setattr(realtime, "market_data", facade)

    # When: an authenticated client subscribes and QMT invokes its callback.
    with TestClient(_app_with(realtime.router)).websocket_connect(
        "/ws/realtime", headers={"X-API-Key": "test-key"}
    ) as ws:
        ws.send_json({"stocks": [" 000001.sz ", "000001.SZ"], "period": "tick"})
        ack = ws.receive_json()
        facade.callbacks["000001.SZ"](
            {
                "event_type": "quote",
                "code": "000001.SZ",
                "data": {"lastPrice": 10.5},
                "source_at": "2026-08-14T09:30:00+08:00",
            }
        )
        quote = ws.receive_json()
        ws.send_json({"action": "close"})

    # Then: the route acknowledges qmt_callback mode and forwards the quote.
    assert ack == {
        "type": "subscribed",
        "mode": "qmt_callback",
        "period": "tick",
        "codes": ["000001.SZ"],
        "subscription_ids": [1],
    }
    assert quote["type"] == "quote"
    assert quote["mode"] == "qmt_callback"
    assert quote["code"] == "000001.SZ"
    assert quote["data"]["lastPrice"] == 10.5
    assert quote["source_at"] == "2026-08-14T09:30:00+08:00"
    assert quote["received_at"]
    assert facade.unsubscribe_calls == [1]


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
def test_realtime_websocket_rejects_missing_or_invalid_header_key(headers):
    # Given: a server configured with an API key.
    client = TestClient(_app_with(realtime.router))

    # When/Then: missing or mismatched X-API-Key is closed with policy violation.
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect("/ws/realtime", headers=headers):
            pass
    assert exc_info.value.code == 1008


def test_realtime_websocket_rejects_more_than_500_codes(monkeypatch):
    # Given: an authenticated callback route.
    facade = CallbackXtData()
    monkeypatch.setattr(realtime, "market_data", facade)

    # When: the client supplies 501 distinct codes.
    with TestClient(_app_with(realtime.router)).websocket_connect(
        "/ws/realtime", headers={"X-API-Key": "test-key"}
    ) as ws:
        ws.send_json({"stocks": [f"{index:06d}.SZ" for index in range(501)]})
        status = ws.receive_json()

    # Then: no terminal subscriptions are attempted.
    assert status == {"status": "unavailable", "reason": "stocks_limit_1_500"}
    assert facade.subscribe_calls == []


def test_realtime_websocket_unsubscribes_partial_success_on_failure(monkeypatch):
    # Given: the first native subscription succeeds and the second fails.
    facade = CallbackXtData(fail_code="000002.SZ")
    monkeypatch.setattr(realtime, "market_data", facade)

    # When: the client requests both codes.
    with TestClient(_app_with(realtime.router)).websocket_connect(
        "/ws/realtime", headers={"X-API-Key": "test-key"}
    ) as ws:
        ws.send_json({"stocks": ["000001.SZ", "000002.SZ"]})
        status = ws.receive_json()

    # Then: capability failure is explicit and every positive seq is released.
    assert status["status"] == "unavailable"
    assert status["reason"] == "native_quote_callback_unavailable"
    assert facade.unsubscribe_calls == [1]


def test_latest_wins_buffer_coalesces_each_security(monkeypatch):
    # Given: two callbacks for the same security before the sender drains.
    clock = iter([1.0, 1.01, 1.25])
    monkeypatch.setattr(realtime.time, "monotonic", lambda: next(clock))
    buffer = realtime.LatestQuoteBuffer()
    first = {"code": "000001.SZ", "data": {"lastPrice": 10.0}}
    latest = {"code": "000001.SZ", "data": {"lastPrice": 10.5}}

    # When: callbacks arrive faster than four events per second.
    buffer.offer(first)
    buffer.offer(latest)

    # Then: only the latest event becomes eligible at the 250ms boundary.
    assert buffer.pop_ready() == latest
    assert buffer.pending_count == 0


def test_realtime_client_sends_api_key_header(monkeypatch):
    # Given: an SDK client and an in-memory websocket connector.
    captured = {}

    class WebSocketFake:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, message):
            captured["message"] = message

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    def connect(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return WebSocketFake()

    monkeypatch.setattr("websockets.connect", connect)
    monkeypatch.setattr(
        "qmt_bridge.client.websocket.inspect.signature",
        lambda _callable: SimpleNamespace(parameters={"additional_headers": None}),
    )
    client = WebSocketMixin()
    client.ws_url = "ws://127.0.0.1:13543"
    client.api_key = "test-key"
    client._headers = lambda: {"X-API-Key": client.api_key}

    # When: the SDK starts a realtime subscription.
    asyncio.run(client.subscribe_realtime(["000001.SZ"], lambda _event: None))

    # Then: authentication is carried in the handshake header, never the query string.
    assert captured["url"] == "ws://127.0.0.1:13543/ws/realtime"
    assert captured["kwargs"] == {"additional_headers": {"X-API-Key": "test-key"}}


def test_whole_quote_websocket_uses_bounded_polling(monkeypatch):
    calls = []

    def get_full_tick(*, code_list):
        calls.append(code_list)
        return {"000001.SZ": {"lastPrice": 10.5}}

    monkeypatch.setattr(whole_quote, "market_data", SimpleNamespace(get_full_tick=get_full_tick))

    with TestClient(_app_with(whole_quote.router)).websocket_connect("/ws/whole_quote") as ws:
        ws.send_json({"codes": ["SH"], "interval_seconds": 3})
        payload = ws.receive_json()

    assert calls == [["SH"]]
    assert payload["type"] == "snapshot"
    assert payload["mode"] == "bigqmt_polling"


def test_formula_websocket_reports_push_as_unsupported():
    with TestClient(_app_with(formula.router)).websocket_connect("/ws/formula") as ws:
        ws.send_json({"action": "subscribe", "formula_name": "MA"})
        payload = ws.receive_json()

    assert payload["status"] == "unsupported"
    assert payload["capability"] == "formula_push"
    assert payload["reason_code"] == "bigqmt_formula_push_not_verified"
    assert payload["retryable"] is False


def test_whole_quote_abrupt_client_close_releases_shared_sampler(monkeypatch):
    calls = []

    def get_full_tick(*, code_list):
        calls.append(code_list)
        return {"000001.SZ": {"lastPrice": 10.5}}

    monkeypatch.setattr(whole_quote, "market_data", SimpleNamespace(get_full_tick=get_full_tick))
    for _ in range(10):
        with TestClient(_app_with(whole_quote.router)).websocket_connect("/ws/whole_quote") as ws:
            ws.send_json({"codes": ["000001.SZ"]})
            assert ws.receive_json()["type"] == "snapshot"

    assert len(calls) == 10


def test_whole_quote_websocket_emits_lifecycle_trace(monkeypatch, trace_events):
    def get_full_tick(*, code_list):
        return {"000001.SZ": {"lastPrice": 10.5}}

    monkeypatch.setattr(whole_quote, "market_data", SimpleNamespace(get_full_tick=get_full_tick))
    with TestClient(_app_with(whole_quote.router)).websocket_connect("/ws/whole_quote") as ws:
        ws.send_json({"codes": ["000001.SZ"]})
        assert ws.receive_json()["type"] == "snapshot"

    names = [event["event_name"] for event in trace_events()]
    assert "ws.whole_quote.connected" in names
    assert "ws.whole_quote.subscription.start" in names
    assert "ws.whole_quote.cleanup" in names
