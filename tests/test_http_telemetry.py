"""HTTP/RPC/SDK correlation using only isolated ASGI and in-memory adapters."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from bigqmt_signal_trader import telemetry
from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
from bigqmt_signal_trader.rpc_client import BigQmtRpcClient
from qmt_bridge import QMTClient
from qmt_bridge.server.app import create_app
from qmt_bridge.server.config import Settings
from qmt_bridge.server.helpers import _call_xtdata_serialized
from qmt_bridge.server.runtime_controller import RuntimeController


def test_http_sync_thread_two_rpc_calls_share_trace_not_rpc_ids(trace_events):
    class Handlers:
        def handle(self, method, params):
            assert method == "ping"
            telemetry.emit("fixture.native", outcome="success")
            return {"pong": True}

    class Responses:
        name = "fixture"

        def send_response(self, request, response):
            pass

    service = RedisPubSubRpcService(redis_client=object(), handlers=Handlers(),
                                   account_id="fixture-account", transport=Responses())
    with ThreadPoolExecutor(max_workers=1) as executor:
        class Transport:
            name = "fixture"

            def send_request(self, request, timeout):
                # A new native thread has no HTTP ContextVar; the RPC trace
                # envelope must restore it rather than rely on ambient context.
                return executor.submit(service.process_request, request).result(timeout=1)

        rpc = BigQmtRpcClient(account_id="fixture-account", redis_config={"transport": "zmq"})
        rpc._transport_instance = Transport()
        app = create_app(Settings())

        @app.get("/_trace/{number}")
        def endpoint(number: int):
            return {"data": [_call_xtdata_serialized(rpc.call, "ping", force_rpc=True) for _ in range(number)]}

        client = TestClient(app)  # Deliberately no lifespan: never connect QMT.
        response = client.get("/_trace/2", headers={
            "X-QMT-Trace-Id": "a" * 32, "X-QMT-Span-Id": "b" * 16, "X-Request-ID": "http-fixture-1",
        })
        assert response.status_code == 200
        assert response.headers["x-qmt-trace-id"] == "a" * 32
        assert response.headers["x-request-id"] == "http-fixture-1"
        assert response.json()["data"] == [{"pong": True}, {"pong": True}]

    events = trace_events()
    native = [item for item in events if item["event_name"] == "fixture.native"]
    assert len(native) == 2
    assert {item["trace_id"] for item in native} == {"a" * 32}
    assert {item["http_request_id"] for item in native} == {"http-fixture-1"}
    assert len({item["rpc_request_id"] for item in native}) == 2
    request = [item for item in events if item["event_name"] == "http.request.end"][-1]
    assert request["parent_span_id"] == "b" * 16
    assert request["route"] == "/_trace/{number}" and request["response_complete"]
    assert request["http_status"] == 200


def test_http_rejections_and_business_status_are_observed_without_native_calls(trace_events):
    app = create_app(Settings(api_key="never-log-this-key", require_auth_for_data=True))

    @app.get("/_integer")
    def integer(value: int):
        return {"value": value}

    @app.get("/_unsupported")
    def unsupported():
        return {"status": "unsupported", "reason_code": "fixture"}

    client = TestClient(app)
    assert client.get("/api/meta/health").status_code == 401
    assert client.get("/_integer?value=no").status_code == 422
    assert client.get("/_missing").status_code == 404
    assert client.post("/_integer").status_code == 405
    assert client.get("/_unsupported").status_code == 200
    events = trace_events()
    end = [item for item in events if item["event_name"] == "http.request.end"]
    assert [item["http_status"] for item in end] == [401, 422, 404, 405, 200]
    assert len({item["trace_id"] for item in end}) == 5
    assert end[-1]["business_status"] == "unsupported"
    assert "never-log-this-key" not in json.dumps(events)
    assert not any(item["event_name"].startswith("rpc.") for item in events)


def test_http_exception_not_misreported_as_completed_response(trace_events):
    app = create_app(Settings())

    @app.get("/_broken")
    def broken():
        raise RuntimeError("private exception detail")

    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/_broken", headers={"X-QMT-Trace-Id": "not-valid", "X-Request-ID": "bad\tvalue"})
    assert response.status_code == 500
    events = trace_events()
    end = [item for item in events if item["event_name"] == "http.request.end"][-1]
    assert end["outcome"] == "error" and end["response_complete"] is False
    assert end["trace_id"] != "not-valid" and len(end["trace_id"]) == 32
    assert "private exception detail" not in json.dumps(events)


def test_optional_provider_error_keeps_response_but_not_arbitrary_error_text(trace_events):
    from qmt_bridge.server.helpers import _call_xtdata_optional

    def failing():
        raise OSError("private provider request details")

    response = _call_xtdata_optional(SimpleNamespace(failing=failing), "failing")
    assert response["status"] == "unavailable"
    assert response["reason"] == "private provider request details"
    assert "private provider request details" not in json.dumps(trace_events())


def test_sdk_http_emits_headers_decode_and_keeps_one_attempt(monkeypatch, trace_events):
    calls = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"status":"partial","data":{}}'

    def open_fixture(request, timeout):
        calls.append((request, timeout))
        return Response()

    monkeypatch.setattr("qmt_bridge.client.base.urlopen_direct", open_fixture)
    client = QMTClient("127.0.0.1", timeout=0.25, api_key="do-not-log")
    assert client._get("/fixture") == {"status": "partial", "data": {}}
    assert len(calls) == 1 and calls[0][1] == 0.25
    headers = {key.lower(): value for key, value in calls[0][0].header_items()}
    assert len(headers["x-qmt-trace-id"]) == 32 and len(headers["x-qmt-span-id"]) == 16
    events = trace_events()
    decoded = [item for item in events if item["event_name"] == "sdk.http.decode.end"][-1]
    assert decoded["business_status"] == "partial"
    assert decoded["trace_id"] == headers["x-qmt-trace-id"]
    assert "do-not-log" not in json.dumps(events)


def test_sdk_ws_callback_failure_closes_without_reconnect(trace_events):
    opened, closed, callbacks = [], [], []

    class Connection:
        close_code = 1000

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            closed.append(True)

        async def send(self, payload):
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            return '{"data":{}}'

    def connect(url, additional_headers=None):
        opened.append(additional_headers)
        return Connection()

    def callback(data):
        callbacks.append(data)
        raise ValueError("private callback failure")

    with pytest.raises(ValueError, match="private callback failure"):
        asyncio.run(QMTClient("127.0.0.1")._consume_subscription(connect, "/ws/realtime", callback, {"stocks": []}))
    assert len(opened) == len(closed) == len(callbacks) == 1
    events = trace_events()
    failed = [item for item in events if item["event_name"] == "sdk.ws.callback_failed"][-1]
    assert failed["trace_id"] == opened[0]["X-QMT-Trace-Id"]
    end = [item for item in events if item["event_name"] == "sdk.ws.end"][-1]
    assert end["received_count"] == 1 and end["handled_count"] == 0 and end["outcome"] == "error"
    assert "private callback failure" not in json.dumps(events)


def test_runtime_recovery_factory_thread_and_background_link(trace_events):
    attempts = []
    runtime = SimpleNamespace(close=lambda: None)

    def factory(settings):
        attempts.append(telemetry.current_context())
        if len(attempts) == 1:
            raise OSError("fixture offline")
        return runtime

    async def immediate(delay):
        await asyncio.sleep(0)

    async def scenario():
        controller = RuntimeController(Settings(), runtime_factory=factory, sleep=immediate)
        with telemetry.bind_context(trace_id="c" * 32):
            await controller.start()
        for _ in range(100):
            if controller.runtime is runtime:
                break
            await asyncio.sleep(0.001)
        assert controller.runtime is runtime and controller.generation == 1
        await controller.close()

    asyncio.run(scenario())
    assert len(attempts) == 2
    assert attempts[0]["trace_id"] == "c" * 32
    assert attempts[1]["trace_id"] != "c" * 32
    assert attempts[1]["caused_by_trace_id"] == "c" * 32
    events = trace_events()
    assert any(item["event_name"] == "runtime.connect_failed" for item in events)
    assert any(item["event_name"] == "runtime.ready" and item["runtime_generation"] == 1 for item in events)
