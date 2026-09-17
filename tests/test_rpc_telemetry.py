"""Telemetry boundaries for the local RPC path; no external services used."""

from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService
from bigqmt_signal_trader.rpc_client import BigQmtRpcClient


class _ResponseTransport:
    name = "local"

    def __init__(self):
        self.on_raw_payload = None
        self.responses = []

    def send_response(self, _request, response):
        self.responses.append(response)


class _Handlers:
    def clear_server_error(self):
        pass

    def handle(self, method, _params):
        assert method == "ping"
        return {"pong": True}


def test_client_to_server_preserves_trace_and_distinct_rpc_id(trace_events):
    server_transport = _ResponseTransport()
    service = RedisPubSubRpcService(
        redis_client=object(), handlers=_Handlers(), account_id="acct",
        transport=server_transport,
    )

    class _LocalTransport:
        name = "local"

        def send_request(self, request, _timeout_seconds):
            return service.process_request(request)

    client = BigQmtRpcClient(account_id="acct", redis_config={"transport": "zmq"})
    client._transport_instance = _LocalTransport()
    assert client.call("ping", force_rpc=True) == {"pong": True}

    events = trace_events()
    selected = [event for event in events if event["event_name"] in {
        "rpc.client.request.start", "rpc.client.wire.send", "rpc.server.received",
        "rpc.server.handler.start", "rpc.server.response",
    }]
    assert len(selected) == 4
    trace_ids = {event["trace_id"] for event in selected}
    rpc_ids = {event["rpc_request_id"] for event in selected}
    assert len(trace_ids) == 1
    assert len(rpc_ids) == 1
    wire_end = [event for event in events if event["event_name"] == "rpc.client.wire.end"][-1]
    assert wire_end["outcome"] == "response_received"
    assert wire_end["duration_scope"] == "transport_round_trip_including_queue"


def test_client_dispatch_is_not_mislabeled_as_queue_dequeue(trace_events):
    class _LocalTransport:
        name = "local"

        def send_request(self, _request, _timeout_seconds):
            return {"ok": True, "data": {}}

    client = BigQmtRpcClient(account_id="acct", redis_config={"transport": "zmq"})
    client._transport_instance = _LocalTransport()
    assert client.call("ping", force_rpc=True) == {}
    names = [event["event_name"] for event in trace_events()]
    assert "rpc.client.transport.dispatch" in names
    assert "rpc.client.queue.dequeue" not in names


def test_unsupported_response_does_not_become_transport_failure(trace_events):
    class _UnsupportedTransport:
        name = "local"

        def send_request(self, _request, _timeout_seconds):
            return {"ok": False, "error_type": "NotImplementedError", "error": "nope"}

    client = BigQmtRpcClient(account_id="acct", redis_config={"transport": "zmq"})
    client._transport_instance = _UnsupportedTransport()
    try:
        client.call("unsupported", force_rpc=True)
    except NotImplementedError:
        pass
    else:
        raise AssertionError("unsupported response must retain its business exception")
    events = trace_events()
    end = [event for event in events if event["event_name"] == "rpc.client.request.end"][-1]
    assert end["outcome"] == "unsupported"


def test_post_execution_or_reconcile_runtime_error_stays_unknown(trace_events):
    class _UncertainHandlers:
        def clear_server_error(self):
            pass

        def handle(self, _method, _params):
            raise RuntimeError("IDEMPOTENCY_CHECK_UNAVAILABLE after native submit")

    transport = _ResponseTransport()
    service = RedisPubSubRpcService(
        redis_client=object(), handlers=_UncertainHandlers(), account_id="acct",
        transport=transport,
    )
    response = service.process_request({
        "request_id": "uncertain-rpc", "account_id": "acct", "method": "submit_order",
        "params": {}, "trace": {"trace_id": "uncertain-trace"},
    })
    assert response["ok"] is False
    events = trace_events()
    response_event = [event for event in events if event["event_name"] == "rpc.server.response"][-1]
    assert response_event["outcome"] == "unknown"
    assert response_event["trace_id"] == "uncertain-trace"
