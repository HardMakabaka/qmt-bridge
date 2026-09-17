from __future__ import annotations

from collections import Counter
import threading
import time

import pytest

from bigqmt_signal_trader.redis_rpc import encode_rpc_request_payload
from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout
from bigqmt_signal_trader.transports.zmq_transport import (
    ZmqTransport,
    _ClientCall,
    _loads,
)


class LoopbackRouter:
    def __init__(self, handler):
        self.handler = handler
        self.methods = []
        self.owner_ident = None
        self.address = None
        self.received = threading.Event()
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="test-zmq-router")

    def start(self):
        self._thread.start()
        assert self._ready.wait(2.0)
        return self

    def stop(self):
        self._stop.set()
        self._thread.join(2.0)
        assert not self._thread.is_alive()

    def _run(self):
        import zmq

        self.owner_ident = threading.get_ident()
        context = zmq.Context()
        router = context.socket(zmq.ROUTER)
        router.setsockopt(zmq.LINGER, 0)
        router.bind("tcp://127.0.0.1:*")
        self.address = router.getsockopt(zmq.LAST_ENDPOINT).decode("utf-8")
        self._ready.set()
        poller = zmq.Poller()
        poller.register(router, zmq.POLLIN)
        try:
            while not self._stop.is_set():
                events = dict(poller.poll(20))
                if not (events.get(router, 0) & zmq.POLLIN):
                    continue
                frames = router.recv_multipart()
                request = _loads(frames[-1])
                self.methods.append(request["method"])
                self.received.set()
                response = self.handler(request)
                if response is not None:
                    router.send_multipart(
                        [frames[0], encode_rpc_request_payload(response).encode("utf-8")]
                    )
        finally:
            router.close(linger=0)
            context.term()


class RecordingZmqTransport(ZmqTransport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_socket_operation_threads = []

    def _assert_client_owner(self):
        self.client_socket_operation_threads.append(threading.get_ident())
        return super()._assert_client_owner()


def _request(method, request_id):
    return {
        "schema_version": 1,
        "request_id": request_id,
        "account_id": "fixture-only",
        "method": method,
        "params": {},
    }


def _response(request):
    return {
        "schema_version": 1,
        "request_id": request["request_id"],
        "account_id": request["account_id"],
        "method": request["method"],
        "ok": True,
        "data": {"method": request["method"]},
        "error": None,
    }


def test_zmq_client_normal_order_cancel_sent_once_and_socket_owner_thread():
    router = LoopbackRouter(_response).start()
    transport = RecordingZmqTransport(connect_address=router.address, account_id="fixture-only")
    caller_ident = threading.get_ident()
    try:
        assert transport.send_request(_request("health", "health-1"), 1.0)["ok"] is True
        assert transport.send_request(_request("order", "order-1"), 1.0)["ok"] is True
        assert transport.send_request(_request("cancel", "cancel-1"), 1.0)["ok"] is True
        owner_ident = transport._client_thread.ident
    finally:
        transport.stop()
        router.stop()

    assert Counter(router.methods) == Counter({"health": 1, "order": 1, "cancel": 1})
    assert transport.client_socket_operation_threads
    assert set(transport.client_socket_operation_threads) == {owner_ident}
    assert owner_ident != caller_ident
    assert transport._dealer is None
    assert transport._client_thread is None


def test_zmq_client_queue_wait_is_inside_deadline_and_expired_request_is_not_sent():
    first_received = threading.Event()

    def handler(request):
        if request["method"] == "slow":
            first_received.set()
            time.sleep(0.2)
        return _response(request)

    router = LoopbackRouter(handler).start()
    transport = ZmqTransport(connect_address=router.address, account_id="fixture-only")
    first_result = {}

    def call_first():
        first_result["response"] = transport.send_request(
            _request("slow", "slow-1"), 0.8
        )

    first_thread = threading.Thread(target=call_first, name="test-first-rpc")
    try:
        first_thread.start()
        assert first_received.wait(1.0)
        started = time.monotonic()
        with pytest.raises(TransportTimeout):
            transport.send_request(_request("expired", "expired-1"), 0.05)
        elapsed = time.monotonic() - started
        first_thread.join(1.0)
        time.sleep(0.1)
    finally:
        transport.stop()
        router.stop()

    assert elapsed < 0.15
    assert first_result["response"]["request_id"] == "slow-1"
    assert router.methods == ["slow"]


def test_zmq_client_state_lock_wait_is_inside_deadline_and_does_not_send():
    router = LoopbackRouter(_response).start()
    transport = ZmqTransport(connect_address=router.address, account_id="fixture-only")
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_client_lock():
        with transport._client_state_lock:
            lock_held.set()
            release_lock.wait(1.0)

    holder = threading.Thread(target=hold_client_lock, name="test-client-lock-holder")
    try:
        holder.start()
        assert lock_held.wait(1.0)
        started = time.monotonic()
        with pytest.raises(TransportTimeout):
            transport.send_request(_request("expired-under-lock", "lock-1"), 0.05)
        elapsed = time.monotonic() - started
    finally:
        release_lock.set()
        holder.join(1.0)
        transport.stop()
        router.stop()

    assert elapsed < 0.15
    assert router.methods == []
    assert transport._client_thread is None


def test_zmq_client_rechecks_deadline_after_writable_poll_before_send():
    class Again(Exception):
        pass

    class Poller:
        def register(self, *_args):
            return None

        def poll(self, timeout):
            del timeout
            time.sleep(0.03)
            return [(dealer, 2)]

    class FakeZmq:
        POLLIN = 1
        POLLOUT = 2
        NOBLOCK = 1

        @staticmethod
        def Poller():
            return Poller()

    FakeZmq.Again = Again

    class Dealer:
        def __init__(self):
            self.send_count = 0
            self.close_lingers = []

        def send(self, *_args, **_kwargs):
            self.send_count += 1

        def close(self, *, linger):
            self.close_lingers.append(linger)

    dealer = Dealer()
    transport = ZmqTransport(account_id="fixture-only", client_linger_ms=500)
    transport._zmq = FakeZmq()
    transport._dealer = dealer
    transport._client_owner_ident = threading.get_ident()
    call = _ClientCall(_request("never-send", "never-send-1"), time.monotonic() + 0.01)

    with pytest.raises(TransportTimeout):
        transport._send_client_payload(dealer, b"payload", call, threading.Event())

    assert dealer.send_count == 0
    assert dealer.close_lingers == [0]
    assert transport._dealer is None


@pytest.mark.parametrize("failure_mode", ["late", "drop"])
def test_zmq_client_late_or_missing_response_is_discarded_and_next_request_recovers(
    failure_mode,
):
    def handler(request):
        if request["method"] == "late":
            time.sleep(0.15)
        if request["method"] == "drop":
            return None
        return _response(request)

    router = LoopbackRouter(handler).start()
    transport = ZmqTransport(connect_address=router.address, account_id="fixture-only")
    try:
        with pytest.raises(TransportTimeout):
            transport.send_request(_request(failure_mode, "%s-1" % failure_mode), 0.05)
        recovered = transport.send_request(_request("recover", "recover-1"), 1.0)
    finally:
        transport.stop()
        router.stop()

    assert recovered["request_id"] == "recover-1"
    assert recovered["data"] == {"method": "recover"}
    assert Counter(router.methods) == Counter({failure_mode: 1, "recover": 1})


@pytest.mark.parametrize("method", ["order", "cancel"])
def test_zmq_client_does_not_replay_timed_out_order_or_cancel(method):
    def handler(request):
        if request["method"] == method:
            return None
        return _response(request)

    router = LoopbackRouter(handler).start()
    transport = ZmqTransport(connect_address=router.address, account_id="fixture-only")
    try:
        with pytest.raises(TransportTimeout):
            transport.send_request(_request(method, "%s-timeout-1" % method), 0.05)
        recovered = transport.send_request(_request("health", "health-after-timeout"), 1.0)
    finally:
        transport.stop()
        router.stop()

    assert recovered["request_id"] == "health-after-timeout"
    assert Counter(router.methods) == Counter({method: 1, "health": 1})


def test_zmq_client_stop_ends_owner_thread_without_terminating_context_and_can_restart():
    hanging_received = threading.Event()

    def handler(request):
        if request["method"] == "hang":
            hanging_received.set()
            return None
        return _response(request)

    router = LoopbackRouter(handler).start()
    transport = ZmqTransport(connect_address=router.address, account_id="fixture-only")
    outcome = {}

    def call_hanging():
        try:
            transport.send_request(_request("hang", "hang-1"), 5.0)
        except Exception as exc:  # noqa: BLE001 - assertion records the transport outcome
            outcome["error"] = exc

    caller = threading.Thread(target=call_hanging, name="test-hanging-rpc")
    try:
        caller.start()
        assert hanging_received.wait(1.0)
        context = transport._ctx
        transport.stop()
        caller.join(1.0)

        assert not caller.is_alive()
        assert isinstance(outcome.get("error"), (TransportError, TransportTimeout))
        assert transport._dealer is None
        assert transport._client_thread is None
        assert not context.closed

        recovered = transport.send_request(_request("restart", "restart-1"), 1.0)
        assert recovered["request_id"] == "restart-1"
    finally:
        transport.stop()
        router.stop()

    assert Counter(router.methods) == Counter({"hang": 1, "restart": 1})
