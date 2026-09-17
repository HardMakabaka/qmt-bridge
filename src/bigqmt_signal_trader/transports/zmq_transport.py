"""ZeroMQ transport for the BigQMT RPC bridge.

Designed for same-host low latency. Topology:

* **Server** binds a ``ROUTER`` socket. Each inbound message arrives as
  ``[identity, payload]``; the server remembers ``identity`` keyed by
  ``request_id`` and replies with ``[identity, payload]`` so ZMQ routes the
  response back to the originating client automatically.
* **Client** connects a ``DEALER`` socket (with a unique random identity), sends
  ``[payload]``, then ``poll``/``recv`` for the response. DEALER gives each
  client an asymmetric async path that pairs naturally with ROUTER.

Wire framing is a single JSON payload per message. The original b64 stock-code
obfuscation (``encode_rpc_request_payload``) is applied too, so payloads stay
opaque even though ZMQ does not need it — keeps the wire uniform with Redis.

The client uses one dedicated I/O owner thread. Public callers enqueue a
single-shot exchange with an absolute monotonic deadline; only that owner
thread creates, uses, rebuilds, and closes the DEALER socket.
"""

import json
import queue
import threading
import time
import uuid

from ..adapters.redis_common import decode_text
from ..redis_rpc import (
    decode_rpc_request_payload,
    encode_rpc_request_payload,
)
from ..telemetry import bind_context, current_context, emit, extract_trace, span
from .base import RpcTransport, TransportError, TransportTimeout


# ZMQ does not support ipc:// on Windows (it trips a signaler abort), so the
# default endpoint is tcp loopback. The port is derived from the account_id so
# distinct accounts don't collide on the same port; override via config when
# needed. Base 15560 keeps it clear of common dev ports.
DEFAULT_ZMQ_HOST = "127.0.0.1"
DEFAULT_ZMQ_BASE_PORT = 15560
DEFAULT_ZMQ_PORT_RANGE = 100  # derived port = base + (account_id_int mod range)
_CLIENT_STOP = object()


class _ClientCall(object):
    def __init__(self, request, deadline):
        self.request = request
        self.deadline = float(deadline)
        self.event = threading.Event()
        self.response = None
        self.error = None
        self._lock = threading.Lock()
        self._cancelled = False
        self._done = False
        self.trace_context = current_context()

    def cancel(self):
        with self._lock:
            if not self._done:
                self._cancelled = True

    def is_cancelled(self):
        with self._lock:
            return self._cancelled

    def complete(self, response=None, error=None):
        with self._lock:
            if self._done or self._cancelled:
                return False
            self.response = response
            self.error = error
            self._done = True
            self.event.set()
            return True


def _default_zmq_port(account_id):
    """Derive a stable port from account_id so each account gets its own socket."""
    text = str(account_id or "")
    digits = "".join(ch for ch in text if ch.isdigit())
    try:
        offset = int(digits) % DEFAULT_ZMQ_PORT_RANGE if digits else 0
    except ValueError:
        offset = 0
    return DEFAULT_ZMQ_BASE_PORT + offset


def _default_zmq_address(account_id, host=None):
    host = host or DEFAULT_ZMQ_HOST
    return "tcp://%s:%d" % (host, _default_zmq_port(account_id))


def _loads(raw):
    if isinstance(raw, dict):
        return dict(raw)
    text = decode_text(raw)
    text = decode_rpc_request_payload(text)
    return json.loads(text)


class ZmqTransport(RpcTransport):
    """ZMQ ROUTER/DEALER transport.

    The same instance plays both roles depending on method called:
    ``send_request`` acts as a client (DEALER connect), ``start_receiving`` +
    ``send_response`` act as a server (ROUTER bind). A deployment normally uses
    one instance per role (the QMT process is the server; the external client
    is the client).
    """

    name = "zmq"

    def __init__(
        self,
        bind_address=None,
        connect_address=None,
        host=None,
        port=None,
        account_id="",
        print_prefix="[bigqmt_rpc]",
        io_threads=1,
        recv_timeout_seconds=1.0,
        server_hwm=10000,
        client_linger_ms=0,
        discovery_redis_client=None,
        discovery_key_template="bigqmt:zmq:addr:{account_id}",
        discovery_ttl_seconds=300,
        port_scan_range=50,
    ):
        super(ZmqTransport, self).__init__(account_id=account_id, print_prefix=print_prefix)
        # Address resolution order: explicit bind_address/connect_address win;
        # otherwise build tcp://host:port from host/port (port defaults to a
        # value derived from account_id so distinct accounts don't collide).
        resolved_host = host or DEFAULT_ZMQ_HOST
        if port is not None:
            resolved_port = int(port)
        else:
            resolved_port = _default_zmq_port(account_id)
        default_addr = "tcp://%s:%d" % (resolved_host, resolved_port)
        self.bind_address = bind_address or default_addr
        self.connect_address = connect_address
        self.bind_host = resolved_host
        self.base_port = resolved_port
        self.io_threads = int(io_threads)
        self.recv_timeout_seconds = float(recv_timeout_seconds)
        self.server_hwm = int(server_hwm)
        self.client_linger_ms = int(client_linger_ms)
        # Discovery remains available for clients, but a server must bind the
        # configured address exactly. ``port_scan_range`` is retained only for
        # backward-compatible config loading and is intentionally not used.
        self.discovery_redis_client = discovery_redis_client
        self.discovery_key_template = discovery_key_template
        self.discovery_ttl_seconds = int(discovery_ttl_seconds)
        self.port_scan_range = int(port_scan_range)

        self._zmq = None  # imported lazily
        self._ctx = None
        # server state
        self._router = None
        self._router_thread = None
        self._actual_bind_address = None  # set after start_receiving()
        self._pending_identities = {}  # request_id -> client identity bytes
        self._identity_lock = threading.Lock()
        self._response_queue = queue.Queue()
        self._queued_response_count = 0
        self._sent_response_count = 0
        # client state
        self._dealer = None
        self._client_state_lock = threading.Lock()
        self._client_thread = None
        self._client_queue = None
        self._client_stop_event = None
        self._client_stopping = False
        self._client_owner_ident = None

    # -- construction helper ----------------------------------------------
    @classmethod
    def from_config(cls, config, account_id="", print_prefix="[bigqmt_rpc]"):
        config = dict(config or {})
        return cls(
            bind_address=config.get("bind_address"),
            connect_address=config.get("connect_address"),
            host=config.get("host"),
            port=config.get("port"),
            account_id=config.get("account_id", account_id),
            print_prefix=print_prefix,
            io_threads=int(config.get("io_threads", 1)),
            recv_timeout_seconds=float(config.get("recv_timeout_seconds", 1.0)),
            server_hwm=int(config.get("server_hwm", 10000)),
            client_linger_ms=int(config.get("client_linger_ms", 0)),
            discovery_redis_client=config.get("discovery_redis_client"),
            discovery_key_template=config.get(
                "discovery_key_template", "bigqmt:zmq:addr:{account_id}"
            ),
            discovery_ttl_seconds=int(config.get("discovery_ttl_seconds", 300)),
            port_scan_range=int(config.get("port_scan_range", 50)),
        )

    # -- shared zmq context -----------------------------------------------
    def _ensure_zmq(self):
        if self._zmq is None:
            try:
                import zmq  # noqa: F401
            except ImportError as exc:  # pragma: no cover - depends on env
                raise TransportError(
                    "pyzmq is required for the zmq transport: %s" % exc
                )
            self._zmq = zmq
        if self._ctx is None:
            self._ctx = self._zmq.Context.instance(self.io_threads)
        return self._zmq, self._ctx

    # -- server side ------------------------------------------------------
    def _bind_configured_address(self):
        """Bind exactly one configured address and reject duplicate servers."""
        zmq, ctx = self._ensure_zmq()
        sock = ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.RCVHWM, self.server_hwm)
        sock.setsockopt(zmq.SNDHWM, self.server_hwm)
        sock.setsockopt(zmq.RCVTIMEO, int(self.recv_timeout_seconds * 1000))
        try:
            sock.bind(self.bind_address)
        except self._zmq.ZMQError as exc:
            try:
                sock.close(linger=0)
            except Exception:
                pass
            if getattr(exc, "errno", None) == zmq.EADDRINUSE:
                # 端口被占——通常是之前策略实例没正常停止。给出友好提示和解决步骤。
                print(
                    "%s ZMQ_BIND_CONFLICT: 端口 %s 被占用！"
                    % (self.print_prefix, self.bind_address)
                )
                print(
                    "%s   原因：之前的 QMT 策略实例没正常停止，仍占着这个端口。"
                    % self.print_prefix
                )
                print(
                    "%s   解决：1) 在 QMT 里停止旧策略再运行；2) 或等 60s 让系统释放端口；"
                    % self.print_prefix
                )
                print(
                    "%s   3) 或改配置用别的端口（BIGQMT_REDIS_CONFIG.zmq.port）"
                    % self.print_prefix
                )
                raise TransportError(
                    "ZMQ_BIND_CONFLICT address=%s; another bridge instance "
                    "already owns the configured endpoint" % self.bind_address
                )
            raise
        self._router = sock
        self._actual_bind_address = self.bind_address
        self._publish_discovery(self.bind_address)

    def _publish_discovery(self, address):
        if self.discovery_redis_client is None:
            return
        key = self.discovery_key_template.format(account_id=self.account_id)
        try:
            self.discovery_redis_client.setex(
                key, self.discovery_ttl_seconds, address
            )
        except Exception as exc:
            print("%s zmq discovery publish failed: %s" % (self.print_prefix, exc))

    def _clear_discovery(self):
        if self.discovery_redis_client is None:
            return
        key = self.discovery_key_template.format(account_id=self.account_id)
        try:
            self.discovery_redis_client.delete(key)
        except Exception:
            pass

    def start_receiving(self, on_request, background_threads=True):
        super(ZmqTransport, self).start_receiving(on_request)
        zmq, ctx = self._ensure_zmq()
        self._bind_configured_address()
        bound = self._actual_bind_address or self.bind_address
        if not background_threads:
            print(
                "%s zmq bound=%s background_threads=False"
                % (self.print_prefix, bound)
            )
            return
        self._router_thread = threading.Thread(
            target=self._router_loop, name="bigqmt-zmq-rpc", daemon=True
        )
        self._router_thread.start()
        print(
            "%s zmq started bound=%s" % (self.print_prefix, self.bind_address)
        )

    def _router_loop(self):
        try:
            while self._running:
                self._drain_response_queue()
                request = self._receive_request()
                if request is not None:
                    self._deliver_request(request)
        finally:
            # Close the ROUTER socket on the thread that owns it. On Windows,
            # closing a ZMQ socket from a different thread trips a signaler
            # assertion (abort); closing it here is safe because this thread
            # created and exclusively used it.
            try:
                self._router.close(linger=0)
            except Exception:
                pass
            self._router = None

    def _receive_request(self, flags=0):
        try:
            frames = self._router.recv_multipart(flags=flags)
        except self._zmq.Again:
            return None
        except Exception as exc:
            if self._running:
                print("%s zmq recv failed: %s" % (self.print_prefix, exc))
                if not flags:
                    time.sleep(0.5)
            return None
        if len(frames) < 2:
            return None
        identity, payload = frames[0], frames[-1]
        try:
            request = _loads(payload)
        except Exception as exc:
            print("%s zmq decode failed: %s" % (self.print_prefix, exc))
            return None
        request_id = str(request.get("request_id") or uuid.uuid4().hex)
        with self._identity_lock:
            self._pending_identities[request_id] = identity
        trace = extract_trace(request)
        emit("rpc.transport.zmq.server.receive", rpc_request_id=request_id,
             method=str(request.get("method") or ""), outcome="success",
             has_trace=bool(trace))
        return request

    def _deliver_request(self, request):
        started = time.perf_counter()
        try:
            with bind_context(**extract_trace(request)):
                with span("rpc.transport.zmq.server.dispatch",
                          rpc_request_id=str(request.get("request_id") or ""),
                          method=str(request.get("method") or "")) as observed:
                    self.deliver(request)
                    observed["outcome"] = "success"
        except Exception as exc:
            print("%s zmq deliver failed: %s" % (self.print_prefix, exc))
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if elapsed_ms > 50.0:
            print("%s zmq slow handler method=%s %.0fms"
                  % (self.print_prefix, request.get("method"), elapsed_ms))

    def _drain_response_queue(self):
        while True:
            try:
                identity, payload = self._response_queue.get_nowait()
            except queue.Empty:
                return
            try:
                self._router.send_multipart([identity, payload])
                self._sent_response_count += 1
                if self._sent_response_count <= 5:
                    print("%s zmq queued response sent" % self.print_prefix)
            except Exception as exc:
                print("%s zmq send failed: %s" % (self.print_prefix, exc))

    def send_response(self, request, response):
        if self._router is None:
            raise TransportError("zmq server socket is not bound")
        request_id = str(
            response.get("request_id") or request.get("request_id") or ""
        )
        with self._identity_lock:
            identity = self._pending_identities.pop(request_id, None)
        if identity is None:
            # No matching peer — drop silently (client may have gone away).
            return
        payload = encode_rpc_request_payload(response).encode("utf-8")
        if self._router_thread is not None and threading.current_thread() is not self._router_thread:
            self._queued_response_count += 1
            if self._queued_response_count <= 5:
                print("%s zmq response queued for router thread" % self.print_prefix)
            self._response_queue.put((identity, payload))
            return
        try:
            self._router.send_multipart([identity, payload])
        except Exception as exc:
            print("%s zmq send failed: %s" % (self.print_prefix, exc))

    def drain_request_queue(self, max_items=20):
        """Drain requests from the scheduled QMT thread when no receiver thread exists."""
        if self._router_thread is not None or self._router is None:
            return 0
        processed = 0
        for _index in range(max(int(max_items), 0)):
            request = self._receive_request(flags=self._zmq.NOBLOCK)
            if request is None:
                break
            self._deliver_request(request)
            processed += 1
        return processed

    # -- client side ------------------------------------------------------
    def _resolve_connect_address(self):
        """Resolve the address to connect to.

        Order: explicit connect_address > discovery lookup > default derived.
        Discovery lets the client find a server that had to move off the
        default port because of a collision.
        """
        if self.connect_address:
            return self.connect_address
        discovered = self._lookup_discovery()
        if discovered:
            return discovered
        return _default_zmq_address(self.account_id)

    def _lookup_discovery(self):
        if self.discovery_redis_client is None:
            return None
        key = self.discovery_key_template.format(account_id=self.account_id)
        try:
            raw = self.discovery_redis_client.get(key)
        except Exception:
            return None
        if not raw:
            return None
        try:
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            return None
        return text or None

    def _ensure_dealer(self):
        self._assert_client_owner()
        zmq, ctx = self._ensure_zmq()
        if self._dealer is None:
            address = self._resolve_connect_address()
            sock = ctx.socket(zmq.DEALER)
            try:
                # Unique identity so ROUTER can route replies back to us.
                sock.setsockopt(zmq.IDENTITY, uuid.uuid4().hex.encode("utf-8")[:16])
                sock.setsockopt(zmq.LINGER, self.client_linger_ms)
                sock.connect(address)
            except Exception:
                try:
                    sock.close(linger=0)
                except Exception:
                    pass
                raise
            self._dealer = sock
            self.connect_address = address
        return self._dealer

    def _assert_client_owner(self):
        if self._client_owner_ident != threading.get_ident():
            raise TransportError("zmq DEALER socket operation must run on its I/O owner thread")

    def _reset_dealer(self, discard_pending=False):
        self._assert_client_owner()
        dealer = self._dealer
        self._dealer = None
        if dealer is not None:
            try:
                dealer.close(
                    linger=0 if discard_pending else self.client_linger_ms
                )
            except Exception:
                pass

    def _enqueue_client_call(self, call):
        remaining = call.deadline - time.monotonic()
        if remaining <= 0 or not self._client_state_lock.acquire(timeout=remaining):
            raise self._client_timeout(call.request)
        try:
            thread = self._client_thread
            if self._client_stopping:
                raise TransportError("zmq client is stopping")
            if time.monotonic() >= call.deadline:
                raise self._client_timeout(call.request)
            if thread is None or not thread.is_alive():
                client_queue = queue.Queue()
                stop_event = threading.Event()
                thread = threading.Thread(
                    target=self._client_io_loop,
                    args=(client_queue, stop_event),
                    name="bigqmt-zmq-client",
                    daemon=True,
                )
                self._client_queue = client_queue
                self._client_stop_event = stop_event
                self._client_thread = thread
                thread.start()
            self._client_queue.put_nowait(call)
            emit("rpc.transport.zmq.client.queued",
                 rpc_request_id=str(call.request.get("request_id") or ""),
                 method=str(call.request.get("method") or ""),
                 queue_depth=self._client_queue.qsize())
        finally:
            self._client_state_lock.release()

    def _client_io_loop(self, client_queue, stop_event):
        self._client_owner_ident = threading.get_ident()
        try:
            while not stop_event.is_set():
                try:
                    call = client_queue.get(timeout=0.05)
                except queue.Empty:
                    continue
                if call is _CLIENT_STOP:
                    break
                with bind_context(**call.trace_context):
                    emit("rpc.transport.zmq.client.dequeued",
                         rpc_request_id=str(call.request.get("request_id") or ""),
                         method=str(call.request.get("method") or ""),
                         queue_depth=client_queue.qsize())
                    if call.is_cancelled() or time.monotonic() >= call.deadline:
                        emit("rpc.transport.zmq.client.expired", critical=True,
                             rpc_request_id=str(call.request.get("request_id") or ""),
                             method=str(call.request.get("method") or ""), outcome="timeout")
                        call.complete(error=self._client_timeout(call.request))
                        continue
                    try:
                        response = self._exchange_client_call(call, stop_event)
                    except Exception as exc:
                        emit("rpc.transport.zmq.client.exchange_failed", critical=True,
                             rpc_request_id=str(call.request.get("request_id") or ""),
                             method=str(call.request.get("method") or ""), outcome="timeout"
                             if isinstance(exc, TransportTimeout) else "unknown",
                             error_type=type(exc).__name__)
                        call.complete(error=exc)
                    else:
                        emit("rpc.transport.zmq.client.response",
                             rpc_request_id=str(call.request.get("request_id") or ""),
                             method=str(call.request.get("method") or ""),
                             outcome="success" if response.get("ok") else "rejected")
                        call.complete(response=response)
        finally:
            self._reset_dealer()
            while True:
                try:
                    call = client_queue.get_nowait()
                except queue.Empty:
                    break
                if call is not _CLIENT_STOP:
                    call.complete(error=TransportError("zmq client stopped"))
            self._client_owner_ident = None

    def _exchange_client_call(self, call, stop_event):
        self._assert_client_owner()
        request = call.request
        request_id = request["request_id"]
        payload = encode_rpc_request_payload(request).encode("utf-8")
        dealer = self._ensure_dealer()
        if self._client_call_expired(call, stop_event):
            self._reset_dealer(discard_pending=True)
            raise self._client_timeout(request)
        try:
            emit("rpc.transport.zmq.client.wire_send", rpc_request_id=request_id,
                 method=str(request.get("method") or ""))
            self._send_client_payload(dealer, payload, call, stop_event)
            poller = self._zmq.Poller()
            poller.register(dealer, self._zmq.POLLIN)
            while True:
                if self._client_call_expired(call, stop_event):
                    self._reset_dealer(discard_pending=True)
                    raise self._client_timeout(request)
                remaining = call.deadline - time.monotonic()
                events = dict(poller.poll(timeout=max(1, min(int(remaining * 1000), 50))))
                if not (events.get(dealer, 0) & self._zmq.POLLIN):
                    continue
                self._assert_client_owner()
                frames = dealer.recv_multipart(flags=self._zmq.NOBLOCK)
                response = _loads(frames[-1])
                if time.monotonic() >= call.deadline or call.is_cancelled():
                    self._reset_dealer(discard_pending=True)
                    raise self._client_timeout(request)
                if response.get("request_id") == request_id:
                    emit("rpc.transport.zmq.client.wire_receive", rpc_request_id=request_id,
                         method=str(request.get("method") or ""),
                         response_ok=bool(response.get("ok")))
                    return response
        except (TransportError, TransportTimeout):
            raise
        except self._zmq.Again:
            self._reset_dealer(discard_pending=True)
            raise self._client_timeout(request)
        except Exception as exc:
            self._reset_dealer(discard_pending=True)
            raise TransportError("zmq client exchange failed: %s" % exc)

    def _send_client_payload(self, dealer, payload, call, stop_event):
        self._assert_client_owner()
        poller = self._zmq.Poller()
        poller.register(dealer, self._zmq.POLLOUT)
        while True:
            if self._client_call_expired(call, stop_event):
                self._reset_dealer(discard_pending=True)
                raise self._client_timeout(call.request)
            remaining = call.deadline - time.monotonic()
            events = dict(poller.poll(timeout=max(1, min(int(remaining * 1000), 50))))
            if not (events.get(dealer, 0) & self._zmq.POLLOUT):
                continue
            if self._client_call_expired(call, stop_event):
                self._reset_dealer(discard_pending=True)
                raise self._client_timeout(call.request)
            try:
                self._assert_client_owner()
                dealer.send(payload, flags=self._zmq.NOBLOCK)
                return
            except self._zmq.Again:
                continue
            except Exception as exc:
                self._reset_dealer(discard_pending=True)
                raise TransportError("zmq send failed: %s" % exc)

    @staticmethod
    def _client_call_expired(call, stop_event):
        return (
            stop_event.is_set()
            or call.is_cancelled()
            or time.monotonic() >= call.deadline
        )

    @staticmethod
    def _client_timeout(request):
        return TransportTimeout("zmq rpc timeout: %s" % request.get("method"))

    def send_request(self, request, timeout_seconds, **_kwargs):
        timeout_seconds = max(float(timeout_seconds), 0.0)
        deadline = time.monotonic() + timeout_seconds
        request = dict(request)
        request.setdefault("request_id", uuid.uuid4().hex)
        # The server cannot compare our monotonic clock, so include an
        # additive absolute deadline.  Its deferred worker will reject a
        # request that waited past the caller's budget before native/QMT work.
        request.setdefault(
            "deadline_epoch_ms", int((time.time() + timeout_seconds) * 1000)
        )
        call = _ClientCall(request, deadline)
        if time.monotonic() >= deadline:
            raise self._client_timeout(request)
        self._enqueue_client_call(call)
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not call.event.wait(remaining):
            call.cancel()
            raise self._client_timeout(request)
        if call.error is not None:
            raise call.error
        return call.response

    # -- lifecycle --------------------------------------------------------
    def stop(self):
        super(ZmqTransport, self).stop()
        # Clear _running so the router loop exits; the loop closes its own
        # socket (closing cross-thread trips a Windows signaler abort).
        thread = self._router_thread
        if thread is not None and thread.is_alive():
            thread.join(2.0)
        if thread is None and self._router is not None:
            try:
                self._router.close(linger=0)
            except Exception:
                pass
            self._router = None
        self._router_thread = None
        # If we were a server that published a discovery address, clear it so
        # clients don't keep hitting a dead endpoint.
        if self._actual_bind_address is not None:
            self._clear_discovery()
            self._actual_bind_address = None
        with self._client_state_lock:
            client_thread = self._client_thread
            client_queue = self._client_queue
            client_stop_event = self._client_stop_event
            if client_thread is not None and client_thread.is_alive():
                self._client_stopping = True
                client_stop_event.set()
                client_queue.put_nowait(_CLIENT_STOP)
        if client_thread is not None and client_thread.is_alive():
            client_thread.join(2.0)
        if client_thread is not None and client_thread.is_alive():
            raise TransportError("zmq client I/O thread did not stop")
        with self._client_state_lock:
            if self._client_thread is client_thread:
                self._client_thread = None
                self._client_queue = None
                self._client_stop_event = None
            self._client_stopping = False
        # Do NOT terminate the shared context — other sockets/users may rely on it.
