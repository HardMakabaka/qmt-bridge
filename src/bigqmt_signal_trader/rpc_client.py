"""Direct Big QMT RPC client."""

from ._client_support import *
from .telemetry import bind_context, current_context, emit, inject_trace, span
from .transports.base import TransportError, TransportTimeout


class RpcRequestRejected(RuntimeError):
    """The embedded queue rejected this request before native execution."""

    def __init__(self, code, message, request_id=None):
        super().__init__(message)
        self.code = code
        self.request_id = request_id


class BigQmtRpcClient:
    def __init__(
        self,
        account_id=None,
        redis_client=None,
        redis_config=None,
        timeout_seconds=None,
        transport=None,
        transport_failure_callback=None,
    ):
        client_config = load_client_config()
        config_redis = dict(client_config.get("redis_config") or {})
        redis_config = dict(redis_config or {})
        merged_redis_config = dict(config_redis)
        merged_redis_config.update(redis_config)
        self.account_id = str(
            account_id
            or merged_redis_config.get("account_id")
            or client_config.get("account_id")
            or os.environ.get("BIGQMT_ACCOUNT_ID")
            or ""
        )
        self.redis_client = redis_client
        self.redis_config = {
            "host": merged_redis_config.get("host") or os.environ.get("BIGQMT_REDIS_HOST", "127.0.0.1"),
            "port": int(merged_redis_config.get("port") or _env_int("BIGQMT_REDIS_PORT", 6379)),
            "db": int(merged_redis_config.get("db") or _env_int("BIGQMT_REDIS_DB", 5)),
            "username": merged_redis_config.get("username", os.environ.get("BIGQMT_REDIS_USERNAME") or ""),
            "password": merged_redis_config.get("password", os.environ.get("BIGQMT_REDIS_PASSWORD") or ""),
        }
        config_timeout = client_config.get("timeout_seconds")
        self.timeout_seconds = float(
            timeout_seconds
            if timeout_seconds is not None
            else config_timeout
            if config_timeout is not None
            else _env_float("BIGQMT_RPC_TIMEOUT_SECONDS", 6.0)
        )
        full_tick_cache_config = dict(client_config.get("full_tick_cache_config") or {})
        self.full_tick_cache_config = {
            "enabled": _bool_value(
                full_tick_cache_config.get("enabled", full_tick_cache_config.get("full_tick_cache_enabled")),
                _env_bool("BIGQMT_FULL_TICK_CACHE_ENABLED", False),
            ),
            "demand_ttl_seconds": float(
                full_tick_cache_config.get("demand_ttl_seconds")
                or full_tick_cache_config.get("full_tick_demand_ttl_seconds")
                or _env_float("BIGQMT_FULL_TICK_DEMAND_TTL_SECONDS", 10.0)
            ),
            "cache_ttl_seconds": float(
                full_tick_cache_config.get("cache_ttl_seconds")
                or full_tick_cache_config.get("full_tick_cache_ttl_seconds")
                or _env_float("BIGQMT_FULL_TICK_CACHE_TTL_SECONDS", 10.0)
            ),
            "wait_seconds": float(
                full_tick_cache_config.get("wait_seconds")
                or full_tick_cache_config.get("full_tick_wait_seconds")
                or _env_float("BIGQMT_FULL_TICK_WAIT_SECONDS", 3.5)
            ),
            "poll_interval_seconds": float(
                full_tick_cache_config.get("poll_interval_seconds")
                or full_tick_cache_config.get("full_tick_poll_interval_seconds")
                or _env_float("BIGQMT_FULL_TICK_POLL_INTERVAL_SECONDS", 0.2)
            ),
        }
        # Client-side local market-data cache. download_history_data* pulls bars
        # over RPC once and persists them here; get_local_data then reads them with
        # no RPC. fallback_rpc=True lets get_local_data fetch+cache a cache miss.
        local_cache_config = dict(client_config.get("local_cache_config") or {})
        self.local_cache_config = {
            "enabled": _bool_value(
                local_cache_config.get("enabled", merged_redis_config.get("local_cache_enabled")),
                _env_bool("BIGQMT_LOCAL_CACHE_ENABLED", True),
            ),
            "dir": (
                local_cache_config.get("dir")
                or merged_redis_config.get("local_cache_dir")
                or os.environ.get("BIGQMT_LOCAL_CACHE_DIR")
                or None
            ),
            "fallback_rpc": _bool_value(
                local_cache_config.get("fallback_rpc", merged_redis_config.get("local_cache_fallback_rpc")),
                _env_bool("BIGQMT_LOCAL_CACHE_FALLBACK_RPC", False),
            ),
            "format": str(
                local_cache_config.get("format")
                or merged_redis_config.get("local_cache_format")
                or os.environ.get("BIGQMT_LOCAL_CACHE_FORMAT")
                or "auto"  # parquet if pyarrow is available, else pickle
            ),
        }
        # Transport selection. Default "redis" keeps the legacy call_redis_rpc
        # path (so existing client configs are unchanged). Setting transport to
        # "zmq"/"mysql"/"shm" (via config or constructor) routes calls through
        # the swappable transport layer instead.
        self.transport_name = str(
            transport
            or merged_redis_config.get("transport")
            or os.environ.get("BIGQMT_RPC_TRANSPORT")
            or "redis"
        ).lower()
        self.zmq_config = dict(merged_redis_config.get("zmq") or {})
        self.mysql_config = dict(merged_redis_config.get("mysql") or {})
        self.execution_event_config = dict(
            merged_redis_config.get("exec_events") or {}
        )
        self.execution_event_config.setdefault(
            "transport",
            "zmq" if self.transport_name == "zmq" else "redis",
        )
        self._transport_instance = None  # lazily built by _transport()
        # FormulaServer read fast-path. QMT's C++ quote service (port 58600)
        # answers reference/history reads in ~0.07ms without touching the QMT
        # python thread. Enabled by default; every miss falls back to RPC, so a
        # client that cannot reach it just runs as before.
        formula_config = dict(
            client_config.get("formula_server_config")
            or merged_redis_config.get("formula_server")
            or {}
        )
        if "enabled" not in formula_config:
            formula_config["enabled"] = _env_bool("BIGQMT_FORMULA_ENABLED", True)
        self.formula_server_config = formula_config
        self._formula_router_instance = None  # lazily built by _formula_router()
        self._quote_callbacks = {}
        self._quote_subscription_codes = {}
        self._quote_event_thread = None
        self._quote_event_running = False
        self._quote_event_count = 0
        self._quote_summary_at = time.monotonic()
        self._transport_failure_callback = transport_failure_callback

    def set_transport_failure_callback(self, callback):
        """Set the bridge-owned failure observer (or ``None`` on shutdown)."""
        self._transport_failure_callback = callback

    def _notify_transport_failure(self, exc):
        callback = self._transport_failure_callback
        if callable(callback):
            try:
                callback(exc)
            except Exception:
                # Lifecycle observation cannot alter native RPC semantics.
                pass

    def _redis(self):
        if self.redis_client is None:
            import redis

            cfg = dict(self.redis_config)
            if not cfg.get("username"):
                cfg.pop("username", None)
            if not cfg.get("password"):
                cfg.pop("password", None)
            self.redis_client = redis.Redis(**cfg)
        return self.redis_client

    def _transport(self):
        if self._transport_instance is None:
            if self.transport_name in ("redis", "", "default"):
                # Legacy path: call_redis_rpc builds its own request envelope.
                return None
            from .transports.factory import build_transport

            client_config = load_client_config()
            config_redis = dict(client_config.get("redis_config") or {})
            zmq_config = dict(config_redis.get("zmq") or {})
            zmq_config.update(self.zmq_config)
            # ZMQ must work without Redis. Discovery is opt-in and unnecessary
            # when connect_address is explicitly configured.
            if (
                not zmq_config.get("connect_address")
                and bool(zmq_config.get("redis_discovery_enabled", False))
            ):
                zmq_config.setdefault("discovery_redis_client", self._redis())
            factory_config = {
                "zmq": zmq_config,
                "mysql": dict(config_redis.get("mysql") or {}, **self.mysql_config),
            }
            self._transport_instance = build_transport(
                self.transport_name,
                factory_config,
                account_id=self.account_id,
                print_prefix="[bigqmt_client]",
            )
        return self._transport_instance

    def _formula_router(self):
        """Lazily build the FormulaServer router. Never raises — a router that
        cannot be built simply means every read goes over RPC."""
        if self._formula_router_instance is None:
            try:
                from .formula_server import build_router

                self._formula_router_instance = build_router(
                    self.formula_server_config, print_prefix="[bigqmt_formula]"
                )
            except Exception as exc:
                print("[bigqmt_formula] disabled (%s: %s)" % (exc.__class__.__name__, exc))

                class _Disabled(object):
                    def supports(self, method):
                        return False

                self._formula_router_instance = _Disabled()
        return self._formula_router_instance

    def call(self, method, params=None, account_id=None, timeout_seconds=None, force_rpc=False):
        target_account = str(account_id or self.account_id or "")
        if not target_account:
            raise ValueError("Big QMT account_id is required")
        rpc_request_id = uuid.uuid4().hex
        outcome = "unknown"
        trace_id = current_context().get("trace_id") or uuid.uuid4().hex
        with bind_context(trace_id=trace_id, rpc_request_id=rpc_request_id,
                          rpc_method=str(method)):
            emit("rpc.client.request.start", rpc_request_id=rpc_request_id,
                 method=str(method), transport=self.transport_name, critical=True)
            try:
                result = self._call_with_telemetry(
                    method, params, target_account, timeout_seconds, force_rpc,
                    rpc_request_id,
                )
                outcome = "success"
                return result
            except NotImplementedError:
                outcome = "unsupported"
                raise
            except (TimeoutError, TransportTimeout):
                outcome = "timeout"
                raise
            except (ConnectionError, OSError, TransportError):
                outcome = "unknown"
                raise
            except (PermissionError, ValueError):
                # These are explicit local/server validation rejections.  Do
                # not apply this label to generic RuntimeError: it can follow
                # an order submission or strict reconciliation attempt.
                outcome = "rejected"
                raise
            except Exception:
                outcome = "unknown"
                raise
            finally:
                emit("rpc.client.request.end", rpc_request_id=rpc_request_id,
                     method=str(method), transport=self.transport_name,
                     outcome=outcome, critical=True)

    def _call_with_telemetry(self, method, params, target_account, timeout_seconds,
                             force_rpc, rpc_request_id):
        wait_seconds = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        # A caller may already have spent part of its request budget waiting
        # for a shared read lane. Do not start a fresh full RPC timeout here.
        try:
            from .request_budget import current_deadline

            deadline = current_deadline()
        except ImportError:
            deadline = None
        def refresh_budget_timeout():
            if deadline is None:
                return float(wait_seconds)
            remaining = max(0.0, float(deadline) - time.monotonic())
            if remaining <= 0.0:
                raise TimeoutError("Big QMT request deadline exhausted before RPC: %s" % method)
            return min(float(wait_seconds), remaining)

        wait_seconds = refresh_budget_timeout()
        # Fast path: reference/history reads answered straight by QMT's
        # FormulaServer, bypassing the strategy process and its GIL. Anything it
        # declines (unmapped method, untranslatable params, server down) raises
        # Unroutable and drops through to the RPC bridge below.
        router = None if force_rpc else self._formula_router()
        if router is not None and router.supports(method):
            from .formula_server import Unroutable

            try:
                emit("formula.route.selected", rpc_request_id=rpc_request_id,
                     method=str(method), outcome="started")
                with span("formula.route", rpc_request_id=rpc_request_id,
                          method=str(method)) as observed:
                    result = _restore_jsonable(
                        router.call(method, params or {}, deadline_monotonic=deadline)
                    )
                    observed["outcome"] = "success"
                    emit("formula.route.end", rpc_request_id=rpc_request_id,
                         method=str(method), outcome="success")
                    return result
            except Unroutable as exc:
                emit("formula.route.fallback", rpc_request_id=rpc_request_id,
                     method=str(method), outcome="fallback",
                     reason_type=type(exc).__name__)
        # FormulaServer may have waited for its own socket/worker lock before
        # declining the request. Recompute rather than reusing the pre-fastpath
        # value, so the RPC fallback cannot exceed the original request budget.
        wait_seconds = refresh_budget_timeout()
        transport = self._transport()
        if transport is not None:
            # Swappable transport path (zmq/mysql/...). Build the request
            # envelope the same way call_redis_rpc does.
            request = inject_trace({
                "schema_version": 1,
                "request_id": rpc_request_id,
                "account_id": target_account,
                "method": method,
                "params": params or {},
                "ttl_seconds": 60,
            })
            try:
                # This boundary enters the selected transport; it is not a
                # ZMQ queue dequeue (Redis/MySQL have different mechanics).
                emit("rpc.client.transport.dispatch", rpc_request_id=rpc_request_id,
                     method=str(method), transport=getattr(transport, "name", self.transport_name))
                with span("rpc.client.wire", rpc_request_id=rpc_request_id,
                          method=str(method), transport=getattr(transport, "name", self.transport_name),
                          duration_scope="transport_round_trip_including_queue") as observed:
                    emit("rpc.client.wire.send", rpc_request_id=rpc_request_id,
                         method=str(method), transport=getattr(transport, "name", self.transport_name))
                    response = transport.send_request(request, wait_seconds)
                    observed["outcome"] = "response_received"
                    emit("rpc.client.wire.receive", rpc_request_id=rpc_request_id,
                         method=str(method), transport=getattr(transport, "name", self.transport_name),
                         response_ok=bool(isinstance(response, dict) and response.get("ok")))
            except (ConnectionError, OSError, TimeoutError) as exc:
                self._notify_transport_failure(exc)
                raise
        else:
            try:
                with span("rpc.client.wire", rpc_request_id=rpc_request_id,
                          method=str(method), transport="redis",
                          duration_scope="transport_round_trip_including_queue") as observed:
                    emit("rpc.client.wire.send", rpc_request_id=rpc_request_id,
                         method=str(method), transport="redis")
                    response = call_redis_rpc(
                        self._redis(), account_id=target_account, method=method,
                        params=params or {}, timeout_seconds=wait_seconds,
                        request_id=rpc_request_id,
                        trace=inject_trace({}).get("trace"),
                    )
                    observed["outcome"] = "response_received"
                    emit("rpc.client.wire.receive", rpc_request_id=rpc_request_id,
                         method=str(method), transport="redis",
                         response_ok=bool(isinstance(response, dict) and response.get("ok")))
            except (ConnectionError, OSError, TimeoutError) as exc:
                self._notify_transport_failure(exc)
                raise
        if not response.get("ok"):
            error = response.get("error") or "Big QMT RPC failed: %s" % method
            error_type = str(response.get("error_type") or "")
            if error_type in ("OVERLOADED", "DEADLINE_EXCEEDED"):
                raise RpcRequestRejected(error_type, error, response.get("request_id"))
            if error_type in ("NotImplementedError", "AttributeError"):
                raise NotImplementedError(error)
            if error_type in ("TimeoutError", "TransportTimeout"):
                exc = TimeoutError(error)
                self._notify_transport_failure(exc)
                raise exc
            if error_type in ("ConnectionError", "OSError", "TransportError"):
                exc = OSError(error)
                self._notify_transport_failure(exc)
                raise exc
            raise RuntimeError(error)
        return _restore_jsonable(response.get("data"))

    def publish_event(self, event_type, payload, stream_template="bigqmt:quote_events:{account_id}"):
        account_id = str(self.account_id or "")
        event = {
            "event_type": str(event_type),
            "account_id": account_id,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "payload": payload or {},
        }
        raw = json.dumps(event, ensure_ascii=False, default=str)
        stream_key = stream_template.format(account_id=account_id)
        redis_client = self._redis()
        try:
            redis_client.xadd(stream_key, {"payload": raw}, maxlen=1000, approximate=True)
        except Exception:
            pass
        try:
            redis_client.publish(stream_key, raw)
        except Exception:
            pass
        return event

    def save_quote_subscription(self, seq, payload, active=True):
        account_id = str(self.account_id or "")
        key = "bigqmt:quote_subscriptions:%s" % account_id
        redis_client = self._redis()
        if active:
            value = json.dumps(payload or {}, ensure_ascii=False, default=str)
            try:
                redis_client.hset(key, str(seq), value)
            except Exception:
                pass
        else:
            try:
                redis_client.hdel(key, str(seq))
            except Exception:
                pass

    def register_quote_callback(self, seq, code, callback):
        seq = int(seq)
        code = str(code).upper()
        self._quote_callbacks[seq] = callback
        self._quote_subscription_codes[seq] = code
        self._start_quote_event_listener()

    def unregister_quote_callback(self, seq):
        code = self._quote_subscription_codes.pop(int(seq), None)
        if code is not None:
            self._quote_callbacks.pop(int(seq), None)

    def _start_quote_event_listener(self):
        if self._quote_event_thread is not None and self._quote_event_thread.is_alive():
            return
        self._quote_event_running = True
        self._quote_event_thread = threading.Thread(
            target=self._quote_event_loop,
            name="bigqmt-quote-events",
            daemon=True,
        )
        self._quote_event_thread.start()
        emit("quote.listener.started", transport="zmq", outcome="success")

    def _quote_event_loop(self):
        config = dict(self.execution_event_config.get("zmq") or {})
        address = str(
            config.get("connect_address") or "tcp://127.0.0.1:15561"
        )
        socket = None
        try:
            import zmq

            socket = zmq.Context.instance().socket(zmq.SUB)
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.RCVTIMEO, 250)
            socket.setsockopt(zmq.RCVHWM, 1000)
            socket.setsockopt(zmq.SUBSCRIBE, b"")
            socket.connect(address)
            while self._quote_event_running:
                try:
                    event = socket.recv_json()
                except zmq.Again:
                    continue
                if not isinstance(event, dict) or event.get("event_type") != "quote":
                    continue
                self._quote_event_count += 1
                now = time.monotonic()
                if self._quote_event_count % 100 == 0 or now - self._quote_summary_at >= 30.0:
                    emit("quote.listener.summary", transport="zmq", outcome="success",
                         event_count=self._quote_event_count,
                         subscription_count=len(self._quote_subscription_codes))
                    self._quote_summary_at = now
                code = str(event.get("code") or "").upper()
                callbacks = [
                    self._quote_callbacks.get(seq)
                    for seq, subscribed_code in list(self._quote_subscription_codes.items())
                    if subscribed_code == code
                ]
                for callback in callbacks:
                    if callback is None:
                        continue
                    try:
                        callback(event)
                    except Exception as exc:
                        emit("quote.listener.callback_error", outcome="unknown",
                             error_type=type(exc).__name__)
                        pass
        except Exception as exc:
            emit("quote.listener.failed", critical=True, outcome="unknown",
                 error_type=type(exc).__name__)
            return
        finally:
            if socket is not None:
                socket.close(linger=0)

    def stop_quote_event_listener(self):
        self._quote_event_running = False
        thread = self._quote_event_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        self._quote_event_thread = None
        emit("quote.listener.stopped", transport="zmq", outcome="success",
             event_count=self._quote_event_count)
