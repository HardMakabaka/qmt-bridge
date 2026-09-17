"""Lifecycle owner for the single Big QMT runtime.

The controller deliberately only reconnects transport failures.  It never
replays broker writes: a recovered runtime is merely made available to the
normal request path.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from collections.abc import Callable
from typing import Any
from bigqmt_signal_trader import telemetry

from .bigqmt import (
    BigQmtConfigurationError,
    discard_bigqmt_runtime,
    initialize_bigqmt_runtime,
    reset_bigqmt_runtime,
)
from .config import Settings

logger = logging.getLogger("qmt_bridge.runtime")

_RETRY_DELAYS = (1, 2, 5, 10, 30)


class RuntimeController:
    """Own exactly one runtime and one best-effort recovery task."""

    def __init__(
        self,
        settings: Settings,
        *,
        runtime_factory: Callable[[Settings], Any] = initialize_bigqmt_runtime,
        manager_factory: Callable[..., Any] | None = None,
        download_start: Callable[[], Any] | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
        state: Any | None = None,
    ) -> None:
        self.settings = settings
        self._runtime_factory = runtime_factory
        self._uses_global_runtime = runtime_factory is initialize_bigqmt_runtime
        self._manager_factory = manager_factory
        self._download_start = download_start
        self._sleep = sleep
        self._state = state
        self.runtime: Any | None = None
        self.trader_manager: Any | None = None
        self.last_error: str | None = None
        self.configuration_error = False
        self._closed = False
        self._recovery_task: asyncio.Task[None] | None = None
        self._connect_lock = asyncio.Lock()
        self._inflight_connect: asyncio.Task[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._needs_hub_invalidation = False
        self._disposed_runtime_ids: set[int] = set()
        self.generation = 0
        self._connect_attempts = 0

    @property
    def recovering(self) -> bool:
        return self._recovery_task is not None and not self._recovery_task.done()

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        await self._connect_once()
        if self.runtime is None and not self.configuration_error:
            self._ensure_recovery()

    async def _connect_once(self) -> bool:
        self._connect_attempts += 1
        with telemetry.span("runtime.connect", attempt=self._connect_attempts,
                            runtime_generation=self.generation) as observation:
            connected = await self._connect_once_observed()
            observation.update(outcome="success" if connected else "unavailable",
                               configuration_error=self.configuration_error,
                               runtime_generation=self.generation)
            return connected

    async def _connect_once_observed(self) -> bool:
        if self._closed:
            return False
        async with self._connect_lock:
            if self.runtime is not None:
                return True
            runtime = None
            manager = None
            try:
                if self._needs_hub_invalidation:
                    await self._invalidate_realtime_hub()
                with telemetry.span("runtime.factory"):
                    connect_task = asyncio.create_task(
                        asyncio.to_thread(self._runtime_factory, self.settings),
                        name="qmt-bridge-connect",
                    )
                    self._inflight_connect = connect_task
                    runtime = await asyncio.shield(connect_task)
                self._inflight_connect = None
                if self._closed:
                    await self._dispose_runtime(runtime)
                    return False
                bind_transport_failure = getattr(runtime, "set_transport_failure_callback", None)
                if callable(bind_transport_failure):
                    bind_transport_failure(self.mark_transport_unavailable)
                if self.settings.account_enabled:
                    manager = self._create_manager(runtime)
                    with telemetry.span("runtime.account_connect"):
                        manager.connect(event_loop=asyncio.get_running_loop())
                self.runtime = runtime
                self.generation += 1
                self.trader_manager = manager
                self.last_error = None
                self.configuration_error = False
                self._needs_hub_invalidation = False
                self._sync_state()
                self._inject_notifier(manager)
                await self._start_download_jobs()
                telemetry.emit("runtime.ready", critical=True, runtime_generation=self.generation,
                               account_enabled=manager is not None)
                logger.info("Big QMT runtime initialized via %s", self.settings.zmq_endpoint)
                return True
            except asyncio.CancelledError:
                # The worker thread cannot be cancelled.  close() drains it and
                # disposes any runtime it returns before process shutdown.
                raise
            except Exception as exc:
                if self._inflight_connect is not None and self._inflight_connect.done():
                    self._inflight_connect = None
                if manager is not None:
                    await self._disconnect_manager(manager)
                if runtime is not None:
                    await self._dispose_runtime(runtime)
                self.runtime = None
                self.trader_manager = None
                self.last_error = f"{exc.__class__.__name__}: {exc}"
                self.configuration_error = isinstance(exc, (BigQmtConfigurationError, ValueError))
                telemetry.emit("runtime.connect_failed", critical=True, outcome="unavailable",
                               error_type=type(exc).__name__, configuration_error=self.configuration_error)
                self._sync_state()
                if self.configuration_error:
                    logger.error("Big QMT configuration is invalid: %s", self.last_error)
                else:
                    logger.warning("Big QMT runtime unavailable: %s", self.last_error)
                return False

    def _inject_notifier(self, manager: Any | None) -> None:
        if self._state is None or manager is None:
            return
        notifier = getattr(self._state, "notifier_manager", None)
        callback = getattr(manager, "_callback", None)
        set_notifier = getattr(callback, "set_notifier", None)
        if notifier is not None and callable(set_notifier):
            set_notifier(notifier)

    async def _start_download_jobs(self) -> None:
        """Restore download state only after the native runtime is available.

        State-directory ownership is deliberately isolated from market/trading:
        another bridge process holding that lock leaves downloads unavailable but
        must not take down read-only quotes or guarded account queries.
        """
        if self._download_start is None or self._closed:
            return
        try:
            with telemetry.span("runtime.download_start"):
                await asyncio.to_thread(self._download_start)
            if self._state is not None:
                self._state.download_jobs_error = None
        except Exception as exc:
            telemetry.emit("runtime.download_unavailable", outcome="unavailable", error_type=type(exc).__name__)
            if self._state is not None:
                self._state.download_jobs_error = f"{exc.__class__.__name__}: {exc}"
            logger.warning("Download jobs unavailable: %s", exc)

    def _create_manager(self, runtime: Any) -> Any:
        if self._manager_factory is None:
            from .trading.manager import BigQmtTradingManager
            factory = BigQmtTradingManager
        else:
            factory = self._manager_factory
        return factory(
            runtime=runtime,
            account_id=self.settings.trading_account_id,
            order_writes_enabled=self.settings.order_writes_enabled,
        )

    def _ensure_recovery(self) -> None:
        if self._closed or self.configuration_error or self.recovering:
            return
        cause = telemetry.current_context()
        # Recovery outlives the failed HTTP request. Start a linked background
        # trace instead of pretending that request remains in flight.
        with telemetry.bind_context(trace_id=uuid.uuid4().hex, span_id=None, parent_span_id=None,
                                    caused_by_trace_id=cause.get("trace_id"),
                                    caused_by_rpc_request_id=cause.get("rpc_request_id")):
            self._recovery_task = asyncio.create_task(self._recover(), name="qmt-bridge-recovery")

    @telemetry.traced("runtime.recovery")
    async def _recover(self) -> None:
        for delay in _RETRY_DELAYS:
            if self._closed:
                return
            telemetry.emit("runtime.retry_scheduled", delay_seconds=delay, runtime_generation=self.generation)
            await self._sleep(delay)
            if await self._connect_once():
                return
            if self.configuration_error:
                return
        # Continue at the bounded final cadence without creating parallel tasks.
        while not self._closed and not self.configuration_error:
            telemetry.emit("runtime.retry_scheduled", delay_seconds=_RETRY_DELAYS[-1], runtime_generation=self.generation)
            await self._sleep(_RETRY_DELAYS[-1])
            if await self._connect_once():
                return

    def status(self) -> dict[str, Any]:
        return {
            "initialized": self.runtime is not None,
            "recovering": self.recovering,
            "configuration_error": self.configuration_error,
            "last_error": self.last_error,
            "generation": self.generation,
        }

    def mark_transport_unavailable(self, exc: BaseException) -> None:
        """Schedule one recovery after an observed RPC transport failure.

        This method is intentionally safe from synchronous FastAPI handlers:
        it posts work to the lifespan loop and never executes or retries a
        broker operation itself.
        """
        if self._closed or self.configuration_error or self._loop is None or self._loop.is_closed():
            return
        context = telemetry.current_context()

        def schedule():
            with telemetry.bind_context(**context):
                asyncio.create_task(self._handle_transport_unavailable(exc), name="qmt-bridge-transport-loss")

        self._loop.call_soon_threadsafe(schedule)

    async def _handle_transport_unavailable(self, exc: BaseException) -> None:
        async with self._connect_lock:
            if self._closed or self.configuration_error or self.runtime is None:
                return
            manager, runtime = self.trader_manager, self.runtime
            self.trader_manager = None
            self.runtime = None
            self.last_error = f"{exc.__class__.__name__}: {exc}"
            self._needs_hub_invalidation = True
            self._sync_state()
            telemetry.emit("runtime.transport_lost", critical=True, outcome="unavailable",
                           error_type=type(exc).__name__, runtime_generation=self.generation)
        if manager is not None:
            await self._disconnect_manager(manager)
        await self._dispose_runtime(runtime)
        self._ensure_recovery()

    async def _invalidate_realtime_hub(self) -> None:
        if self._state is None:
            return
        hub = getattr(self._state, "realtime_quote_hub", None)
        invalidate = getattr(hub, "invalidate", None)
        if callable(invalidate):
            result = invalidate(reason="native_runtime_replaced")
            if inspect.isawaitable(result):
                await result

    def _sync_state(self) -> None:
        if self._state is not None:
            self._state.bigqmt_runtime = self.runtime
            self._state.trader_manager = self.trader_manager
            self._state.bigqmt_runtime_error = self.last_error
            self._state.runtime_generation = self.generation

    @staticmethod
    def _close_runtime(runtime: Any) -> None:
        close = getattr(runtime, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                telemetry.emit("runtime.close_failed", critical=True, outcome="error", error_type=type(exc).__name__)
                logger.exception("Error closing failed Big QMT runtime")

    @telemetry.traced("runtime.dispose")
    async def _dispose_runtime(self, runtime: Any) -> None:
        if runtime is None:
            return
        identity = id(runtime)
        if identity in self._disposed_runtime_ids:
            return
        self._disposed_runtime_ids.add(identity)
        if self._uses_global_runtime:
            discarded = await asyncio.to_thread(discard_bigqmt_runtime, runtime)
            if discarded:
                return
        await asyncio.to_thread(self._close_runtime, runtime)

    @staticmethod
    async def _disconnect_manager(manager: Any) -> None:
        disconnect = getattr(manager, "disconnect", None)
        if callable(disconnect):
            try:
                await asyncio.to_thread(disconnect)
            except Exception as exc:
                telemetry.emit("runtime.account_disconnect_failed", critical=True, outcome="error",
                               error_type=type(exc).__name__)
                logger.exception("Error disconnecting Big QMT account manager")

    async def _drain_inflight_connect(self) -> None:
        task = self._inflight_connect
        if task is None:
            return
        timeout = max(1.0, float(self.settings.rpc_timeout_seconds) + 1.0)
        try:
            runtime = await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except asyncio.TimeoutError:
            telemetry.emit("runtime.connect_drain_timeout", critical=True, outcome="unknown", timeout_seconds=timeout)
            def dispose_later(done: asyncio.Task[Any]) -> None:
                try:
                    runtime = done.result()
                except Exception:
                    return
                asyncio.create_task(self._dispose_runtime(runtime))
            task.add_done_callback(dispose_later)
        except Exception:
            pass
        else:
            await self._dispose_runtime(runtime)
        finally:
            if task.done():
                self._inflight_connect = None

    @telemetry.traced("runtime.close")
    async def close(self) -> None:
        self._closed = True
        task = self._recovery_task
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        manager, runtime = self.trader_manager, self.runtime
        self.trader_manager = None
        self.runtime = None
        self._sync_state()
        if manager is not None:
            await self._disconnect_manager(manager)
        if runtime is not None:
            await self._dispose_runtime(runtime)
        await self._drain_inflight_connect()
        # A failed factory can have populated the legacy module proxy before it
        # raised.  Clear only after the owned runtime and in-flight result have
        # been disposed.
        if self._uses_global_runtime:
            await asyncio.to_thread(reset_bigqmt_runtime)
