"""Abstract notifier backend and manager that dispatches events to backends."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
import asyncio

from fastapi import APIRouter, HTTPException, Request
from bigqmt_signal_trader.telemetry import bind_context, current_context, emit, span

if TYPE_CHECKING:
    from ..config import Settings

logger = logging.getLogger("qmt_bridge.notify")
_NOTIFY_QUEUE_SIZE = 256

# ---------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------


class NotifierBackend(ABC):
    """Interface every notification backend must implement."""

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def send(self, event: dict) -> None: ...

    @abstractmethod
    def name(self) -> str: ...


# ---------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------


class NotifierManager:
    """Manages multiple notifier backends with event filtering."""

    def __init__(self, settings: Settings) -> None:
        self._backends: list[NotifierBackend] = []
        self._allow: set[str] | None = None
        self._deny: set[str] = set()
        self._queue: asyncio.Queue[tuple[dict, dict]] = asyncio.Queue(maxsize=_NOTIFY_QUEUE_SIZE)
        self._worker: asyncio.Task | None = None
        self._stopped = False
        self.dropped_events = 0
        self._inflight_event: tuple[dict, dict] | None = None

        # Parse event type filters
        if settings.notify_event_types:
            self._allow = {
                t.strip()
                for t in settings.notify_event_types.split(",")
                if t.strip()
            }
        if settings.notify_ignore_event_types:
            self._deny = {
                t.strip()
                for t in settings.notify_ignore_event_types.split(",")
                if t.strip()
            }

        # Instantiate backends
        backend_names = [
            n.strip()
            for n in settings.notify_backends.split(",")
            if n.strip()
        ]
        for bname in backend_names:
            backend = self._create_backend(bname, settings)
            if backend is not None:
                self._backends.append(backend)

        if not self._backends:
            logger.warning(
                "Notification enabled but no backends configured "
                "(set QMT_BRIDGE_NOTIFY_BACKENDS)"
            )

    @staticmethod
    def _create_backend(
        name: str, settings: Settings
    ) -> NotifierBackend | None:
        if name == "feishu":
            from .feishu import FeishuWebhookBackend

            if not settings.feishu_webhook_url:
                logger.warning("feishu backend requested but FEISHU_WEBHOOK_URL is empty")
                return None
            return FeishuWebhookBackend(
                webhook_url=settings.feishu_webhook_url,
                secret=settings.feishu_webhook_secret,
            )
        if name == "webhook":
            from .webhook import GenericWebhookBackend

            if not settings.webhook_url:
                logger.warning("webhook backend requested but WEBHOOK_URL is empty")
                return None
            return GenericWebhookBackend(
                webhook_url=settings.webhook_url,
                secret=settings.webhook_secret,
            )
        logger.warning("Unknown notify backend: %s", name)
        return None

    @property
    def backend_names(self) -> list[str]:
        return [b.name() for b in self._backends]

    def _should_notify(self, event: dict) -> bool:
        event_type = event.get("type", "")
        if event_type in self._deny:
            return False
        if self._allow is not None and event_type not in self._allow:
            return False
        return True

    async def dispatch(self, event: dict, *, bypass_filter: bool = False) -> None:
        """Send event to all backends (never raises)."""
        if not bypass_filter and not self._should_notify(event):
            emit("notify.filter", outcome="rejected", event_type=event.get("type"))
            return
        emit("notify.filter", outcome="success", event_type=event.get("type"), bypass_filter=bypass_filter)
        for backend in self._backends:
            try:
                with span("notify.send", backend=backend.name(), event_type=event.get("type")) as result:
                    await backend.send(event)
                    result["outcome"] = "success"
            except Exception as exc:
                emit("notify.send", critical=True, outcome="error", backend=backend.name(), error_type=type(exc).__name__)
                logger.exception("Notify backend %s failed", backend.name())

    def submit(self, event: dict) -> bool:
        """Queue a callback event without creating an unbounded task.

        This method must be called from the application event loop.  Callback
        bridges schedule their bounded drain onto that loop before invoking it.
        ``False`` means the event was intentionally not accepted and is logged
        for reconciliation rather than being silently lost.
        """
        if self._worker is None or self._worker.done() or self._stopped:
            emit("notify.enqueue", critical=True, outcome="rejected", event_type=event.get("type"))
            logger.warning("Notification event rejected: notifier worker is not running")
            return False
        try:
            self._queue.put_nowait((event, dict(current_context())))
            emit("notify.enqueue", event_type=event.get("type"), queue_size=self._queue.qsize())
            return True
        except asyncio.QueueFull:
            self.dropped_events += 1
            emit("notify.enqueue", critical=True, outcome="overloaded", event_type=event.get("type"), dropped_events=self.dropped_events)
            logger.warning(
                "Notification queue overflow; event dropped (dropped_events=%s, reconcile if needed)",
                self.dropped_events,
            )
            return False

    async def _run_worker(self) -> None:
        while True:
            event, context = await self._queue.get()
            self._inflight_event = (event, context)
            try:
                # The worker was started during application startup. Restore
                # the submitting request/callback trace for this one item.
                with bind_context(**context):
                    await self.dispatch(event)
            finally:
                self._inflight_event = None
                self._queue.task_done()

    async def start(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        self._stopped = False
        for backend in self._backends:
            try:
                await backend.start()
                emit("notify.backend_started", critical=True, backend=backend.name())
                logger.info("Notifier backend started: %s", backend.name())
            except Exception:
                emit("notify.backend_started", critical=True, outcome="error", backend=backend.name())
                logger.exception("Failed to start notifier backend: %s", backend.name())
        self._worker = asyncio.create_task(
            self._run_worker(), name="qmt-notification-worker"
        )
        emit("notify.worker_started", critical=True)

    async def stop(self) -> None:
        self._stopped = True
        worker = self._worker
        self._worker = None
        inflight_canceled_unknown = self._inflight_event is not None
        if worker is not None:
            # A backend can be stalled in I/O; shutdown must not retain a
            # bridge-owned task indefinitely.
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            emit("notify.worker_stopped", critical=True)
        purged = 0
        while True:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                purged += 1
            except asyncio.QueueEmpty:
                break
        if purged or inflight_canceled_unknown:
            self.dropped_events += purged + int(inflight_canceled_unknown)
            emit(
                "notify.shutdown_drop",
                critical=True,
                outcome="unknown",
                purged_events=purged,
                inflight_canceled_unknown=inflight_canceled_unknown,
                dropped_events=self.dropped_events,
            )
        for backend in self._backends:
            try:
                await backend.stop()
                emit("notify.backend_stopped", critical=True, backend=backend.name())
                logger.info("Notifier backend stopped: %s", backend.name())
            except Exception:
                logger.exception("Error stopping notifier backend: %s", backend.name())


# ---------------------------------------------------------------------
# Test endpoint
# ---------------------------------------------------------------------

router = APIRouter(prefix="/api/notify", tags=["notify"])


@router.post("/test")
async def test_notify(request: Request):
    """Send a test notification to all configured backends."""
    notifier: NotifierManager | None = getattr(
        request.app.state, "notifier_manager", None
    )
    if notifier is None:
        raise HTTPException(503, "Notification module not enabled")
    test_event = {
        "type": "test",
        "data": {"message": "QMT Bridge notification test"},
    }
    emit("notify.test_dispatch", critical=True)
    await notifier.dispatch(test_event, bypass_filter=True)
    return {"status": "sent", "backends": notifier.backend_names}
