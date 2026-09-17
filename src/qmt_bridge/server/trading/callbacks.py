"""Bridge native Big QMT trading events into the asyncio application loop."""

import asyncio
import logging
import threading
from collections import deque

from bigqmt_signal_trader.telemetry import bind_context, current_context, emit

logger = logging.getLogger("qmt_bridge.trading.callbacks")


def _as_dict(value) -> dict:
    """Accept native plain mappings without retaining XtQuant object shims."""
    return dict(value) if isinstance(value, dict) else {"value": value}


class BridgeTraderCallback:
    """Receives the native ``on_order``/``on_trade`` callback contract."""

    def __init__(self):
        self._loop: asyncio.AbstractEventLoop | None = None
        self._notifier = None
        self._events = deque(maxlen=1024)
        self._events_lock = threading.Lock()
        self._drain_scheduled = False

    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def set_notifier(self, notifier):
        self._notifier = notifier

    def _dispatch(self, event: dict):
        """Bound native-thread work before handing events to the asyncio loop."""
        if self._loop is None:
            emit("trading.callback_drop", critical=True, outcome="unavailable", reason="event_loop_missing")
            return
        context = current_context()
        data = event.get("data") if isinstance(event, dict) else {}
        emit(
            "trading.callback_received",
            critical=True,
            event_type=event.get("type"),
            order_sys_id=(data or {}).get("order_sys_id"),
            trade_id=(data or {}).get("trade_id"),
        )
        with self._events_lock:
            if len(self._events) == self._events.maxlen:
                self._events.popleft()
                self._events.append((
                    {"type": "gap", "reason": "native_callback_overflow", "reconcile_required": True},
                    context,
                ))
                emit("trading.callback_queue_overflow", critical=True, outcome="gap")
            self._events.append((event, context))
            if self._drain_scheduled:
                return
            self._drain_scheduled = True
        try:
            self._loop.call_soon_threadsafe(self._drain)
        except RuntimeError:
            # Application shutdown can race a native callback.  Do not leak a
            # callback-thread exception or retain events for a closed loop.
            with self._events_lock:
                self._events.clear()
                self._drain_scheduled = False

    def _drain(self):
        from ..ws.trade_callback import publish_trade_event
        while True:
            with self._events_lock:
                if not self._events:
                    self._drain_scheduled = False
                    return
                event, context = self._events.popleft()
            with bind_context(**context):
                emit("trading.callback_publish", critical=True, event_type=event.get("type"))
                publish_trade_event(event)
                if self._notifier is not None:
                    self._notifier.submit(event)

    def on_disconnected(self):
        logger.warning("Big QMT trading client disconnected")
        self._dispatch({"type": "disconnected"})

    def on_order(self, order):
        logger.debug("on_order: %s", order)
        self._dispatch({"type": "order", "data": _as_dict(order)})

    def on_trade(self, trade):
        logger.debug("on_trade: %s", trade)
        self._dispatch({"type": "trade", "data": _as_dict(trade)})
