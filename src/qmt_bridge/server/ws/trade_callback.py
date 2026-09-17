"""Bounded trade-event WebSocket fan-out."""

import asyncio
import hmac
import logging
import uuid
from dataclasses import dataclass, field

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from bigqmt_signal_trader.telemetry import bind_context, current_context, emit

router = APIRouter()
logger = logging.getLogger("qmt_bridge.ws.trade")
_TRADE_QUEUE_SIZE = 128


@dataclass(eq=False)
class _TradeListener:
    ws: WebSocket
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=_TRADE_QUEUE_SIZE))
    overflowed: bool = False

    def offer(self, event: dict) -> None:
        if self.overflowed:
            return
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            # Losing a trade event must never look like a normal stream. Discard
            # the backlog, deliver an explicit gap, then close the connection.
            self.overflowed = True
            while True:
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            self.queue.put_nowait({"type": "gap", "reason": "trade_event_overflow", "reconcile_required": True})
            emit("ws.trade.queue_overflow", critical=True, outcome="gap", dropped_events=_TRADE_QUEUE_SIZE)


_trade_listeners: set[_TradeListener] = set()


def publish_trade_event(event: dict) -> None:
    """Non-blocking loop-thread publication used by native callback drains."""
    for listener in tuple(_trade_listeners):
        listener.offer(event)
    data = event.get("data") if isinstance(event, dict) else None
    cursor = event.get("cursor") if isinstance(event, dict) else None
    if not isinstance(cursor, dict) and isinstance(data, dict):
        cursor = data.get("cursor")
    emit("ws.trade.publish", critical=True, event_type=(event or {}).get("type"), event_epoch=(cursor or {}).get("epoch"), event_sequence=(cursor or {}).get("sequence"))


async def broadcast_trade_event(event: dict):
    """Compatibility coroutine; publication itself never waits on a client."""
    publish_trade_event(event)


async def _sender(listener: _TradeListener) -> None:
    while True:
        event = await listener.queue.get()
        try:
            await listener.ws.send_json(event)
        except Exception as exc:
            emit(
                "ws.trade.send",
                critical=True,
                outcome="error",
                event_type=event.get("type"),
                error_type=type(exc).__name__,
            )
            raise
        emit("ws.trade.send", event_type=event.get("type"))
        if event.get("type") == "gap":
            await listener.ws.close(code=1013, reason="trade event overflow; reconcile required")
            return


@router.websocket("/ws/trade")
async def ws_trade(ws: WebSocket, api_key: str = Query("", alias="api_key")):
    """Trade event WebSocket, with one bounded sender queue per connection."""
    from ..config import get_settings

    session_id = current_context().get("ws_session_id") or uuid.uuid4().hex
    settings = getattr(ws.app.state, "settings", None) or get_settings()
    # Header is canonical; query is retained only for existing SDK callers.
    supplied = ws.headers.get("X-API-Key", "") or api_key
    if not settings.api_key:
        emit("ws.trade.auth", critical=True, outcome="rejected", ws_session_id=session_id)
        await ws.close(code=1008, reason="API key not configured on server")
        return
    if not supplied or not hmac.compare_digest(supplied, settings.api_key):
        emit("ws.trade.auth", critical=True, outcome="rejected", ws_session_id=session_id)
        await ws.close(code=1008, reason="Invalid API key")
        return

    await ws.accept()
    scope = bind_context(ws_session_id=session_id, ws_endpoint="trade")
    scope.__enter__()
    emit("ws.trade.connected", ws_session_id=session_id)
    listener = _TradeListener(ws)
    _trade_listeners.add(listener)
    sender = asyncio.create_task(_sender(listener))
    receiver = None
    logger.info("Trade WebSocket client connected")
    try:
        async def receive_controls():
            while True:
                await ws.receive_text()

        receiver = asyncio.create_task(receive_controls())
        done, pending = await asyncio.wait(
            {sender, receiver}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*done, *pending, return_exceptions=True)
    except WebSocketDisconnect:
        emit("ws.trade.disconnected", ws_session_id=session_id)
    finally:
        _trade_listeners.discard(listener)
        for task in (sender, receiver):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in (sender, receiver) if task is not None),
            return_exceptions=True,
        )
        emit("ws.trade.cleanup", critical=True, ws_session_id=session_id)
        scope.__exit__(None, None, None)
        logger.info("Trade WebSocket client disconnected")
