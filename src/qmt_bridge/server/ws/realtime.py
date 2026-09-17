import asyncio
import hmac
import json
import time
import uuid
from datetime import datetime, timezone

import anyio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..bigqmt import BigQmtRuntimeUnavailable, get_bigqmt_runtime, market_data
from ..config import get_settings
from .quote_hub import LatestValueQueue, RealtimeQuoteHub
from bigqmt_signal_trader.telemetry import bind_context, current_context, emit, span

router = APIRouter()


# Kept as a small public helper for integrations which want the historical
# per-code rate limiter. The WebSocket uses the stricter one-slot queue below.
class LatestQuoteBuffer:
    def __init__(self):
        self._pending = {}
        self._last_sent = {}

    @property
    def pending_count(self):
        return len(self._pending)

    def offer(self, event):
        code = str(event.get("code") or "").upper()
        if code:
            self._pending[code] = (time.monotonic(), event)

    def pop_ready(self):
        now = time.monotonic()
        for code, (_offered_at, event) in list(self._pending.items()):
            if now - self._last_sent.get(code, 0.0) < 0.25:
                continue
            self._pending.pop(code, None)
            self._last_sent[code] = now
            return event
        return None

    def next_delay(self):
        if not self._pending:
            return None
        now = time.monotonic()
        return max(0.0, min(self._last_sent.get(code, 0.0) + 0.25 - now for code in self._pending))


def _normalize_codes(raw):
    if not isinstance(raw, list):
        return []
    return list(dict.fromkeys(str(value or "").strip().upper() for value in raw if str(value or "").strip()))


def _hub(ws: WebSocket) -> RealtimeQuoteHub:
    hub = getattr(ws.app.state, "realtime_quote_hub", None)
    if hub is None:
        hub = RealtimeQuoteHub(market_data, generation=_runtime_generation)
        ws.app.state.realtime_quote_hub = hub
    return hub


def _runtime_generation():
    try:
        return id(get_bigqmt_runtime())
    except BigQmtRuntimeUnavailable:
        # Unit-test facades and deliberately uninitialized applications have no
        # recoverable QMT generation. Their subscriptions still exercise the
        # same sharing code.
        return None


async def _send_quotes(ws, queue: LatestValueQueue):
    while True:
        event = await queue.queue.get()
        if event.get("type") == "unavailable":
            emit("ws.realtime.send", critical=True, outcome="unavailable")
            await ws.send_json(event)
            await ws.close(code=1012, reason="native QMT runtime replaced")
            return
        await ws.send_json({
            "type": "quote",
            "mode": "qmt_callback",
            "code": event["code"],
            "data": event["data"],
            "source_at": event.get("source_at"),
            "received_at": datetime.now(timezone.utc).isoformat(),
        })


@router.websocket("/ws/realtime")
async def ws_realtime(ws: WebSocket):
    session_id = current_context().get("ws_session_id") or uuid.uuid4().hex
    settings = getattr(ws.app.state, "settings", None) or get_settings()
    api_key = ws.headers.get("X-API-Key", "")
    # Realtime native callbacks consume a scarce global QMT subscription budget,
    # so retain authentication even when ordinary read-only HTTP is public.
    if not settings.api_key or not api_key or not hmac.compare_digest(api_key, settings.api_key):
        emit("ws.realtime.auth", critical=True, outcome="rejected", ws_session_id=session_id)
        await ws.close(code=1008, reason="Invalid API key")
        return

    await ws.accept()
    scope = bind_context(ws_session_id=session_id, ws_endpoint="realtime")
    scope.__enter__()
    emit("ws.realtime.connected", ws_session_id=session_id)
    codes = []
    period = "tick"
    queue = None
    sender = None
    receiver = None
    try:
        payload = json.loads(await ws.receive_text())
        codes = _normalize_codes(payload.get("stocks"))
        period = str(payload.get("period") or "tick").lower()
        if not 1 <= len(codes) <= 500:
            emit("ws.realtime.subscription", outcome="rejected", ws_session_id=session_id)
            await ws.send_json({"status": "unavailable", "reason": "stocks_limit_1_500"})
            await ws.close(code=1008)
            return
        if period != "tick":
            emit("ws.realtime.subscription", outcome="unsupported", ws_session_id=session_id, period=period)
            await ws.send_json({"status": "unavailable", "reason": "period_tick_required"})
            await ws.close(code=1008)
            return

        with span("ws.realtime.subscription", ws_session_id=session_id, code_count=len(codes), period=period) as result:
            subscription_ids, queue = await _hub(ws).subscribe(codes, period)
            result["outcome"] = "success"
        await ws.send_json({
            "type": "subscribed", "mode": "qmt_callback", "period": period,
            "codes": codes, "subscription_ids": subscription_ids,
        })
        sender = asyncio.create_task(_send_quotes(ws, queue))
        # Only controls are read here. Sender owns all writes so a slow client
        # cannot block native callback delivery or another connection.
        async def receive_controls():
            while True:
                message = json.loads(await ws.receive_text())
                if message.get("action") in {"close", "unsubscribe"}:
                    emit("ws.realtime.control_close", ws_session_id=session_id)
                    await ws.close()
                    return

        receiver = asyncio.create_task(receive_controls())
        done, pending = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*done, *pending, return_exceptions=True)
    except WebSocketDisconnect:
        emit("ws.realtime.disconnected", ws_session_id=session_id)
    except asyncio.CancelledError:
        # See whole_quote: an abrupt in-memory peer exit can arrive as task
        # cancellation instead of WebSocketDisconnect.
        emit("ws.realtime.disconnected", outcome="canceled", ws_session_id=session_id)
    except (AttributeError, NotImplementedError, OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        emit("ws.realtime.subscription", critical=True, outcome="unsupported", ws_session_id=session_id)
        try:
            await ws.send_json({"status": "unavailable", "reason": "native_quote_callback_unavailable"})
            await ws.close(code=1011)
        except (RuntimeError, WebSocketDisconnect):
            pass
    finally:
        with anyio.CancelScope(shield=True):
            for task in (sender, receiver):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (sender, receiver) if task is not None),
                return_exceptions=True,
            )
            if queue is not None:
                await _hub(ws).unsubscribe(codes, period, queue)
            emit("ws.realtime.cleanup", critical=True, ws_session_id=session_id)
            scope.__exit__(None, None, None)
