"""Shared polling WebSocket endpoint for ``/ws/whole_quote``."""

import asyncio
import hmac
import json
import uuid

import anyio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..bigqmt import market_data
from ..config import get_settings
from ..helpers import _call_xtdata_serialized, _numpy_to_python
from .quote_hub import WholeQuoteHub
from bigqmt_signal_trader.telemetry import bind_context, current_context, emit, span

router = APIRouter()


def _normalize_codes(raw):
    if not isinstance(raw, list):
        return []
    return list(dict.fromkeys(str(value or "").strip().upper() for value in raw if str(value or "").strip()))


def _allowed(ws: WebSocket) -> bool:
    settings = getattr(ws.app.state, "settings", None) or get_settings()
    if not settings.require_auth_for_data:
        return True
    supplied = ws.headers.get("X-API-Key", "")
    return bool(settings.api_key and supplied and hmac.compare_digest(supplied, settings.api_key))


def _hub(ws: WebSocket) -> WholeQuoteHub:
    hub = getattr(ws.app.state, "whole_quote_hub", None)
    if hub is None:
        hub = WholeQuoteHub(lambda *, code_list: _call_xtdata_serialized(market_data.get_full_tick, code_list=code_list))
        ws.app.state.whole_quote_hub = hub
    return hub


@router.websocket("/ws/whole_quote")
async def ws_whole_quote(ws: WebSocket):
    session_id = current_context().get("ws_session_id") or uuid.uuid4().hex
    if not _allowed(ws):
        emit("ws.whole_quote.auth", critical=True, outcome="rejected", ws_session_id=session_id)
        await ws.close(code=1008, reason="Invalid API key")
        return
    await ws.accept()
    scope = bind_context(ws_session_id=session_id, ws_endpoint="whole_quote")
    scope.__enter__()
    emit("ws.whole_quote.connected", ws_session_id=session_id)
    hub = _hub(ws)
    key = None
    subscriber = None
    sender = None
    receiver = None
    try:
        payload = json.loads(await ws.receive_text())
        code_list = _normalize_codes(payload.get("codes"))
        interval = min(max(float(payload.get("interval_seconds", 3.0)), 3.0), 60.0)
        if not code_list:
            emit("ws.whole_quote.subscription", outcome="rejected", ws_session_id=session_id)
            await ws.send_json({"status": "error", "reason": "codes_required"})
            await ws.close(code=1008)
            return
        with span("ws.whole_quote.subscription", ws_session_id=session_id, code_count=len(code_list)) as result:
            key, subscriber = await hub.subscribe(code_list, interval)
            result["outcome"] = "success"

        async def send_snapshots():
            while True:
                data = await subscriber.queue.get()
                if isinstance(data, dict) and data.get("type") == "unavailable":
                    emit("ws.whole_quote.send", critical=True, outcome="unavailable", ws_session_id=session_id)
                    await ws.send_json(data)
                    await ws.close(code=1012, reason="native whole quote unavailable")
                    return
                await ws.send_json({"type": "snapshot", "mode": "bigqmt_polling", "data": _numpy_to_python(data)})

        sender = asyncio.create_task(send_snapshots())
        async def receive_controls():
            while True:
                message = json.loads(await ws.receive_text())
                if message.get("action") in {"close", "unsubscribe"}:
                    emit("ws.whole_quote.control_close", ws_session_id=session_id)
                    await ws.close()
                    return

        receiver = asyncio.create_task(receive_controls())
        done, pending = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*done, *pending, return_exceptions=True)
    except WebSocketDisconnect:
        emit("ws.whole_quote.disconnected", ws_session_id=session_id)
        pass
    except asyncio.CancelledError:
        # Starlette's in-memory WebSocket peer can cancel the endpoint before
        # it reports a disconnect. Cleanup below is shielded; treating this as
        # a peer disconnect prevents the TestClient portal future from being
        # left cancelled.
        emit("ws.whole_quote.disconnected", outcome="canceled", ws_session_id=session_id)
    except (json.JSONDecodeError, TypeError, ValueError):
        emit("ws.whole_quote.request", critical=True, outcome="rejected", ws_session_id=session_id)
        try:
            await ws.send_json({"status": "error", "reason": "invalid_request"})
            await ws.close(code=1008)
        except (RuntimeError, WebSocketDisconnect):
            pass
    finally:
        # TestClient cancels a route when a peer exits abruptly.  Shield the
        # ownership cleanup so the final subscriber always tears down the hub.
        with anyio.CancelScope(shield=True):
            for task in (sender, receiver):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (sender, receiver) if task is not None),
                return_exceptions=True,
            )
            if key is not None and subscriber is not None:
                await hub.unsubscribe(key, subscriber)
            emit("ws.whole_quote.cleanup", critical=True, ws_session_id=session_id)
            scope.__exit__(None, None, None)
