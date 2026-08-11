"""WebSocket endpoint — realtime quote subscription /ws/realtime."""

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from ..bigqmt import xtdata

from ..helpers import _call_xtdata_serialized, _numpy_to_python

router = APIRouter()


@router.websocket("/ws/realtime")
async def ws_realtime(ws: WebSocket):
    await ws.accept()
    try:
        msg = await ws.receive_text()
        payload = json.loads(msg)
        stocks: list[str] = payload.get("stocks", [])
        period: str = payload.get("period", "tick")
        interval = min(max(float(payload.get("interval_seconds", 1.0)), 0.2), 60.0)
        if not stocks:
            await ws.send_json(
                {"status": "error", "reason": "stocks_required"}
            )
            await ws.close(code=1008)
            return
        while True:
            data = await asyncio.to_thread(
                _call_xtdata_serialized,
                xtdata.get_full_tick,
                code_list=stocks,
            )
            await ws.send_json(
                {
                    "type": "snapshot",
                    "mode": "bigqmt_polling",
                    "period": period,
                    "data": _numpy_to_python(data),
                }
            )
            try:
                control = await asyncio.wait_for(
                    ws.receive_text(), timeout=interval
                )
            except asyncio.TimeoutError:
                continue
            if control:
                message = json.loads(control)
                if message.get("action") in {"close", "unsubscribe"}:
                    await ws.close()
                    return
    except WebSocketDisconnect:
        pass
