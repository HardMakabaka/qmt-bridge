"""WebSocket endpoint — whole market quote subscription /ws/whole_quote."""

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from ..bigqmt import xtdata

from ..helpers import _call_xtdata_serialized, _numpy_to_python

router = APIRouter()


@router.websocket("/ws/whole_quote")
async def ws_whole_quote(ws: WebSocket):
    await ws.accept()
    try:
        msg = await ws.receive_text()
        payload = json.loads(msg)
        code_list: list[str] = payload.get("codes", [])
        interval = min(max(float(payload.get("interval_seconds", 3.0)), 3.0), 60.0)
        if not code_list:
            await ws.send_json(
                {"status": "error", "reason": "codes_required"}
            )
            await ws.close(code=1008)
            return
        while True:
            data = await asyncio.to_thread(
                _call_xtdata_serialized,
                xtdata.get_full_tick,
                code_list=code_list,
            )
            await ws.send_json(
                {
                    "type": "snapshot",
                    "mode": "bigqmt_polling",
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
