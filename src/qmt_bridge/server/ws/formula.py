from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..helpers import _status_payload
from bigqmt_signal_trader.telemetry import emit

router = APIRouter()


@router.websocket("/ws/formula")
async def ws_formula(ws: WebSocket):
    await ws.accept()
    try:
        await ws.receive_text()
        emit("ws.formula.subscription", critical=True, outcome="unsupported")
        await ws.send_json(
            _status_payload(
                "unsupported",
                reason="bigqmt_formula_push_not_verified",
                function="formula_push",
            )
        )
        await ws.close(code=1003)
    except WebSocketDisconnect:
        emit("ws.formula.disconnected")
