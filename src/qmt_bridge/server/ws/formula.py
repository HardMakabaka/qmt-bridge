from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..helpers import _status_payload

router = APIRouter()


@router.websocket("/ws/formula")
async def ws_formula(ws: WebSocket):
    await ws.accept()
    try:
        await ws.receive_text()
        await ws.send_json(
            _status_payload(
                "unsupported",
                reason="bigqmt_formula_push_not_verified",
                function="formula_push",
            )
        )
        await ws.close(code=1003)
    except WebSocketDisconnect:
        pass
