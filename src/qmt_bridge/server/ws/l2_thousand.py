from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..helpers import _status_payload

router = APIRouter()


@router.websocket("/ws/l2_thousand")
async def ws_l2_thousand(ws: WebSocket):
    await ws.accept()
    try:
        await ws.receive_text()
        await ws.send_json(
            _status_payload(
                "unsupported",
                reason="bigqmt_l2_push_not_verified",
                function="l2_thousand_push",
            )
        )
        await ws.close(code=1003)
    except WebSocketDisconnect:
        pass
