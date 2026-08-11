from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/l2_thousand")
async def ws_l2_thousand(ws: WebSocket):
    await ws.accept()
    try:
        await ws.receive_text()
        await ws.send_json(
            {
                "status": "unsupported",
                "capability": "l2_thousand_push",
                "reason": "bigqmt_l2_push_not_verified",
            }
        )
        await ws.close(code=1003)
    except WebSocketDisconnect:
        pass
