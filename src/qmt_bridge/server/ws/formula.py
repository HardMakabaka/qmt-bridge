from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/formula")
async def ws_formula(ws: WebSocket):
    await ws.accept()
    try:
        await ws.receive_text()
        await ws.send_json(
            {
                "status": "unsupported",
                "capability": "formula_push",
                "reason": "bigqmt_formula_push_not_verified",
            }
        )
        await ws.close(code=1003)
    except WebSocketDisconnect:
        pass
