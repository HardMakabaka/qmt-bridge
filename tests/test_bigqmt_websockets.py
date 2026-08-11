from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from qmt_bridge.server.ws import formula, l2_thousand, realtime, whole_quote


def _app_with(router):
    app = FastAPI()
    app.include_router(router)
    return app


def test_realtime_websocket_polls_full_tick(monkeypatch):
    calls = []

    def get_full_tick(*, code_list):
        calls.append(code_list)
        return {"000001.SZ": {"lastPrice": 10.5}}

    monkeypatch.setattr(realtime, "xtdata", SimpleNamespace(get_full_tick=get_full_tick))

    with TestClient(_app_with(realtime.router)).websocket_connect("/ws/realtime") as ws:
        ws.send_json({"stocks": ["000001.SZ"], "period": "tick"})
        payload = ws.receive_json()

    assert calls == [["000001.SZ"]]
    assert payload["type"] == "snapshot"
    assert payload["mode"] == "bigqmt_polling"
    assert payload["data"]["000001.SZ"]["lastPrice"] == 10.5


def test_whole_quote_websocket_uses_bounded_polling(monkeypatch):
    calls = []

    def get_full_tick(*, code_list):
        calls.append(code_list)
        return {"000001.SZ": {"lastPrice": 10.5}}

    monkeypatch.setattr(
        whole_quote,
        "xtdata",
        SimpleNamespace(get_full_tick=get_full_tick),
    )

    with TestClient(_app_with(whole_quote.router)).websocket_connect("/ws/whole_quote") as ws:
        ws.send_json({"codes": ["SH"], "interval_seconds": 3})
        payload = ws.receive_json()

    assert calls == [["SH"]]
    assert payload["type"] == "snapshot"
    assert payload["mode"] == "bigqmt_polling"


def test_formula_websocket_reports_push_as_unsupported():
    with TestClient(_app_with(formula.router)).websocket_connect("/ws/formula") as ws:
        ws.send_json({"action": "subscribe", "formula_name": "MA"})
        payload = ws.receive_json()

    assert payload == {
        "status": "unsupported",
        "capability": "formula_push",
        "reason": "bigqmt_formula_push_not_verified",
    }


def test_l2_thousand_websocket_reports_push_as_unsupported():
    with TestClient(_app_with(l2_thousand.router)).websocket_connect("/ws/l2_thousand") as ws:
        ws.send_json({"stocks": ["000001.SZ"]})
        payload = ws.receive_json()

    assert payload == {
        "status": "unsupported",
        "capability": "l2_thousand_push",
        "reason": "bigqmt_l2_push_not_verified",
    }
