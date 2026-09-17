import asyncio
from datetime import datetime
from threading import Lock
from types import SimpleNamespace

import httpx2 as httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout
from qmt_bridge.server.config import Settings, get_settings
from qmt_bridge.server.history_readiness import ProbeState
from qmt_bridge.server.routers import meta


class Frame:
    def __init__(self, rows): self.rows = rows
    def reset_index(self): return self
    def to_dict(self, _orient): return self.rows


class Client:
    def __init__(self, result=None, error=None): self.result, self.error, self.calls = result, error, []
    def call(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.error: raise self.error
        return self.result


def _app(client=None):
    app = FastAPI()
    app.include_router(meta.router)
    app.state.bigqmt_runtime = None if client is None else SimpleNamespace(client=client)
    app.state.active_write_requests = 0
    app.state.active_write_requests_lock = Lock()
    return app


def _url(**extra):
    query = {"stock": "000300.SH", "start_time": "20260820145700", "end_time": "20260820145900", "timeout_seconds": "8", **extra}
    return "/api/meta/history-readiness?" + "&".join(f"{key}={value}" for key, value in query.items())


def test_download_transport_stuck_cannot_be_reported_healthy():
    from qmt_bridge.server.helpers import _mark_xtdata_transport_stuck, _reset_xtdata_transport_for_tests
    client = Client({})
    _mark_xtdata_transport_stuck("native download thread did not exit")
    try:
        with TestClient(_app(client)) as http:
            payload = http.get(_url()).json()
            recovery = http.get("/api/meta/recovery-status").json()
        assert payload["status"] == "unavailable"
        assert payload["failure_kind"] == "transport"
        assert payload["rpc_status"] == "connection_error"
        assert payload["error"]["code"] == "xtdata_transport_stuck"
        assert recovery["download_transport"]["status"] == "blocked"
        assert client.calls == []
    finally:
        _reset_xtdata_transport_for_tests()
    with TestClient(_app(client)) as http:
        assert http.get("/api/meta/recovery-status").json()["download_transport"]["status"] == "available"


def test_history_readiness_forces_native_rpc_and_classifies_complete_empty_partial_and_zero_volume():
    rows = [{"stime": 1787209020000, "volume": 0, "open": 1, "high": 1, "low": 1, "close": 1}, {"stime": "20260820145800", "volume": 2, "open": 1, "high": 1, "low": 1, "close": 1}, {"stime": "20260820145900", "volume": 3, "open": 1, "high": 1, "low": 1, "close": 1}]
    client = Client({"000300.SH": Frame(rows)})
    with TestClient(_app(client)) as http:
        payload = http.get(_url()).json()
    assert payload["status"] == "healthy" and payload["failure_kind"] == "none"
    assert payload["quality_flags"] == ["zero_volume_not_tradability"]
    assert client.calls[0][0][0] == "get_market_data_ex"
    assert client.calls[0][1]["force_rpc"] is True
    assert client.calls[0][1]["params"]["field_list"] == ["time", "open", "high", "low", "close", "volume"]
    assert client.calls[0][1]["params"]["fill_data"] is False
    assert client.calls[0][1]["params"]["subscribe"] is False
    assert payload["probe"]["subscribe"] is False

    with TestClient(_app(Client({"000300.SH": []}))) as http:
        empty = http.get(_url()).json()
    assert (empty["status"], empty["rpc_status"], empty["data_status"], empty["failure_kind"]) == ("data_gap", "ok", "empty", "data")
    with TestClient(_app(Client({"000300.SH": Frame(rows[:2])}))) as http:
        partial = http.get(_url()).json()
    assert (partial["status"], partial["data_status"], partial["failure_kind"]) == ("data_gap", "partial", "data")


def test_history_readiness_timeout_and_invalid_query_do_not_claim_transport_success():
    with TestClient(_app(Client(error=TimeoutError("fixture")))) as http:
        timeout = http.get(_url(timeout_seconds="0.01")).json()
        invalid = http.get(_url(end_time="20260820150000")).json()
    assert (timeout["rpc_status"], timeout["failure_kind"]) == ("timeout", "transport")
    assert (invalid["rpc_status"], invalid["failure_kind"]) == ("unavailable", "configuration")


def test_history_readiness_lunch_and_weekend_are_not_missing_bars():
    lunch = Client({})
    with TestClient(_app(lunch)) as http:
        payload = http.get(_url(start_time="20260820113100", end_time="20260820113200")).json()
    assert (payload["status"], payload["data_status"], payload["failure_kind"]) == ("healthy", "complete", "none")
    assert payload["probe"]["expected_minutes"] == []
    assert lunch.calls == []

    weekend = Client({})
    with TestClient(_app(weekend)) as http:
        payload = http.get(_url(start_time="20260822145700", end_time="20260822145900")).json()
    assert (payload["status"], payload["data_status"]) == ("healthy", "complete")
    assert weekend.calls == []


@pytest.mark.parametrize("error,status", [
    (TransportTimeout("fixture"), "timeout"),
    (TransportError("fixture"), "connection_error"),
])
def test_history_readiness_classifies_real_transport_errors_without_message_guessing(error, status):
    client = Client(error=error)
    with TestClient(_app(client)) as http:
        payload = http.get(_url()).json()
    assert (payload["rpc_status"], payload["failure_kind"]) == (status, "transport")


@pytest.mark.parametrize("extra", [
    {"stock": "not-a-code"},
    {"start_time": "20260820145730"},
    {"end_time": "20260820150000"},
    {"timeout_seconds": "8.1"},
])
def test_history_readiness_rejects_out_of_contract_parameters_without_rpc(extra):
    client = Client()
    with TestClient(_app(client)) as http:
        payload = http.get(_url(**extra)).json()
    assert payload["failure_kind"] == "configuration"
    assert client.calls == []


def test_history_readiness_accepts_dataframe_index_time_and_requires_all_finite_ohlc():
    class IndexedFrame:
        def to_dict(self, _orient):
            return []

        def reset_index(self):
            return Frame([
                {"index": datetime(2026, 8, 20, 14, 57), "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
                {"index": datetime(2026, 8, 20, 14, 58), "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
                {"index": datetime(2026, 8, 20, 14, 59), "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
            ])

    with TestClient(_app(Client({"000300.SH": IndexedFrame()}))) as http:
        assert http.get(_url()).json()["status"] == "healthy"
    invalid = [
        {"stime": "20260820145700", "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
        {"stime": "20260820145800", "volume": 1, "open": 1, "high": float("nan"), "low": 1, "close": 1},
        {"stime": "20260820145900", "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
    ]
    with TestClient(_app(Client({"000300.SH": invalid}))) as http:
        payload = http.get(_url()).json()
    assert (payload["status"], payload["data_status"], payload["failure_kind"]) == ("data_gap", "invalid", "data")


@pytest.mark.parametrize("rows", [
    [
        {"stime": "20260820145700", "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
        {"stime": "20260820145700", "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
    ],
    [
        {"stime": "20260820145600", "volume": 1, "open": 1, "high": 1, "low": 1, "close": 1},
    ],
])
def test_history_readiness_marks_duplicate_or_out_of_window_bars_invalid(rows):
    with TestClient(_app(Client({"000300.SH": rows}))) as http:
        payload = http.get(_url()).json()
    assert payload["data_status"] == "invalid"


def test_probe_state_fences_late_requests_and_only_records_complete_success():
    state = ProbeState()
    first = state.begin(("000300.SH", "20260820145700", "20260820145900"))
    replacement = state.begin(("000300.SH", "20260821145700", "20260821145900"))
    assert state.update(first, "ok", "complete", "none")[2] is False
    assert state.update(replacement, "ok", "partial", "data") == (None, 0, True)
    stale_same_request = state.begin(replacement.key)
    latest = state.begin(replacement.key)
    assert state.update(stale_same_request, "timeout", "empty", "transport")[2] is False
    success_at, failures, applied = state.update(latest, "ok", "complete", "none")
    assert applied is True and success_at is not None and failures == 0
    after_config = state.begin(replacement.key)
    assert state.update(after_config, "unavailable", "invalid", "configuration")[:2] == (success_at, 0)


def test_recovery_status_fails_closed_when_counter_state_is_missing():
    app = FastAPI()
    app.include_router(meta.router)
    with TestClient(app) as http:
        payload = http.get("/api/meta/recovery-status").json()
    assert (payload["counter_status"], payload["active_write_requests"], payload["restart_safe"]) == ("unknown", None, False)


def test_history_readiness_optional_data_auth_rejects_without_key_before_rpc():
    client = Client({"000300.SH": []})
    app = _app(client)
    app.dependency_overrides[get_settings] = lambda: Settings(api_key="fixture-key", require_auth_for_data=True)
    with TestClient(app) as http:
        assert http.get(_url()).status_code == 401
        assert client.calls == []
        assert http.get(_url(), headers={"X-API-Key": "fixture-key"}).status_code == 200
    assert len(client.calls) == 1


WRITE_PATHS = (
    "/api/trading/order", "/api/trading/cancel", "/api/trading/batch_order",
    "/api/trading/batch_cancel", "/api/trading/sync_transaction",
)


def _write_tracking_app(path, *, raises=False):
    from qmt_bridge.server import app as app_module

    application = app_module.create_app(Settings(account_enabled=False))
    entered, release = asyncio.Event(), asyncio.Event()

    @application.post(path)
    async def write_endpoint():
        entered.set()
        await release.wait()
        if raises:
            raise RuntimeError("fixture")
        return {"ok": True}

    return application, entered, release


@pytest.mark.parametrize("path", WRITE_PATHS)
def test_real_app_write_middleware_blocks_recovery_while_each_write_is_inflight(path):
    application, entered, release = _write_tracking_app(path)
    assert isinstance(application.state.history_readiness_state, ProbeState)

    async def scenario():
        transport = httpx.ASGITransport(app=application, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            task = asyncio.create_task(client.post(path))
            await asyncio.wait_for(entered.wait(), timeout=1)
            assert (await client.get("/api/meta/recovery-status")).json()["restart_safe"] is False
            release.set()
            assert (await task).status_code == 200
            assert (await client.get("/api/meta/recovery-status")).json()["restart_safe"] is True

    asyncio.run(scenario())


@pytest.mark.parametrize("path", (
    "/api/trading/order", "/api/trading/cancel", "/api/trading/sync_transaction",
))
def test_real_app_write_middleware_cleans_counter_after_exception(path):
    application, entered, release = _write_tracking_app(path, raises=True)

    async def scenario():
        transport = httpx.ASGITransport(app=application, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            task = asyncio.create_task(client.post(path))
            await asyncio.wait_for(entered.wait(), timeout=1)
            assert (await client.get("/api/meta/recovery-status")).json()["restart_safe"] is False
            release.set()
            assert (await task).status_code == 500
            assert (await client.get("/api/meta/recovery-status")).json()["restart_safe"] is True

    asyncio.run(scenario())


def test_app_tracks_only_order_and_cancel_requests_and_cleans_up_after_exception(monkeypatch):
    from qmt_bridge.server import app as app_module
    from qmt_bridge.server.config import Settings

    monkeypatch.setattr(app_module, "initialize_bigqmt_runtime", lambda _settings: (_ for _ in ()).throw(RuntimeError("fixture")))
    application = app_module.create_app(Settings(account_enabled=False))
    observed = []

    @application.post("/api/trading/order")
    async def order():
        observed.append(application.state.active_write_requests)
        raise RuntimeError("fixture")

    @application.post("/api/trading/cancel")
    async def cancel():
        observed.append(application.state.active_write_requests)
        return {"ok": True}

    with TestClient(application, raise_server_exceptions=False) as http:
        assert http.post("/api/trading/order").status_code == 500
        assert http.post("/api/trading/cancel").status_code == 200
    assert observed == [1, 1]
    assert application.state.active_write_requests == 0
