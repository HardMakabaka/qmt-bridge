import sys
import threading
import time
from types import ModuleType, SimpleNamespace

import pandas as pd

xtquant_stub = ModuleType("xtquant")
xtquant_stub.market_data = SimpleNamespace()
sys.modules.setdefault("xtquant", xtquant_stub)

from qmt_bridge.server.models import HistoryDownloadJobRequest
from qmt_bridge.server.routers import download


def _wait(job_id: str, timeout: float = 2.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = download.get_history_download_job(job_id)
        if result["status"] in {"completed", "failed", "canceled"}:
            return result
        time.sleep(0.01)
    raise AssertionError("history job did not finish")


def setup_function() -> None:
    download.reset_download_job_manager_for_tests()


def test_history_job_uses_only_bigqmt_single_symbol_download(monkeypatch):
    calls = []

    def native(stock, **_kwargs):
        calls.append(stock)

    monkeypatch.setattr(download, "market_data", SimpleNamespace(
        download_history_data=native,
        get_market_data_ex=lambda **kwargs: {
            stock: pd.DataFrame({"close": [1.0]}) for stock in kwargs["stock_list"]
        },
    ))
    created = download.create_history_download_job(
        HistoryDownloadJobRequest(stocks=["000001.SZ", "000001.SZ", "600000.SH"], batch_size=2)
    )
    result = _wait(created["job_id"])

    assert result["status"] == "completed"
    assert result["total"] == 2
    assert calls == ["000001.SZ", "600000.SH"]
    assert result["download_invocation_completed"] is True


def test_cancel_is_cooperative_and_keeps_native_lane_until_call_returns(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def native(stock, **_kwargs):
        calls.append(stock)
        entered.set()
        release.wait(1)

    monkeypatch.setattr(download, "market_data", SimpleNamespace(
        download_history_data=native,
        get_market_data_ex=lambda **kwargs: {
            stock: pd.DataFrame({"close": [1.0]}) for stock in kwargs["stock_list"]
        },
    ))
    created = download.create_history_download_job(
        HistoryDownloadJobRequest(stocks=["000001.SZ", "600000.SH"], batch_size=2)
    )
    assert entered.wait(1)
    canceled = download.cancel_history_download_job(created["job_id"])
    assert canceled["stop_requested"] is True
    assert canceled["status"] == "running"
    assert calls == ["000001.SZ"]

    release.set()
    result = _wait(created["job_id"])
    assert result["status"] == "canceled"
    assert calls == ["000001.SZ"]


def test_cancelled_native_call_still_hits_watchdog_and_is_not_marked_canceled(monkeypatch):
    entered = threading.Event()
    release = threading.Event()

    def native(_stock, **_kwargs):
        entered.set()
        release.wait(1)

    monkeypatch.setattr(download, "DOWNLOAD_BATCH_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(download, "DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(download, "DOWNLOAD_STOP_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(download, "market_data", SimpleNamespace(
        download_history_data=native,
        get_market_data_ex=lambda **kwargs: {
            stock: pd.DataFrame({"close": [1.0]}) for stock in kwargs["stock_list"]
        },
    ))
    created = download.create_history_download_job(HistoryDownloadJobRequest(stocks=["000001.SZ"]))
    assert entered.wait(1)
    download.cancel_history_download_job(created["job_id"])
    try:
        result = _wait(created["job_id"])
        assert result["status"] == "failed"
        assert result["native_inflight_symbols"] == ["000001.SZ"]
    finally:
        release.set()


def test_miniqmt_only_download_routes_are_not_registered():
    paths = {route.path for route in download.router.routes}
    assert "/api/download/ipo_data" not in paths
    assert "/api/download/option_data" not in paths
