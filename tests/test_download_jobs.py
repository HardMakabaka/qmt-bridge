import sys
import time
from types import ModuleType, SimpleNamespace

import pandas as pd

xtquant_stub = ModuleType("xtquant")
xtquant_stub.xtdata = SimpleNamespace()
sys.modules.setdefault("xtquant", xtquant_stub)

from qmt_bridge.server.app import create_app
from qmt_bridge.server.models import HistoryDownloadJobRequest
from qmt_bridge.server.routers import download


def _wait_job(job_id: str, *, timeout: float = 2.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = download.get_history_download_job(job_id)
        if payload["status"] in {"completed", "failed", "timeout", "canceled"}:
            return payload
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not finish")


def setup_function():
    download.reset_download_job_manager_for_tests()


def test_sector_data_download_completes_with_progress(monkeypatch):
    calls = []

    def download_history_data2(stocks, period, start_time="", end_time="", callback=None, incrementally=None):
        calls.append((stocks, period, start_time, end_time, incrementally))
        callback({"finished": 1, "total": 1, "message": "done"})
        return {"sector": "ok"}

    monkeypatch.setattr(download, "SECTOR_DOWNLOAD_USE_SUBPROCESS", False)
    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(
            download_history_data2=download_history_data2,
            get_sector_list=lambda: ["沪深A股", "英伟达概念"],
        ),
    )

    payload = download.download_sector_data(timeout_seconds=1)

    assert payload["status"] == "ok"
    assert payload["before_sector_count"] == 2
    assert payload["after_sector_count"] == 2
    assert payload["last_progress"] == {"finished": 1, "total": 1, "message": "done"}
    assert payload["method"] == "xtdata.download_history_data2"
    assert payload["child_process_isolated"] is False
    assert payload["result"] == {"sector": "ok"}
    assert calls == [([], (2009, 86400000), "", "", None)]


def test_sector_data_download_timeout_stops_xtdata(monkeypatch):
    stopped = {"value": False}

    def download_history_data2(stocks, period, start_time="", end_time="", callback=None, incrementally=None):
        while not stopped["value"]:
            time.sleep(0.005)
        return {}

    def stop_supply_history_data2():
        stopped["value"] = True

    monkeypatch.setattr(download, "SECTOR_DOWNLOAD_USE_SUBPROCESS", False)
    monkeypatch.setattr(download, "SECTOR_DOWNLOAD_STOP_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(
            download_history_data2=download_history_data2,
            get_sector_list=lambda: ["沪深A股"],
            get_client=lambda: SimpleNamespace(stop_supply_history_data2=stop_supply_history_data2),
        ),
    )

    payload = download.download_sector_data(timeout_seconds=0.02)

    assert payload["status"] == "timeout"
    assert payload["reason"] == "qmt_sector_download_timeout"
    assert payload["before_sector_count"] == 1
    assert payload["after_sector_count"] == 1
    assert payload["last_progress"] is None
    assert payload["child_process_isolated"] is False
    assert payload["thread_alive"] is False
    assert stopped["value"] is True


def test_sector_data_download_subprocess_timeout_is_bounded(monkeypatch):
    def execute_sector_download_process(timeout_seconds):
        return (
            "timeout",
            {
                "method": "xtdata.download_sector_data",
                "timeout_seconds": timeout_seconds,
                "stdout_tail": "before_sector_count=36",
                "stderr_tail": "",
                "child_process_isolated": True,
                "child_process_killed": True,
            },
        )

    monkeypatch.setattr(download, "SECTOR_DOWNLOAD_USE_SUBPROCESS", True)
    monkeypatch.setattr(download, "_execute_sector_download_process", execute_sector_download_process)
    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(get_sector_list=lambda: ["沪深A股"]),
    )

    payload = download.download_sector_data(timeout_seconds=0.02)

    assert payload["status"] == "timeout"
    assert payload["reason"] == "qmt_sector_download_timeout"
    assert payload["before_sector_count"] == 1
    assert payload["after_sector_count"] == 1
    assert payload["method"] == "xtdata.download_sector_data"
    assert payload["child_process_isolated"] is True
    assert payload["child_process_killed"] is True
    assert payload["thread_alive"] is False
    assert payload["stdout_tail"] == "before_sector_count=36"


def test_sector_data_download_busy_when_previous_thread_survives_timeout(monkeypatch):
    release = {"value": False}

    def download_history_data2(stocks, period, start_time="", end_time="", callback=None, incrementally=None):
        while not release["value"]:
            time.sleep(0.005)
        return {}

    monkeypatch.setattr(download, "SECTOR_DOWNLOAD_USE_SUBPROCESS", False)
    monkeypatch.setattr(download, "SECTOR_DOWNLOAD_STOP_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(
            download_history_data2=download_history_data2,
            get_sector_list=lambda: ["沪深A股"],
            get_client=lambda: SimpleNamespace(stop_supply_history_data2=lambda: None),
        ),
    )

    first = download.download_sector_data(timeout_seconds=0.02)
    second = download.download_sector_data(timeout_seconds=0.02)

    assert first["status"] == "timeout"
    assert first["thread_alive"] is True
    assert second["status"] == "busy"
    assert second["reason"] == "qmt_sector_download_already_running"
    assert second["thread_alive"] is True

    release["value"] = True
    deadline = time.time() + 1
    while time.time() < deadline:
        active_thread = download._sector_download_active.get("thread")
        if active_thread is None or not active_thread.is_alive():
            break
        time.sleep(0.01)


def test_history_download_job_completes_with_progress(monkeypatch):
    calls = []

    def download_history_data2(stocks, *, period, start_time, end_time, callback):
        calls.append((stocks, period, start_time, end_time))
        callback({"finished": len(stocks), "total": len(stocks)})
        return {stock: {"ok": True} for stock in stocks}

    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(download_history_data2=download_history_data2),
    )

    created = download.create_history_download_job(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ", "600000.SH"],
            period="1d",
            start_time="20260620",
            end_time="20260629",
            batch_size=2,
        )
    )
    job = _wait_job(created["job_id"])

    assert job["status"] == "completed"
    assert job["processed"] == 2
    assert job["failed_symbols"] == []
    assert job["last_progress"] == {"finished": 2, "total": 2}
    assert calls == [(["000001.SZ", "600000.SH"], "1d", "20260620", "20260629")]


def test_slow_batch_is_split_to_single_symbol_retry(monkeypatch):
    calls = []
    stopped = {"value": False}

    def download_history_data2(stocks, *, period, start_time, end_time, callback):
        calls.append(tuple(stocks))
        if len(stocks) > 1:
            while not stopped["value"]:
                time.sleep(0.005)
            return {}
        callback({"finished": 1, "total": 1})
        return {stocks[0]: {"ok": True}}

    def stop_supply_history_data2():
        stopped["value"] = True

    monkeypatch.setattr(download, "DOWNLOAD_BATCH_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(download, "DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(download, "DOWNLOAD_STOP_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(
            download_history_data2=download_history_data2,
            get_client=lambda: SimpleNamespace(stop_supply_history_data2=stop_supply_history_data2),
        ),
    )

    created = download.create_history_download_job(
        HistoryDownloadJobRequest(stocks=["000001.SZ", "600000.SH"], batch_size=2)
    )
    job = _wait_job(created["job_id"])

    assert job["status"] == "completed"
    assert set(job["slow_symbols"]) == {"000001.SZ", "600000.SH"}
    assert job["failed_symbols"] == []
    assert calls[0] == ("000001.SZ", "600000.SH")
    assert ("000001.SZ",) in calls
    assert ("600000.SH",) in calls


def test_single_symbol_failure_is_recorded_without_blocking(monkeypatch):
    def download_history_data2(stocks, *, period, start_time, end_time, callback):
        if stocks == ["600599.SH"]:
            raise RuntimeError("bad symbol")
        callback({"finished": 1, "total": 1})
        return {stocks[0]: {"ok": True}}

    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(download_history_data2=download_history_data2),
    )

    created = download.create_history_download_job(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ", "600599.SH"],
            batch_size=1,
            max_attempts=1,
        )
    )
    job = _wait_job(created["job_id"])

    assert job["status"] == "completed"
    assert job["processed"] == 2
    assert job["failed_symbols"] == ["600599.SH"]
    assert job["symbol_results"]["000001.SZ"]["status"] == "ok"
    assert job["symbol_results"]["600599.SH"]["status"] == "error"


def test_history_download_job_fails_when_validation_has_no_rows(monkeypatch):
    def download_history_data2(stocks, *, period, start_time, end_time, callback):
        callback({"finished": len(stocks), "total": len(stocks)})
        return {stock: {"ok": True} for stock in stocks}

    def get_market_data_ex(**kwargs):
        return {stock: pd.DataFrame() for stock in kwargs["stock_list"]}

    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(
            download_history_data2=download_history_data2,
            get_market_data_ex=get_market_data_ex,
        ),
    )

    created = download.create_history_download_job(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ", "300750.SZ"],
            period="1m",
            start_time="20250102093000",
            end_time="20250102143000",
            batch_size=2,
            max_attempts=1,
        )
    )
    job = _wait_job(created["job_id"])

    assert job["status"] == "failed"
    assert set(job["failed_symbols"]) == {"000001.SZ", "300750.SZ"}
    assert {result["status"] for result in job["symbol_results"].values()} == {
        "target_missing"
    }


def test_history_download_job_accepts_verified_nonempty_rows(monkeypatch):
    def download_history_data2(stocks, *, period, start_time, end_time, callback):
        callback({"finished": len(stocks), "total": len(stocks)})
        return {stock: {"ok": True} for stock in stocks}

    def get_market_data_ex(**kwargs):
        return {
            stock: pd.DataFrame(
                [{"open": 10.0, "high": 10.2, "low": 9.9, "close": 10.1}]
            )
            for stock in kwargs["stock_list"]
        }

    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(
            download_history_data2=download_history_data2,
            get_market_data_ex=get_market_data_ex,
        ),
    )

    created = download.create_history_download_job(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ"],
            period="1m",
            start_time="20260515093000",
            end_time="20260515143000",
            batch_size=1,
        )
    )
    job = _wait_job(created["job_id"])

    assert job["status"] == "completed"
    assert job["failed_symbols"] == []
    assert job["symbol_results"]["000001.SZ"]["status"] == "ok"


def test_history_download_jobs_are_fifo(monkeypatch):
    calls = []

    def download_history_data2(stocks, *, period, start_time, end_time, callback):
        calls.append(stocks[0])
        time.sleep(0.02)
        callback({"finished": 1, "total": 1})
        return {}

    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(download_history_data2=download_history_data2),
    )

    first = download.create_history_download_job(HistoryDownloadJobRequest(stocks=["000001.SZ"]))
    second = download.create_history_download_job(HistoryDownloadJobRequest(stocks=["600000.SH"]))

    _wait_job(first["job_id"])
    _wait_job(second["job_id"])

    assert calls == ["000001.SZ", "600000.SH"]


def test_cancel_queued_history_download_job(monkeypatch):
    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(download_history_data2=lambda _stocks, **_kwargs: {}),
    )

    created = download.create_history_download_job(HistoryDownloadJobRequest(stocks=["000001.SZ"]))
    canceled = download.cancel_history_download_job(created["job_id"])

    assert canceled["stop_requested"] is True
    assert canceled["status"] in {"queued", "running", "canceled", "completed"}


def test_legacy_batch_route_is_not_registered():
    app = create_app()
    legacy_path = "/api/download/" + "batch"
    paths = {
        (route.path, tuple(sorted(getattr(route, "methods", []) or [])))
        for route in app.routes
    }

    assert not any(path == legacy_path for path, _methods in paths)
    assert any(path == "/api/download/jobs" for path, _methods in paths)
