import sys
import threading
import time
import os
import subprocess
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pandas as pd

xtquant_stub = ModuleType("xtquant")
xtquant_stub.market_data = SimpleNamespace()
sys.modules.setdefault("xtquant", xtquant_stub)

from qmt_bridge.server.download_state import DownloadStateStore
from qmt_bridge.server.models import HistoryDownloadJobRequest
from qmt_bridge.server.routers import download
from bigqmt_signal_trader.telemetry import bind_context


def _wait(manager, job_id, timeout=2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = manager.get(job_id)
        if payload["status"] in {"completed", "failed", "canceled", "requires_reconciliation"}:
            return payload
        time.sleep(0.01)
    raise AssertionError("job did not finish")


def _visible_data(_getter, *, stocks):
    return {stock: pd.DataFrame({"close": [1.0]}) for stock in stocks}


def test_job_deduplicates_symbols_and_persists_progress(tmp_path, monkeypatch):
    calls = []

    def native(stock, **_kwargs):
        calls.append(stock)

    monkeypatch.setattr(download, "market_data", SimpleNamespace(
        download_history_data=native,
        get_market_data_ex=lambda **kwargs: _visible_data(None, stocks=kwargs["stock_list"]),
    ))
    manager = download._DownloadJobManager(state_dir=str(tmp_path), defer_market_hours=False)
    created = manager.submit(HistoryDownloadJobRequest(stocks=["000001.SZ", "000001.SZ", "600000.SH"]))
    result = _wait(manager, created["job_id"])

    assert result["total"] == 2
    assert calls == ["000001.SZ", "600000.SH"]
    assert (tmp_path / (created["job_id"] + ".json")).exists()


def test_download_worker_restores_submit_trace_context(tmp_path, monkeypatch, trace_events):
    monkeypatch.setattr(download, "market_data", SimpleNamespace(
        download_history_data=lambda _stock, **_kwargs: None,
        get_market_data_ex=lambda **kwargs: _visible_data(None, stocks=kwargs["stock_list"]),
    ))
    manager = download._DownloadJobManager(state_dir=str(tmp_path), defer_market_hours=False)
    with bind_context(trace_id="download-trace", http_request_id="request-1"):
        created = manager.submit(HistoryDownloadJobRequest(stocks=["000001.SZ"]))
    assert _wait(manager, created["job_id"])["status"] == "completed"
    events = trace_events()
    worker_events = [item for item in events if item["event_name"] == "download_job_native_started"]
    assert worker_events and worker_events[-1]["trace_id"] == "download-trace"
    assert worker_events[-1]["http_request_id"] == "request-1"


def test_restart_marks_native_inflight_as_reconciliation_required(tmp_path):
    DownloadStateStore(tmp_path).save({
        "job_id": "interrupted",
        "status": "failed",
        "created_at": time.time(),
        "stocks": ["000001.SZ"],
        "native_inflight_symbols": ["000001.SZ"],
    })

    manager = download._DownloadJobManager(state_dir=str(tmp_path), defer_market_hours=False)
    restored = manager.get("interrupted")

    assert restored["status"] == "requires_reconciliation"
    assert "unknown" in restored["error"]


def test_restored_queued_job_rechecks_prior_success_before_skipping(tmp_path, monkeypatch):
    calls = []
    DownloadStateStore(tmp_path).save({
        "job_id": "queued",
        "status": "queued",
        "created_at": time.time(),
        "stocks": ["000001.SZ"],
        "period": "1d", "start_time": "", "end_time": "", "batch_size": 1,
        "max_attempts": 1, "total": 1, "processed": 1, "symbol_results": {
            "000001.SZ": {"status": "ok"},
        },
        "failed_symbols": [], "slow_symbols": [], "current_batch": [],
        "download_invoked_symbols": ["000001.SZ"], "native_inflight_symbols": [],
    })
    monkeypatch.setattr(download, "market_data", SimpleNamespace(
        download_history_data=lambda stock, **_kwargs: calls.append(stock),
        get_market_data_ex=lambda **kwargs: _visible_data(None, stocks=kwargs["stock_list"]),
    ))

    manager = download._DownloadJobManager(state_dir=str(tmp_path), defer_market_hours=False)
    result = _wait(manager, "queued")

    assert result["status"] == "completed"
    assert calls == []


def test_market_session_yields_before_starting_native_call(tmp_path, monkeypatch):
    open_session = {"value": True}
    calls = []
    monkeypatch.setattr(download, "market_data", SimpleNamespace(
        download_history_data=lambda stock, **_kwargs: calls.append(stock),
        get_market_data_ex=lambda **kwargs: _visible_data(None, stocks=kwargs["stock_list"]),
    ))
    manager = download._DownloadJobManager(
        state_dir=str(tmp_path), market_session=lambda: open_session["value"], defer_market_hours=True,
    )
    created = manager.submit(HistoryDownloadJobRequest(stocks=["000001.SZ"]))
    deadline = time.time() + 1
    while time.time() < deadline and manager.get(created["job_id"])["status"] != "paused_market_hours":
        time.sleep(0.01)
    assert calls == []

    open_session["value"] = False
    with manager._condition:
        manager._condition.notify_all()
    assert _wait(manager, created["job_id"])["status"] == "completed"
    assert calls == ["000001.SZ"]


def test_state_directory_has_one_process_owner(tmp_path):
    source_root = Path(__file__).resolve().parents[1] / "src"
    env = os.environ | {"PYTHONPATH": str(source_root), "DOWNLOAD_STATE_DIR": str(tmp_path)}
    holder = subprocess.Popen(
        [sys.executable, "-c", (
            "import os,time; from qmt_bridge.server.download_state import DownloadStateStore; "
            "store=DownloadStateStore(os.environ['DOWNLOAD_STATE_DIR']); print('ready', flush=True); time.sleep(3)"
        )],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        contender = subprocess.run(
            [sys.executable, "-c", (
                "import os,sys; from qmt_bridge.server.download_state import DownloadStateStore; "
                "\ntry: DownloadStateStore(os.environ['DOWNLOAD_STATE_DIR'])\n"
                "except RuntimeError: sys.exit(0)\nelse: sys.exit(1)"
            )],
            capture_output=True, text=True, env=env, timeout=5,
        )
        assert contender.returncode == 0, contender.stderr
    finally:
        holder.terminate()
        holder.wait(timeout=5)


def test_import_does_not_create_state_or_start_worker(tmp_path):
    source_root = Path(__file__).resolve().parents[1] / "src"
    env = os.environ | {
        "PYTHONPATH": str(source_root),
        "QMT_BRIDGE_DOWNLOAD_STATE_DIR": str(tmp_path),
    }
    result = subprocess.run(
        [sys.executable, "-c", (
            "from qmt_bridge.server.routers import download; "
            "assert download._download_jobs is None"
        )],
        capture_output=True, text=True, env=env, timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert list(tmp_path.iterdir()) == []


def test_sector_subprocess_environment_carries_trace_context():
    with bind_context(trace_id="sector-trace", span_id="sector-span", http_request_id="request-2"):
        env = download._sector_download_env()
    assert env["QMT_BRIDGE_TRACE_TRACE_ID"] == "sector-trace"
    assert env["QMT_BRIDGE_TRACE_SPAN_ID"] == "sector-span"
    assert env["QMT_BRIDGE_TRACE_HTTP_REQUEST_ID"] == "request-2"
