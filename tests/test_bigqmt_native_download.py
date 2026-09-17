import importlib
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from bigqmt_signal_trader import download_jobs
from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.data_client import BigQmtDataClient
from qmt_bridge.server.models import DownloadRequest, HistoryDownloadJobRequest
from qmt_bridge.server.routers import download as download_router
from qmt_bridge.server.routers import legacy as legacy_router


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}

    def setex(self, key, _ttl, value):
        self.values[key] = value

    def set(self, key, value):
        self.values[key] = value

    def get(self, key):
        return self.values.get(key)

    def delete(self, key):
        self.values.pop(key, None)

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    def lpop(self, key):
        values = self.lists.get(key) or []
        return values.pop(0) if values else None

    def expire(self, _key, _ttl):
        return True


class NativeDownloadRpcFake:
    local_cache_config = {"enabled": False}

    def __init__(self, *, rows_visible=True, on_download=None):
        self.rows_visible = rows_visible
        self.on_download = on_download
        self.calls = []

    def call(self, method, params=None, timeout_seconds=None, **_kwargs):
        params = dict(params or {})
        self.calls.append((method, params, timeout_seconds))
        if method == "download_history_data":
            if self.on_download is not None:
                self.on_download(params)
            return None
        if method == "get_market_data_ex":
            return {
                stock: (
                    pd.DataFrame({"close": [10.0], "volume": [100.0]})
                    if self.rows_visible
                    else pd.DataFrame()
                )
                for stock in params["stock_list"]
            }
        raise AssertionError("unexpected RPC method: %s" % method)


def _wait_http_job(manager, job_id, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = manager.get(job_id)
        if payload["status"] in {"completed", "failed", "timeout", "canceled"}:
            return payload
        time.sleep(0.01)
    raise AssertionError("native HTTP download job did not finish")


def test_provider_calls_only_injected_bigqmt_global_with_official_four_arguments():
    calls = []

    class Context:
        def download_history_data(self, **_kwargs):
            pytest.fail("ContextInfo is not the official Big QMT download API")

    class MiniXtdata:
        def download_history_data(self, *_args, **_kwargs):
            pytest.fail("MiniQMT xtdata must not be used by the Big QMT provider")

    def global_download(*args):
        calls.append(args)

    result = BigQmtMarketDataProvider(
        Context(),
        native_xtdata=MiniXtdata(),
        native_history_downloader=global_download,
    ).download_history_data(
        "600519.SH", "1m", "20260910093000", "20260910150000", True
    )

    assert result is None
    assert calls == [
        ("600519.SH", "1m", "20260910093000", "20260910150000")
    ]


def test_provider_fails_closed_without_injected_bigqmt_global():
    with pytest.raises(
        NotImplementedError, match="bigqmt_global_download_history_data_unavailable"
    ):
        BigQmtMarketDataProvider(object()).download_history_data(
            "600519.SH", "1m", "", ""
        )


def test_provider_propagates_native_download_exception():
    def broken(*_args):
        raise RuntimeError("terminal rejected download")

    provider = BigQmtMarketDataProvider(
        object(), native_history_downloader=broken
    )
    with pytest.raises(RuntimeError, match="terminal rejected download"):
        provider.download_history_data("600519.SH", "1m", "", "")


def _handlers(provider):
    return BigQmtRpcHandlers(
        account_id="acct",
        market_data=provider,
        position_provider=object(),
    )


def test_rpc_advertises_single_symbol_download_only_when_global_is_available():
    unavailable = _handlers(BigQmtMarketDataProvider(object()))
    available = _handlers(
        BigQmtMarketDataProvider(
            object(), native_history_downloader=lambda *_args: None
        )
    )

    assert "download_history_data" not in unavailable.allowed_methods
    assert "download_history_data2" not in unavailable.allowed_methods
    assert unavailable.handle("ping")["native_history_download_available"] is False
    with pytest.raises(ValueError, match="not allowed"):
        unavailable.handle("download_history_data", {})

    assert "download_history_data" in available.allowed_methods
    assert "download_history_data2" not in available.allowed_methods
    assert available.handle("ping")["native_history_download_available"] is True


def test_rpc_cannot_force_advertise_missing_download_with_allowed_methods_override():
    handlers = BigQmtRpcHandlers(
        account_id="acct",
        market_data=BigQmtMarketDataProvider(object()),
        position_provider=object(),
        allowed_methods={"ping", "download_history_data", "download_history_data2"},
    )

    assert handlers.allowed_methods == {"ping"}


def test_rpc_does_not_advertise_captured_global_when_method_is_explicitly_disabled():
    handlers = BigQmtRpcHandlers(
        account_id="acct",
        market_data=BigQmtMarketDataProvider(
            object(), native_history_downloader=lambda *_args: None
        ),
        position_provider=object(),
        allowed_methods={"ping"},
    )

    assert handlers.handle("ping")["native_history_download_available"] is False


def test_strategy_captures_runtime_injected_global_download_function(monkeypatch):
    strategy = importlib.import_module("bigqmt_signal_trader_strategy")
    marker = lambda *_args: None
    monkeypatch.setattr(strategy, "download_history_data", marker, raising=False)
    monkeypatch.setattr(strategy, "_config", {})
    monkeypatch.setattr(strategy, "_qmt_api", {})

    config = strategy._build_config()

    assert config["qmt_api"]["download_history_data"] is marker


def test_strategy_builds_rpc_provider_with_captured_global_downloader():
    strategy = importlib.import_module("bigqmt_signal_trader_strategy")
    marker = lambda *_args: None
    config = {
        "enable_rpc": True,
        "account_id": "acct",
        "qmt_api": {"download_history_data": marker},
        "rpc": {
            "enabled": True,
            "redis_client": object(),
            "response_redis_client": object(),
        },
    }

    service = strategy._build_rpc_service(
        object(),
        SimpleNamespace(order_gateway=None, position_sync_sink=None),
        config,
    )

    assert service.handlers.market_data.native_history_download_available is True
    assert "download_history_data" in service.handlers.allowed_methods


@pytest.mark.parametrize("inject_global", [True, False])
def test_full_dryrun_entry_propagates_only_an_injected_download_global(
    inject_global,
):
    """Execute the real QMT entry shape in an isolated interpreter.

    The compiled QMT wrapper executes this entry with runtime functions in its
    globals. Local strategy modules are separate module dictionaries, so the
    entry must explicitly forward the callable through bind_runtime_api.
    """
    root = Path(__file__).resolve().parents[1]
    script = """
import sys
import tempfile
from pathlib import Path
root = Path({root!r})
source_entry = root / 'src' / 'BIGQMT_REDIS_DRYRUN.py'
overlay = root / 'src' / 'mecostock_bigqmt_mode_overlay.py'
wrapper = root / 'src' / 'MECOSTOCK_BIGQMT_ZMQ.py'
marker = lambda *_args: None
with tempfile.TemporaryDirectory() as temp_dir:
    installed_entry = Path(temp_dir) / 'MECOSTOCK_BIGQMT_BRIDGE.py'
    installed_entry.write_bytes(
        source_entry.read_bytes() + b'\\r\\n\\r\\n' + overlay.read_bytes()
    )
    sys.path.insert(0, temp_dir)
    sys.path.insert(1, str(root / 'src'))
    namespace = {{
        '__file__': str(Path(temp_dir) / 'MECOSTOCK_BIGQMT_ZMQ.py'),
        '__name__': '__main__',
    }}
    if {inject_global!r}:
        namespace['download_history_data'] = marker
    with wrapper.open('rb') as source_file:
        wrapper_source = source_file.read()
    exec(compile(wrapper_source, str(wrapper), 'exec'), namespace, namespace)
    captured = namespace['_strategy']._qmt_api.get('download_history_data')
    print('NATIVE_DOWNLOAD_CAPTURED=' + ('1' if captured is marker else '0'))
""".format(root=str(root), inject_global=inject_global)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    expected = "1" if inject_global else "0"
    assert "NATIVE_DOWNLOAD_CAPTURED=" + expected in completed.stdout


def test_job_defaults_to_single_symbol_native_download_and_does_not_claim_coverage(
    monkeypatch,
):
    redis = FakeRedis()
    calls = []

    class Provider:
        def download_history_data(self, *args):
            calls.append(args)
            return None

        def download_history_data2(self, *_args):
            pytest.fail("Big QMT job must not use the MiniQMT batch API")

    created = download_jobs.submit_download_job(
        redis,
        "acct",
        ["000001.SZ", "600000.SH", "600519.SH"],
        "1m",
        start_time="20260910093000",
        end_time="20260910150000",
        chunk_size=2,
    )
    monkeypatch.setattr(download_jobs.time, "time", lambda: 1.0)

    first = download_jobs.pump_download_jobs(
        redis, Provider(), "acct", max_wall_seconds=60
    )
    stored = download_jobs.read_download_status(redis, "acct", created["job_id"])

    assert created["method"] == "download_history_data"
    assert first["state"] == download_jobs.RUNNING
    assert first["done"] == 2
    assert len(calls) == 2
    assert all(len(args) == 5 for args in calls)
    assert stored["download_invocation_completed"] is False
    assert stored["coverage_verified"] is False

    second = download_jobs.pump_download_jobs(
        redis, Provider(), "acct", max_wall_seconds=60
    )
    stored = download_jobs.read_download_status(redis, "acct", created["job_id"])

    assert second["state"] == download_jobs.DONE
    assert second["done"] == 3
    assert stored["download_invocation_completed"] is True
    assert stored["coverage_verified"] is False


def test_job_rejects_mininqmt_batch_method_instead_of_falling_back():
    redis = FakeRedis()
    created = download_jobs.submit_download_job(
        redis,
        "acct",
        ["000001.SZ"],
        "1m",
        method="download_history_data2",
    )

    result = download_jobs.pump_download_jobs(redis, object(), "acct")

    assert result["state"] == download_jobs.FAILED
    assert result["done"] == 0
    assert "unsupported_bigqmt_download_method" in result["error"]
    stored = download_jobs.read_download_status(redis, "acct", created["job_id"])
    assert stored["coverage_verified"] is False


def test_bigqmt_facade_passes_timeout_as_metadata_not_native_payload():
    client = NativeDownloadRpcFake()
    facade = BigQmtDataClient(client)

    result = facade.download_history_data(
        "600519.SH",
        "1m",
        "20260910093000",
        "20260910150000",
        incrementally=True,
        dividend_type="front",
        timeout_seconds=1.25,
    )

    assert result is None
    assert client.calls == [
        (
            "download_history_data",
            {
                "stock_code": "600519.SH",
                "period": "1m",
                "start_time": "20260910093000",
                "end_time": "20260910150000",
            },
            1.25,
        )
    ]


def test_bigqmt_facade_maps_missing_remote_global_to_unsupported():
    class MissingGlobalClient:
        def call(self, *_args, **_kwargs):
            raise RuntimeError(
                "rpc method is not allowed: download_history_data"
            )

    with pytest.raises(
        NotImplementedError, match="native_history_download_unavailable"
    ):
        BigQmtDataClient(MissingGlobalClient()).download_history_data(
            "600519.SH", "1m"
        )


def test_http_manager_uses_real_bigqmt_facade_single_calls_and_verifies_rows(monkeypatch):
    client = NativeDownloadRpcFake(rows_visible=True)
    monkeypatch.setattr(download_router, "market_data", BigQmtDataClient(client))
    manager = download_router._DownloadJobManager(state_dir="", defer_market_hours=False)

    created = manager.submit(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ", "600000.SH"],
            period="1m",
            start_time="20260910093000",
            end_time="20260910150000",
            batch_size=2,
            max_attempts=1,
        )
    )
    result = _wait_http_job(manager, created["job_id"])

    calls = [call for call in client.calls if call[0] == "download_history_data"]
    assert result["status"] == "completed"
    assert result["download_invocation_completed"] is True
    assert result["history_visibility_verified"] is True
    assert result["coverage_verified"] is False
    assert [call[1]["stock_code"] for call in calls] == [
        "000001.SZ",
        "600000.SH",
    ]
    assert all(0 < call[2] <= download_router.DOWNLOAD_BATCH_TIMEOUT_SECONDS for call in calls)
    assert all("timeout_seconds" not in call[1] for call in calls)
    assert all(call[0] != "download_history_data2" for call in client.calls)


def test_http_manager_none_download_result_does_not_verify_missing_rows(monkeypatch):
    client = NativeDownloadRpcFake(rows_visible=False)
    monkeypatch.setattr(download_router, "market_data", BigQmtDataClient(client))
    monkeypatch.setattr(download_router, "DOWNLOAD_HISTORY_VISIBILITY_ATTEMPTS", 1)
    manager = download_router._DownloadJobManager(state_dir="", defer_market_hours=False)

    created = manager.submit(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ"], max_attempts=1
        )
    )
    result = _wait_http_job(manager, created["job_id"])

    assert result["status"] == "failed"
    assert result["download_invocation_completed"] is True
    assert result["history_visibility_verified"] is False
    assert result["coverage_verified"] is False
    assert result["symbol_results"]["000001.SZ"]["status"] == "target_missing"


def test_http_manager_cancel_after_first_native_symbol_stops_next_call(monkeypatch):
    manager = download_router._DownloadJobManager(state_dir="", defer_market_hours=False)

    def request_cancel(_params):
        with manager._condition:
            for job in manager._jobs.values():
                job["stop_requested"] = True

    client = NativeDownloadRpcFake(rows_visible=True, on_download=request_cancel)
    monkeypatch.setattr(download_router, "market_data", BigQmtDataClient(client))

    created = manager.submit(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ", "600000.SH"], batch_size=2, max_attempts=1
        )
    )
    result = _wait_http_job(manager, created["job_id"])

    calls = [call for call in client.calls if call[0] == "download_history_data"]
    assert result["status"] == "canceled"
    assert len(calls) == 1
    assert result["download_invocation_completed"] is False
    assert result["coverage_verified"] is False


def test_http_manager_batch_budget_expiry_stops_before_next_native_symbol(monkeypatch):
    def consume_budget(_params):
        time.sleep(0.03)

    client = NativeDownloadRpcFake(rows_visible=True, on_download=consume_budget)
    monkeypatch.setattr(download_router, "market_data", BigQmtDataClient(client))
    monkeypatch.setattr(download_router, "DOWNLOAD_BATCH_TIMEOUT_SECONDS", 0.02)
    manager = download_router._DownloadJobManager(state_dir="", defer_market_hours=False)

    created = manager.submit(
        HistoryDownloadJobRequest(
            stocks=["000001.SZ", "600000.SH"], batch_size=2, max_attempts=1
        )
    )
    result = _wait_http_job(manager, created["job_id"])

    calls = [call for call in client.calls if call[0] == "download_history_data"]
    assert result["status"] == "failed"
    assert len(calls) == 1
    assert 0 < calls[0][2] <= 0.02
    assert result["download_invocation_completed"] is False
    assert result["coverage_verified"] is False


def test_legacy_single_download_reports_invocation_separately_from_coverage(monkeypatch):
    client = NativeDownloadRpcFake()
    monkeypatch.setattr(legacy_router, "market_data", BigQmtDataClient(client))

    result = legacy_router.download_data(
        DownloadRequest(
            stock="600519.SH",
            period="1m",
            start="20260910093000",
            end="20260910150000",
        )
    )

    assert result["status"] == "download_invoked"
    assert result["download_invocation_completed"] is True
    assert result["history_visibility_verified"] is False
    assert result["coverage_verified"] is False
    assert client.calls[0][0] == "download_history_data"
