import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import ModuleType, SimpleNamespace

import pandas as pd

xtquant_stub = ModuleType("xtquant")
xtquant_stub.xtdata = SimpleNamespace()
sys.modules.setdefault("xtquant", xtquant_stub)

from qmt_bridge import QMTClient
from qmt_bridge.server.binary_cache import BinaryCache, reset_binary_cache
from qmt_bridge.server import singleton
from qmt_bridge.server.config import Settings
from qmt_bridge.server.helpers import _reset_xtdata_transport_for_tests
from qmt_bridge.server.routers import (
    calendar,
    download,
    etf,
    instrument,
    market,
    option,
    sector,
    utility,
)
from qmt_bridge.server.trading.manager import XtTraderManager


def setup_function():
    _reset_xtdata_transport_for_tests()


def test_default_client_and_server_port_is_fixed_13543(monkeypatch):
    monkeypatch.delenv("QMT_BRIDGE_PORT", raising=False)
    monkeypatch.delenv("QMT_BRIDGE_SINGLETON", raising=False)

    settings = Settings.from_env()
    client = QMTClient("127.0.0.1")

    assert settings.port == 13543
    assert settings.singleton is True
    assert client.base_url == "http://127.0.0.1:13543"


def test_instrument_total_share_reports_missing_xtdata_function(monkeypatch):
    monkeypatch.setattr(instrument, "xtdata", SimpleNamespace())

    payload = instrument.get_total_share(stocks="000001.SZ")

    assert payload["status"] == "unsupported"
    assert payload["data"]["000001.SZ"]["function"] == "get_total_share"
    assert payload["unit_contract"]["warning"] == "stock share capital; never treat as ETF fund shares"


def test_instrument_total_share_calls_xtdata_function(monkeypatch):
    def get_total_share(stock):
        return {"stock": stock, "total_share": 123}

    monkeypatch.setattr(instrument, "xtdata", SimpleNamespace(get_total_share=get_total_share))

    payload = instrument.get_total_share(stocks="000001.SZ")

    assert payload["status"] == "ok"
    assert payload["data"]["000001.SZ"]["data"] == {"stock": "000001.SZ", "total_share": 123}


def test_fund_iopv_reports_per_symbol_unsupported(monkeypatch):
    monkeypatch.setattr(etf, "xtdata", SimpleNamespace())

    payload = etf.get_fund_iopv(stocks="510300.SH")

    assert payload["status"] == "unsupported"
    assert payload["data"]["510300.SH"]["function"] == "get_etf_iopv"
    assert payload["unit_contract"]["shares"] == "not_provided_by_qmt_iopv"


def test_utility_industry_name_invokes_qmt_argument_order(monkeypatch):
    calls = []

    def get_industry_name_of_stock(industry_type, stock):
        calls.append((industry_type, stock))
        return "银行"

    monkeypatch.setattr(
        utility,
        "xtdata",
        SimpleNamespace(get_industry_name_of_stock=get_industry_name_of_stock),
    )

    payload = utility.get_industry_name(stock="000001.SZ", industry_type="SW2")

    assert calls == [("SW2", "000001.SZ")]
    assert payload == {
        "status": "ok",
        "stock": "000001.SZ",
        "industry_type": "SW2",
        "data": "银行",
    }


def test_sector_stocks_preserves_yyyymmdd_real_timetag(monkeypatch):
    calls = []

    def get_stock_list_in_sector(sector_name, real_timetag=-1):
        calls.append((sector_name, real_timetag))
        return ["000001.SZ"]

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(get_stock_list_in_sector=get_stock_list_in_sector),
    )

    payload = sector.get_sector_stocks(sector="英伟达概念", real_timetag="20240102")

    assert calls == [("英伟达概念", "20240102")]
    assert payload["real_timetag"] == "20240102"
    assert payload["stocks"] == ["000001.SZ"]


def test_sector_stock_memberships_filters_keyword_and_preserves_date(monkeypatch):
    calls = []

    def get_sector_list():
        return ["英伟达概念", "CPO", "沪深A股"]

    def get_stock_list_in_sector(sector_name, real_timetag=-1):
        calls.append((sector_name, real_timetag))
        if sector_name == "英伟达概念":
            return ["000001.SZ", "300001.SZ"]
        if sector_name == "CPO":
            return ["300001.SZ"]
        return ["000001.SZ", "600000.SH"]

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(
            get_sector_list=get_sector_list,
            get_stock_list_in_sector=get_stock_list_in_sector,
        ),
    )

    payload = sector.get_stock_sector_memberships(
        stock="000001",
        real_timetag="20240102",
        keyword="英伟达",
    )

    assert calls == [("英伟达概念", "20240102")]
    assert payload["status"] == "ok"
    assert payload["sector_count"] == 1
    assert payload["matched_count"] == 1
    assert payload["sectors"] == ["英伟达概念"]


def test_sector_stock_memberships_reports_keyword_cache_miss(monkeypatch):
    calls = []

    def get_sector_list():
        return ["沪深A股", "创业板"]

    def get_stock_list_in_sector(sector_name, real_timetag=-1):
        calls.append((sector_name, real_timetag))
        return ["000001.SZ"]

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(
            get_sector_list=get_sector_list,
            get_stock_list_in_sector=get_stock_list_in_sector,
        ),
    )

    payload = sector.get_stock_sector_memberships(
        stock="000001.SZ",
        real_timetag="20240102",
        keyword="英伟达",
    )

    assert calls == []
    assert payload["status"] == "unavailable"
    assert payload["reason"] == "qmt_sector_keyword_no_match"
    assert payload["matched_count"] == 0


def test_market_subscribe_warmup_unsubscribes_by_default(monkeypatch):
    calls = []

    def subscribe_quote(stock, *, period, start_time, end_time, count):
        calls.append(("subscribe", stock, period, start_time, end_time, count))
        return 42

    def unsubscribe_quote(seq):
        calls.append(("unsubscribe", seq))
        return True

    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(subscribe_quote=subscribe_quote, unsubscribe_quote=unsubscribe_quote),
    )

    payload = market.subscribe_warmup({"stocks": ["000001.SZ"], "period": "1m"})

    assert payload["status"] == "ok"
    assert payload["data"]["seq"] == 42
    assert payload["data"]["unsubscribe"]["status"] == "ok"
    assert calls == [
        ("subscribe", "000001.SZ", "1m", "", "", 0),
        ("unsubscribe", 42),
    ]


def test_market_unsubscribe_allows_zero_seq_for_live_validation(monkeypatch):
    calls = []

    def unsubscribe_quote(seq):
        calls.append(seq)
        return False

    monkeypatch.setattr(market, "xtdata", SimpleNamespace(unsubscribe_quote=unsubscribe_quote))

    payload = market.unsubscribe_quote({"seq": 0})

    assert calls == [0]
    assert payload["status"] == "ok"
    assert payload["data"] is False


def test_market_history_ex_uses_local_binary_cache(monkeypatch, tmp_path):
    calls = []

    def get_market_data_ex(**kwargs):
        calls.append(kwargs)
        return {
            "000001.SZ": pd.DataFrame(
                [{"time": "20260623", "open": 1.0, "close": 2.0}]
            )
        }

    reset_binary_cache(
        BinaryCache(enabled=True, cache_dir=tmp_path, ttl_seconds=3600, max_bytes=10_000_000)
    )
    monkeypatch.setattr(market, "xtdata", SimpleNamespace(get_market_data_ex=get_market_data_ex))
    try:
        first = market.get_history_ex(
            stocks="000001.SZ",
            period="1d",
            start_time="20260623",
            end_time="20260623",
            use_cache=True,
        )
        second = market.get_history_ex(
            stocks="000001.SZ",
            period="1d",
            start_time="20260623",
            end_time="20260623",
            use_cache=True,
        )
    finally:
        reset_binary_cache(None)

    assert len(calls) == 1
    assert first == second
    assert first["data"]["000001.SZ"][0]["open"] == 1.0
    assert list(tmp_path.rglob("*.pkl"))


def test_market_history_ex_does_not_cache_empty_history(monkeypatch, tmp_path):
    calls = []

    def get_market_data_ex(**kwargs):
        calls.append(kwargs)
        return {"000001.SZ": pd.DataFrame()}

    reset_binary_cache(
        BinaryCache(enabled=True, cache_dir=tmp_path, ttl_seconds=3600, max_bytes=10_000_000)
    )
    monkeypatch.setattr(market, "xtdata", SimpleNamespace(get_market_data_ex=get_market_data_ex))
    try:
        market.get_history_ex(stocks="000001.SZ", start_time="20260623", end_time="20260623", use_cache=True)
        market.get_history_ex(stocks="000001.SZ", start_time="20260623", end_time="20260623", use_cache=True)
    finally:
        reset_binary_cache(None)

    assert len(calls) == 2
    assert not list(tmp_path.rglob("*.pkl"))


def test_binary_cache_coalesces_concurrent_identical_misses(tmp_path):
    cache = BinaryCache(
        enabled=True,
        cache_dir=tmp_path,
        ttl_seconds=3600,
        max_bytes=10_000_000,
    )
    loader_started = threading.Event()
    duplicate_loader_started = threading.Event()
    release_loader = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def loader():
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        loader_started.set()
        if call_number > 1:
            duplicate_loader_started.set()
        assert release_loader.wait(1)
        return {"value": 1}

    def cached_call():
        return cache.cached_call("same", {"stock": "000001.SZ"}, loader)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(cached_call)
        assert loader_started.wait(1)
        second = executor.submit(cached_call)
        try:
            assert not duplicate_loader_started.wait(0.1)
        finally:
            release_loader.set()
        first_result = first.result(timeout=1)
        second_result = second.result(timeout=1)

    assert calls == 1
    assert first_result[0] == {"value": 1}
    assert second_result[0] == {"value": 1}


def test_market_and_download_xtdata_calls_are_serialized(monkeypatch):
    market_call_started = threading.Event()
    download_call_started = threading.Event()
    release_market_call = threading.Event()

    def get_market_data_ex(**_kwargs):
        market_call_started.set()
        assert release_market_call.wait(1)
        return {}

    def download_index_weight():
        download_call_started.set()

    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(get_market_data_ex=get_market_data_ex),
    )
    monkeypatch.setattr(
        download,
        "xtdata",
        SimpleNamespace(download_index_weight=download_index_weight),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        market_result = executor.submit(
            market.get_history_ex,
            stocks="000001.SZ",
            period="1d",
            start_time="",
            end_time="",
            count=-1,
            dividend_type="none",
            fill_data=True,
            use_cache=False,
        )
        assert market_call_started.wait(1)
        download_result = executor.submit(download.download_index_weight)
        try:
            assert not download_call_started.wait(0.1)
        finally:
            release_market_call.set()
        assert market_result.result(timeout=1) == {"data": {}}
        assert download_result.result(timeout=1) == {"status": "ok"}

    assert download_call_started.is_set()


def test_market_and_calendar_xtdata_calls_are_serialized(monkeypatch):
    market_call_started = threading.Event()
    calendar_call_started = threading.Event()
    release_market_call = threading.Event()

    def get_market_data_ex(**_kwargs):
        market_call_started.set()
        assert release_market_call.wait(1)
        return {}

    def get_trading_dates(*_args, **_kwargs):
        calendar_call_started.set()
        return [20260721]

    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(get_market_data_ex=get_market_data_ex),
    )
    monkeypatch.setattr(
        calendar,
        "xtdata",
        SimpleNamespace(get_trading_dates=get_trading_dates),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        market_result = executor.submit(
            market.get_history_ex,
            stocks="000001.SZ",
            period="1d",
            start_time="",
            end_time="",
            count=-1,
            dividend_type="none",
            fill_data=True,
            use_cache=False,
        )
        assert market_call_started.wait(1)
        calendar_result = executor.submit(
            calendar.get_trading_dates,
            market="SH",
            start_time="20260721",
            end_time="20260721",
            count=-1,
        )
        try:
            assert not calendar_call_started.wait(0.1)
        finally:
            release_market_call.set()
        assert market_result.result(timeout=1) == {"data": {}}
        assert calendar_result.result(timeout=1) == {
            "market": "SH",
            "dates": [20260721],
        }

    assert calendar_call_started.is_set()


def test_market_history_ex_reports_none_payload_as_unavailable(monkeypatch):
    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(get_market_data_ex=lambda **_kwargs: None),
    )

    payload = market.get_history_ex(
        stocks="000001.SZ",
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        use_cache=False,
    )

    assert payload["status"] == "unavailable"
    assert payload["data"] is None
    assert payload["reason_code"] == "xtdata_get_market_data_ex_returned_none"
    assert payload["function"] == "get_market_data_ex"
    assert payload["retryable"] is True


def test_market_local_data_reports_non_mapping_payload_as_error(monkeypatch):
    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(get_local_data=lambda **_kwargs: []),
    )

    payload = market.get_local_data(
        stocks="000001.SZ",
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        use_cache=False,
    )

    assert payload["status"] == "error"
    assert payload["data"] is None
    assert payload["reason_code"] == "xtdata_get_local_data_invalid_response"
    assert payload["function"] == "get_local_data"
    assert payload["error_type"] == "list"
    assert payload["retryable"] is False


def test_market_divid_factors_caches_nonempty_dataframe(monkeypatch, tmp_path):
    calls = []

    def get_divid_factors(stock, *, start_time, end_time):
        calls.append((stock, start_time, end_time))
        return pd.DataFrame([{"time": "20250101", "factor": 1.0}])

    reset_binary_cache(
        BinaryCache(enabled=True, cache_dir=tmp_path, ttl_seconds=3600, max_bytes=10_000_000)
    )
    monkeypatch.setattr(market, "xtdata", SimpleNamespace(get_divid_factors=get_divid_factors))
    try:
        first = market.get_divid_factors(
            stock="000001.SZ",
            start_time="20240101",
            end_time="20260101",
            use_cache=True,
        )
        second = market.get_divid_factors(
            stock="000001.SZ",
            start_time="20240101",
            end_time="20260101",
            use_cache=True,
        )
    finally:
        reset_binary_cache(None)

    assert calls == [("000001.SZ", "20240101", "20260101")]
    assert first == second
    assert first["data"][0]["factor"] == 1.0


def test_option_list_reports_missing_option_sector_data_as_unavailable(monkeypatch):
    def get_option_list(*_args, **_kwargs):
        raise TypeError("unsupported operand type(s) for +: 'NoneType' and 'str'")

    monkeypatch.setattr(option, "xtdata", SimpleNamespace(get_option_list=get_option_list))

    payload = option.get_option_list(undl_code="510050.SH", dedate="202606")

    assert payload["status"] == "unavailable"
    assert payload["reason"].startswith("qmt_option_sector_data_unavailable:")


def test_singleton_does_not_stop_current_wrapper_parent(monkeypatch):
    stopped_commands = []

    def fake_processes(script):
        if "ParentProcessId" in script:
            return [
                {"ProcessId": 11, "ParentProcessId": 10},
                {"ProcessId": 10, "ParentProcessId": 9},
                {"ProcessId": 9, "ParentProcessId": 8},
                {"ProcessId": 20, "ParentProcessId": 8},
            ]
        return [
            {"ProcessId": 9, "Name": "qmt-server.exe", "CommandLine": "qmt-server --port 13543"},
            {"ProcessId": 10, "Name": "qmt-server.exe", "CommandLine": "qmt-server --port 13543"},
            {"ProcessId": 20, "Name": "qmt-server.exe", "CommandLine": "qmt-server --port 18081"},
        ]

    def fake_run(cmd, **_kwargs):
        stopped_commands.append(cmd)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(singleton.os, "getpid", lambda: 11)
    monkeypatch.setattr(singleton.os, "getppid", lambda: 10)
    monkeypatch.setattr(singleton, "_powershell_json", fake_processes)
    monkeypatch.setattr(singleton.subprocess, "run", fake_run)

    stopped = singleton.stop_existing_qmt_servers(port=13543, enabled=True)

    assert [item["pid"] for item in stopped] == [20]
    assert len(stopped_commands) == 1
    assert stopped_commands[0][-1] == (
        "Stop-Process -Id 20 -Force; "
        "Wait-Process -Id 20 -Timeout 10 -ErrorAction SilentlyContinue"
    )


def test_trader_manager_uses_current_credit_query_names():
    calls = []

    class Trader:
        def query_stk_compacts(self, account):
            calls.append(("query_stk_compacts", account))
            return ["debt"]

        def query_credit_slo_code(self, account):
            calls.append(("query_credit_slo_code", account))
            return ["slo"]

    manager = XtTraderManager(account_id="acct-1")
    manager._trader = Trader()
    manager._account = "account-object"

    assert manager.query_credit_debt() == ["debt"]
    assert manager.query_slo_stocks() == ["slo"]
    assert calls == [
        ("query_stk_compacts", "account-object"),
        ("query_credit_slo_code", "account-object"),
    ]


def test_trader_manager_reports_missing_credit_available_as_unsupported():
    manager = XtTraderManager(account_id="acct-1")
    manager._trader = SimpleNamespace()
    manager._account = "account-object"

    payload = manager.query_credit_available(stock_code="000001.SZ")

    assert payload["status"] == "unsupported"
    assert payload["function"] == "query_credit_available"


def test_trader_manager_query_data_requires_result_path():
    manager = XtTraderManager(account_id="acct-1")
    manager._trader = SimpleNamespace()
    manager._account = "account-object"

    payload = manager.query_data(data_type="orders")

    assert payload["status"] == "unsupported"
    assert payload["reason"] == "xttrader_query_data_requires_result_path"
