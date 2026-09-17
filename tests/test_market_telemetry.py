"""Telemetry evidence for bounded BigQMT market-path summaries."""

from types import SimpleNamespace

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.data_client import BigQmtDataClient
from qmt_bridge.server.binary_cache import BinaryCache


def _names(events):
    return [event["event_name"] for event in events]


def test_binary_cache_emits_hit_and_noncacheable_store_skip(trace_events, tmp_path):
    cache = BinaryCache(enabled=True, cache_dir=tmp_path, ttl_seconds=60,
                        max_bytes=1_000_000)
    cache.cached_call("market", {"period": "1d"}, lambda: {"x": 1})
    cache.cached_call("market", {"period": "1d"}, lambda: {"x": 2})
    cache.cached_call("skip", {"period": "1m"}, lambda: {"x": 1},
                      should_store=lambda _value: False)

    names = _names(trace_events())
    assert "market.binary_cache.stored" in names
    assert "market.binary_cache.hit" in names
    assert "market.binary_cache.store_skipped" in names


def test_context_local_data_emits_scalar_source_and_summary(trace_events):
    class Context:
        def get_local_data(self, stock, start, end, period, divid, count):
            return {"20260910150000": {"close": 10}}

    value = BigQmtMarketDataProvider(Context()).get_local_data(
        stock_list=["000001.SZ"], period="1d", start_time="20260910",
        end_time="20260910", count=1,
    )

    assert value["000001.SZ"].iloc[0]["close"] == 10
    events = trace_events()
    end = next(event for event in events if event["event_name"] == "market.context_local_data.end")
    assert end["outcome"] == "success"
    assert end["returned_rows"] == 1


def test_history_rpc_path_emits_source_and_result_summary(trace_events, monkeypatch):
    from qmt_bridge.server.routers import market

    def get_market_data_ex(**_kwargs):
        return {"000001.SZ": pd.DataFrame({"time": ["20260910150000"], "close": [10.0]})}

    monkeypatch.setattr(market, "market_data", SimpleNamespace(get_market_data_ex=get_market_data_ex))
    monkeypatch.setattr(market, "_call_xtdata_serialized", lambda fn, **kwargs: fn(**kwargs))
    app = FastAPI()
    app.include_router(market.router)
    with TestClient(app) as client:
        response = client.get("/api/market/history_ex?stocks=000001.SZ&period=1m")

    assert response.status_code == 200
    events = trace_events()
    names = _names(events)
    assert "market.history.path_selected" in names
    result = next(event for event in events if event["event_name"] == "market.history.result")
    assert result["source"] == "qmt_rpc.1m"
    assert result["returned_rows"] == 1


def test_minute_tail_early_returns_emit_terminal_outcomes(trace_events, monkeypatch, tmp_path):
    from qmt_bridge.server.routers import market

    monkeypatch.delenv("QMT_BRIDGE_LOCAL_DAT_ROOT", raising=False)
    root_missing = market.get_minute_tail(
        stocks="000001.SZ", start_time="20260910100000", end_time="20260910100100",
        count=1, refresh_missing=False,
    )
    assert root_missing["status"] == "unavailable"

    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    monkeypatch.setattr(market, "read_closed_minute_tail", lambda *_a, **_kw: (_ for _ in ()).throw(OSError("fixture")))
    read_error = market.get_minute_tail(
        stocks="000001.SZ", start_time="20260910100000", end_time="20260910100100",
        count=1, refresh_missing=False,
    )
    assert read_error["status"] == "error"

    events = [event for event in trace_events() if event["event_name"] == "market.minute_tail.result"]
    assert [(event["outcome"], event["reason_code"]) for event in events] == [
        ("unavailable", "qmt_local_dat_root_unavailable"),
        ("error", "minute_tail_local_dat_read_failed"),
    ]


def test_history_provider_error_emits_terminal_summary(trace_events, monkeypatch):
    from qmt_bridge.server.routers import market

    monkeypatch.setattr(
        market, "market_data",
        SimpleNamespace(get_market_data_ex=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture"))),
    )
    monkeypatch.setattr(market, "_call_xtdata_serialized", lambda fn, **kwargs: fn(**kwargs))
    app = FastAPI()
    app.include_router(market.router)
    with TestClient(app) as client:
        payload = client.get("/api/market/history_ex?stocks=000001.SZ&period=1m").json()

    assert payload["status"] == "error"
    terminal = next(event for event in trace_events() if event["event_name"] == "market.history.result")
    assert terminal["outcome"] == "error"
    assert terminal["source"] == "unavailable"


def test_data_client_padding_filter_emits_final_empty_summary(trace_events):
    class Client:
        local_cache_config = {"enabled": False}

        def call(self, *_args, **_kwargs):
            return {
                "000001.SZ": pd.DataFrame({
                    "close": [0], "suspendFlag": [1], "volume": [0], "amount": [0],
                })
            }

    value = BigQmtDataClient(Client()).get_market_data_ex(
        stock_list=["000001.SZ"], fill_data=False,
    )
    assert value["000001.SZ"].empty
    result = next(event for event in trace_events() if event["event_name"] == "market.data_client.result")
    assert (result["outcome"], result["returned_rows"], result["filtered_rows"]) == ("empty", 0, 1)
