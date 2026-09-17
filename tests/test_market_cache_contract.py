import pickle
import sys
from threading import Event, Thread
from importlib import import_module
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


binary_cache = import_module("qmt_bridge.server.binary_cache")
helpers = import_module("qmt_bridge.server.helpers")
market = import_module("qmt_bridge.server.routers.market")
BinaryCache = binary_cache.BinaryCache
reset_binary_cache = binary_cache.reset_binary_cache
XtdataTransportStuckError = helpers.XtdataTransportStuckError
LocalMarketCache = import_module("bigqmt_signal_trader.local_cache").LocalMarketCache


def _expected_status(status, reason, function, **extra):
    payload = {
        "status": status,
        "data": None,
        "reason": reason,
        "reason_code": reason,
        "message": reason,
        "capability": function,
        "provider": "bigqmt",
        "retryable": status == "unavailable",
        "details": {},
        "function": function,
    }
    payload.update(extra)
    return payload


@pytest.fixture
def market_client():
    app = FastAPI()
    app.include_router(market.router)
    with TestClient(app) as client:
        yield client


@pytest.mark.parametrize(
    ("function_name", "path", "shape", "value_path"),
    [
        (
            "get_local_data",
            "/api/market/local_data?stocks=000001.SZ",
            "frame_dict",
            ("data", "000001.SZ", 0, "close"),
        ),
        (
            "get_market_data",
            "/api/market/market_data?stocks=000001.SZ&fields=close",
            "field_dict",
            ("data", "000001.SZ", 0, "close"),
        ),
        (
            "get_market_data3",
            "/api/market/market_data3?stocks=000001.SZ&fields=close",
            "frame_dict",
            ("data", "000001.SZ", 0, "close"),
        ),
        (
            "get_divid_factors",
            "/api/market/divid_factors?stock=000001.SZ",
            "dataframe",
            ("data", 0, "factor"),
        ),
    ],
)
def test_mutable_market_reads_refresh_by_default(
    monkeypatch,
    tmp_path,
    market_client,
    function_name,
    path,
    shape,
    value_path,
):
    # Given a mutable provider result and an enabled 24-hour binary cache.
    calls = 0

    def provider(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if shape == "field_dict":
            return {
                "close": pd.DataFrame(
                    [[float(calls)]],
                    index=["000001.SZ"],
                    columns=["20260721"],
                )
            }
        frame = pd.DataFrame(
            [{"time": "20260721", "factor" if shape == "dataframe" else "close": float(calls)}]
        )
        if shape == "frame_dict":
            return {"000001.SZ": frame}
        return frame

    reset_binary_cache(
        BinaryCache(enabled=True, cache_dir=tmp_path, ttl_seconds=86400, max_bytes=10_000_000)
    )
    monkeypatch.setattr(market, "market_data", SimpleNamespace(**{function_name: provider}))

    try:
        # When the same open-ended HTTP read is repeated after the provider changes.
        first = market_client.get(path)
        second = market_client.get(path)
    finally:
        reset_binary_cache(None)

    # Then the route reads the provider again instead of returning a stale default-cache hit.
    assert first.status_code == 200
    assert second.status_code == 200
    assert calls == 2
    value = second.json()
    for key in value_path:
        value = value[key]
    assert value == 2.0


@pytest.mark.parametrize(
    ("function_name", "path"),
    [
        ("get_full_tick", "/api/market/snapshot?stocks=000001.SZ"),
        ("get_full_tick", "/api/market/indices"),
        ("get_market_data_ex", "/api/market/history_ex?stocks=000001.SZ"),
        ("get_local_data", "/api/market/local_data?stocks=000001.SZ&cache=false"),
        ("get_divid_factors", "/api/market/divid_factors?stock=000001.SZ&cache=false"),
        ("get_market_data", "/api/market/market_data?stocks=000001.SZ&cache=false"),
        ("get_market_data3", "/api/market/market_data3?stocks=000001.SZ&cache=false"),
        ("get_full_kline", "/api/market/full_kline?stock=000001.SZ"),
        ("get_all_subscription", "/api/market/subscriptions"),
    ],
)
def test_market_read_provider_errors_are_structured(
    monkeypatch,
    market_client,
    function_name,
    path,
):
    # Given any market read whose QMT provider raises.
    def provider(*_args, **_kwargs):
        raise RuntimeError("provider offline")

    monkeypatch.setattr(market, "market_data", SimpleNamespace(**{function_name: provider}))

    # When the route is invoked through HTTP.
    response = market_client.get(path)

    # Then it returns the stable structured error contract rather than HTTP 500.
    assert response.status_code == 200
    assert response.json() == _expected_status(
        "unavailable",
        "provider offline",
        function_name,
        error_type="RuntimeError",
    )


@pytest.mark.parametrize(
    ("function_name", "path"),
    [
        ("get_full_tick", "/api/market/snapshot?stocks=000001.SZ"),
        ("get_all_subscription", "/api/market/subscriptions"),
    ],
)
def test_market_transport_stuck_is_structured_unavailable(
    monkeypatch,
    market_client,
    function_name,
    path,
):
    # Given the shared native transport has been marked stuck until bridge restart.
    def provider(*_args, **_kwargs):
        raise XtdataTransportStuckError("blocked by timed-out native call")

    monkeypatch.setattr(market, "market_data", SimpleNamespace(**{function_name: provider}))

    # When any market read enters the shared response boundary.
    response = market_client.get(path)

    # Then transport poisoning is unavailable, not an ordinary provider error.
    assert response.status_code == 200
    assert response.json() == _expected_status(
        "unavailable",
        "xtdata_transport_stuck",
        function_name,
        error_type="XtdataTransportStuckError",
        detail="blocked by timed-out native call",
    )


@pytest.mark.parametrize(
    ("function_name", "path"),
    [
        ("get_full_tick", "/api/market/snapshot?stocks=000001.SZ"),
        ("get_full_tick", "/api/market/indices"),
        ("get_market_data_ex", "/api/market/history_ex?stocks=000001.SZ"),
        ("get_local_data", "/api/market/local_data?stocks=000001.SZ&cache=false"),
        ("get_divid_factors", "/api/market/divid_factors?stock=000001.SZ&cache=false"),
        ("get_market_data", "/api/market/market_data?stocks=000001.SZ&cache=false"),
        ("get_market_data3", "/api/market/market_data3?stocks=000001.SZ&cache=false"),
        ("get_full_kline", "/api/market/full_kline?stock=000001.SZ"),
    ],
)
def test_market_read_none_payloads_are_unavailable(
    monkeypatch,
    market_client,
    function_name,
    path,
):
    # Given a market provider that returns no payload.
    monkeypatch.setattr(
        market,
        "market_data",
        SimpleNamespace(**{function_name: lambda *_args, **_kwargs: None}),
    )

    # When the route is invoked through HTTP.
    response = market_client.get(path)

    # Then absence is explicit and cannot be mistaken for empty market data.
    assert response.status_code == 200
    assert response.json() == _expected_status(
        "unavailable",
        f"xtdata_{function_name}_returned_none",
        function_name,
    )


@pytest.mark.parametrize(
    ("function_name", "path"),
    [
        ("get_full_tick", "/api/market/snapshot?stocks=000001.SZ"),
        ("get_full_tick", "/api/market/indices"),
        ("get_market_data_ex", "/api/market/history_ex?stocks=000001.SZ"),
        ("get_local_data", "/api/market/local_data?stocks=000001.SZ&cache=false"),
        ("get_divid_factors", "/api/market/divid_factors?stock=000001.SZ&cache=false"),
        ("get_market_data", "/api/market/market_data?stocks=000001.SZ&cache=false"),
        ("get_market_data3", "/api/market/market_data3?stocks=000001.SZ&cache=false"),
    ],
)
def test_mapping_market_reads_reject_wrong_shapes(
    monkeypatch,
    market_client,
    function_name,
    path,
):
    # Given a QMT function returning the wrong container type.
    monkeypatch.setattr(
        market,
        "market_data",
        SimpleNamespace(**{function_name: lambda *_args, **_kwargs: []}),
    )

    # When the route is invoked through HTTP.
    response = market_client.get(path)

    # Then conversion never raises and the malformed shape is explicit.
    assert response.status_code == 200
    assert response.json() == _expected_status(
        "error",
        f"xtdata_{function_name}_invalid_response",
        function_name,
        error_type="list",
    )


@pytest.mark.parametrize(
    "bad_envelope",
    [
        ["not", "an", "envelope"],
        {"metadata": {"created_at": "not-a-timestamp"}, "data": {"value": 1}},
    ],
)
def test_binary_cache_recovers_from_wrong_shaped_pickle(tmp_path, bad_envelope):
    # Given a syntactically valid pickle whose envelope has the wrong shape.
    cache = BinaryCache(
        enabled=True,
        cache_dir=tmp_path,
        ttl_seconds=3600,
        max_bytes=10_000_000,
    )
    namespace = "malformed"
    params = {"stock": "000001.SZ"}
    path = cache.path_for(namespace, params)
    path.parent.mkdir(parents=True)
    with path.open("wb") as handle:
        pickle.dump(bad_envelope, handle)
    calls = 0

    def loader():
        nonlocal calls
        calls += 1
        return {"value": 2}

    # When the same cached call is made twice.
    first = cache.cached_call(namespace, params, loader)
    second = cache.cached_call(namespace, params, loader)

    # Then corruption becomes one recoverable miss and the replacement is reusable.
    assert calls == 1
    assert first[0] == {"value": 2}
    assert first[1]["hit"] is False
    assert second[0] == {"value": 2}
    assert second[1]["hit"] is True
    with path.open("rb") as handle:
        assert isinstance(pickle.load(handle), dict)


def test_binary_cache_singleflight_wait_is_bounded(tmp_path):
    cache = BinaryCache(enabled=True, cache_dir=tmp_path, ttl_seconds=3600,
                        max_bytes=10_000_000, singleflight_wait_seconds=0.01)
    entered, release = Event(), Event()
    result = []

    def loader():
        entered.set()
        assert release.wait(1)
        return {"value": 1}

    owner = Thread(target=lambda: result.append(cache.cached_call("bounded", {}, loader)))
    owner.start()
    assert entered.wait(1)
    with pytest.raises(TimeoutError, match="singleflight_wait_timeout"):
        cache.cached_call("bounded", {}, lambda: pytest.fail("waiter must not load"))
    release.set()
    owner.join(timeout=1)
    assert result[0][0] == {"value": 1}


def test_local_market_cache_filters_epoch_milliseconds_by_shanghai_date(tmp_path):
    cache = LocalMarketCache(cache_dir=tmp_path, fmt="pkl")
    frame = pd.DataFrame(
        {
            "time": [
                1786896000000,
                1786982400000,
                1787068800000,
                1787155200000,
                1787241600000,
            ],
            "close": [9.0, 10.0, 11.0, 12.0, 13.0],
        }
    )
    cache.write("000001.SZ", "1d", frame)

    result = cache.read(
        "000001.SZ",
        "1d",
        start_time="20260818",
        end_time="20260820",
    )

    assert result["close"].tolist() == [10.0, 11.0, 12.0]
