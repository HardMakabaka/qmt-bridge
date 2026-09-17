from datetime import datetime
import struct
from types import SimpleNamespace

import pandas as pd
import pytest

from qmt_bridge.server.minute_incremental import read_closed_minute_tail
from qmt_bridge.server.qmt_local_dat import read_qmt_local_dat_1m


def _write_day(root, stock, times):
    code, market = stock.split(".")
    path = root / market / "60" / f"{code}.DAT"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as out:
        out.write(b"\xfe\xff\xff\xff\xff\xff\xff\x7f")
        for value in times:
            stamp = int(datetime(2026, 2, 10, value // 10000, value // 100 % 100).timestamp())
            out.write(struct.pack("<16I", stamp, 10100, 10200, 10000, 10100, 0, 100, 0,
                                  101000, 0, 0, 0, 0, 10100, 0, 0))


def test_incremental_reads_closed_partial_day_without_weakening_full_day(tmp_path):
    _write_day(tmp_path, "600000.SH", (93000, 93100, 93200, 93300))
    args = dict(start_time="20260210093000", end_time="20260210093200", count=3)
    assert read_qmt_local_dat_1m(tmp_path, ("600000.SH",), **args)["600000.SH"].empty
    frame = read_closed_minute_tail(tmp_path, ("600000.SH",), **args,
                                   now=datetime(2026, 2, 10, 9, 32, 5))["600000.SH"]
    assert len(frame) == 3
    assert str(frame.index[-1]).endswith("093200")


@pytest.mark.parametrize("end", ["20260210093300", "20260210093201", "20260211093200"])
def test_rejects_unclosed_or_cross_day_windows(tmp_path, end):
    with pytest.raises(ValueError):
        read_closed_minute_tail(tmp_path, ("600000.SH",), start_time="20260210093000",
                                end_time=end, count=3, now=datetime(2026, 2, 10, 9, 32, 5))


def test_complete_day_contract_remains_241(tmp_path):
    times = tuple(h * 10000 + m * 100 for h, start, end in
                  ((9, 30, 59), (10, 0, 59), (11, 0, 30), (13, 1, 59), (14, 0, 59))
                  for m in range(start, end + 1)) + (150000,)
    _write_day(tmp_path, "600000.SH", times)
    frame = read_qmt_local_dat_1m(tmp_path, ("600000.SH",), start_time="20260210",
                                 end_time="20260210", count=-1)["600000.SH"]
    assert len(frame) == 241


@pytest.mark.parametrize("time_shape", ["epoch_and_stime", "epoch_only", "text", "datetime_index"])
@pytest.mark.parametrize("stale", [False, True])
def test_minute_tail_accepts_rpc_timestamps_without_accepting_stale_data(monkeypatch, tmp_path, time_shape, stale):
    from qmt_bridge.server.routers import market

    stamps = ["20260909112000", "20260909112100", "20260909112200"]
    frame = pd.DataFrame({"open": [11.74] * 3, "high": [11.74] * 3,
                          "low": [11.72] * 3, "close": [11.73] * 3,
                          "volume": [100] * 3, "amount": [117300] * 3})
    epoch_ms = [1788924000000, 1788924060000, 1788924120000]
    if time_shape.startswith("epoch"):
        frame["time"] = epoch_ms
        if time_shape == "epoch_and_stime":
            frame["stime"] = stamps
    elif time_shape == "text":
        frame["time"] = stamps
    else:
        frame.index = pd.to_datetime(stamps, format="%Y%m%d%H%M%S")
        frame.index.name = "time"
    if stale:
        frame = frame.iloc[:-1]
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    monkeypatch.setattr(market, "read_closed_minute_tail", lambda *args, **kwargs: {"000001.SZ": pd.DataFrame()})
    monkeypatch.setattr(market, "market_data", SimpleNamespace(get_market_data_ex_scoped=lambda **kwargs: {"000001.SZ": frame}))
    monkeypatch.setattr(market, "_call_xtdata_serialized", lambda fn, **kwargs: fn(**kwargs))
    result = market.get_minute_tail(stocks="000001.SZ", start_time=stamps[0],
                                    end_time=stamps[-1], count=3, refresh_missing=True)
    assert result["status"] == ("partial" if stale else "ok")
    assert result["missing_stocks"] == (["000001.SZ"] if stale else [])
    assert len(result["data"]["000001.SZ"]) == (0 if stale else 3)
    if not stale:
        assert result["source_by_stock"]["000001.SZ"] == "qmt_rpc_fallback.1m"
        assert result["volume_unit_by_stock"]["000001.SZ"] == "lots"


def test_history_ex_keeps_per_stock_lineage_and_raw_volume_unit_for_mixed_dat_rpc(monkeypatch, tmp_path):
    from qmt_bridge.server.routers import market

    local = pd.DataFrame(
        {"open": [10.0], "high": [10.0], "low": [10.0], "close": [10.0],
         "volume": [1000.0], "amount": [10000.0]},
        index=["20260911093000"],
    )
    native = pd.DataFrame(
        {"stime": ["20260911093000"], "open": [9.0], "high": [9.0], "low": [9.0],
         "close": [9.0], "volume": [100], "amount": [90000.0]},
    )
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    monkeypatch.setattr(
        market,
        "read_qmt_local_dat_1m",
        lambda *_args, **_kwargs: {"000001.SZ": local, "600000.SH": pd.DataFrame()},
    )
    monkeypatch.setattr(
        market,
        "market_data",
        SimpleNamespace(get_market_data_ex=lambda **_kwargs: {"600000.SH": native}),
    )
    monkeypatch.setattr(market, "_call_xtdata_serialized", lambda fn, **kwargs: fn(**kwargs))

    result = market.get_history_ex(
        stocks="000001.SZ,600000.SH", period="1m",
        start_time="20260911093000", end_time="20260911093000", count=-1,
        dividend_type="none",
    )

    assert result["source"] == "qmt_local_dat.1m+qmt_rpc"
    assert result["source_by_stock"] == {
        "000001.SZ": "qmt_local_dat.1m", "600000.SH": "qmt_rpc_fallback.1m",
    }
    assert result["volume_unit_by_stock"] == {"000001.SZ": "shares", "600000.SH": "lots"}
    assert result["data"]["000001.SZ"][0]["volume"] == 1000.0
    assert result["data"]["600000.SH"][0]["volume"] == 100


def test_minute_tail_passes_only_remaining_budget_to_native_rpc(monkeypatch, tmp_path):
    from qmt_bridge.server.routers import market

    calls = []
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    monkeypatch.setattr(market, "read_closed_minute_tail", lambda *args, **kwargs: {"000001.SZ": pd.DataFrame()})

    def bounded(budget, callback):
        assert budget == 8.0  # Caller uses a 10-second HTTP timeout.
        return callback(3.25)

    def native(**kwargs):
        calls.append(kwargs)
        return {}

    monkeypatch.setattr(market, "_call_xtdata_serialized_with_budget", bounded)
    monkeypatch.setattr(market, "market_data", SimpleNamespace(get_market_data_ex_scoped=native))
    result = market.get_minute_tail(stocks="000001.SZ", start_time="20260909112000",
                                    end_time="20260909112200", count=3, refresh_missing=True)
    assert result["status"] == "partial"
    assert len(calls) == 1 and calls[0]["timeout_seconds"] == 3.25


def test_minute_tail_queue_expiry_is_explicit_without_invoking_native(monkeypatch, tmp_path):
    from qmt_bridge.server.routers import market

    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    monkeypatch.setattr(market, "read_closed_minute_tail", lambda *args, **kwargs: {"000001.SZ": pd.DataFrame()})

    def expired(budget, callback):
        raise TimeoutError("minute RPC budget expired before transport entry")

    def forbidden(**kwargs):
        raise AssertionError("Expired requests must not reach QMT")

    monkeypatch.setattr(market, "_call_xtdata_serialized_with_budget", expired)
    monkeypatch.setattr(market, "market_data", SimpleNamespace(get_market_data_ex_scoped=forbidden))
    result = market.get_minute_tail(stocks="000001.SZ", start_time="20260909112000",
                                    end_time="20260909112200", count=3, refresh_missing=True)
    assert result["status"] == "partial"
    assert result["rpc_error"]["status"] == "unavailable"
    assert result["data"]["000001.SZ"] == []


def test_minute_tail_does_not_wait_for_unrelated_market_read_lock(monkeypatch, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from qmt_bridge.server import helpers
    from qmt_bridge.server.routers import market

    frame = pd.DataFrame({"stime": ["20260914102900"], "open": [10.0],
                          "high": [10.0], "low": [10.0], "close": [10.0],
                          "volume": [100], "amount": [100000.0]})
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    monkeypatch.setattr(market, "read_closed_minute_tail", lambda *a, **kw: {"000001.SZ": pd.DataFrame()})
    monkeypatch.setattr(market, "market_data", SimpleNamespace(
        get_market_data_ex_scoped=lambda **kw: {"000001.SZ": frame}))
    with ThreadPoolExecutor(max_workers=1) as executor:
        helpers._XTDATA_TRANSPORT._call_lock.acquire()
        try:
            future = executor.submit(market.get_minute_tail, stocks="000001.SZ",
                                     start_time="20260914102900", end_time="20260914102900",
                                     count=1, refresh_missing=True)
            result = future.result(timeout=0.5)
            assert result["status"] == "ok"
        finally:
            helpers._XTDATA_TRANSPORT._call_lock.release()


def test_minute_lane_preserves_native_transport_unavailable_guard():
    from qmt_bridge.server import helpers

    helpers._mark_xtdata_transport_stuck("native download failed to stop")
    try:
        with pytest.raises(helpers.XtdataTransportStuckError):
            helpers._call_minute_xtdata_serialized_with_budget(
                8, lambda _: pytest.fail("unavailable native runtime must not be called"))
    finally:
        helpers._reset_xtdata_transport_for_tests()
