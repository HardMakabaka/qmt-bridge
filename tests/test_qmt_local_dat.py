from __future__ import annotations

from datetime import datetime
from pathlib import Path
import struct
import sys
from types import ModuleType, SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pandas as pd


xtquant_stub = ModuleType("xtquant")
xtquant_stub.xtdata = SimpleNamespace()
sys.modules.setdefault("xtquant", xtquant_stub)


def _expected_times() -> tuple[int, ...]:
    values: list[int] = []
    for hour, start, end in ((9, 30, 59), (10, 0, 59), (11, 0, 30)):
        values.extend(hour * 10000 + minute * 100 for minute in range(start, end + 1))
    for hour, start, end in ((13, 1, 59), (14, 0, 59)):
        values.extend(hour * 10000 + minute * 100 for minute in range(start, end + 1))
    values.append(150000)
    return tuple(values)


def _write_record(
    handle,
    timestamp: int,
    *,
    open_price: int = 10_190,
    high_price: int = 10_200,
    low_price: int = 10_170,
    close_price: int = 10_190,
    volume_lots: int = 10_784,
    amount: int = 10_981_004,
) -> None:
    values = [
        timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
        0,
        volume_lots,
        432,
        amount & 0xFFFFFFFF,
        amount >> 32,
        13,
        1_065_353_216,
        1_099_222_570,
        close_price,
        0,
        32_765,
    ]
    handle.write(struct.pack("<16I", *values))


def _write_day(root: Path, stock: str, times: tuple[int, ...]) -> None:
    code, market = stock.split(".")
    target = root / market / "60" / f"{code}.DAT"
    target.parent.mkdir(parents=True)
    with target.open("wb") as handle:
        handle.write(b"\xfe\xff\xff\xff\xff\xff\xff\x7f")
        for time_int in times:
            hour = time_int // 10000
            minute = time_int // 100 % 100
            timestamp = int(datetime(2026, 2, 10, hour, minute).timestamp())
            _write_record(handle, timestamp)


def _write_daily_history(root: Path, stock: str) -> None:
    code, market = stock.split(".")
    target = root.parent / "userdata_mini" / "datadir" / market / "86400" / f"{code}.DAT"
    target.parent.mkdir(parents=True)
    with target.open("wb") as handle:
        handle.write(b"\xfe\xff\xff\xff\xff\xff\xff\x7f")
        _write_record(
            handle,
            int(datetime(2019, 1, 18).timestamp()),
            volume_lots=6_720_953,
        )
        _write_record(
            handle,
            int(datetime(2026, 7, 31).timestamp()),
            volume_lots=9_250_903,
        )


def test_read_complete_qmt_local_dat_day(tmp_path: Path) -> None:
    from qmt_bridge.server.qmt_local_dat import read_qmt_local_dat_1m

    _write_day(tmp_path, "600000.SH", _expected_times())

    frames = read_qmt_local_dat_1m(
        tmp_path,
        ("600000.SH",),
        start_time="20260210",
        end_time="20260210",
        count=-1,
    )

    frame = frames["600000.SH"]
    assert len(frame) == 241
    assert frame.index[0] == "20260210093000"
    assert frame.index[-1] == "20260210150000"
    assert frame.iloc[0].to_dict() == {
        "open": 10.19,
        "high": 10.2,
        "low": 10.17,
        "close": 10.19,
        "volume": 1_078_400.0,
        "amount": 10_981_004.0,
    }


def test_incomplete_qmt_local_dat_day_is_not_returned(tmp_path: Path) -> None:
    from qmt_bridge.server.qmt_local_dat import read_qmt_local_dat_1m

    _write_day(tmp_path, "600000.SH", _expected_times()[:-1])

    frames = read_qmt_local_dat_1m(
        tmp_path,
        ("600000.SH",),
        start_time="20260210",
        end_time="20260210",
        count=-1,
    )

    assert frames["600000.SH"].empty


def test_read_qmt_local_daily_dat_from_userdata_mini(tmp_path: Path) -> None:
    from qmt_bridge.server.qmt_local_dat import read_qmt_local_dat_1d

    root = tmp_path / "datadir"
    root.mkdir()
    _write_daily_history(root, "512890.SH")

    frames = read_qmt_local_dat_1d(
        root,
        ("512890.SH",),
        start_time="20190118",
        end_time="20260731",
        count=-1,
    )

    frame = frames["512890.SH"]
    assert len(frame) == 2
    assert frame.index.tolist() == ["20190118", "20260731"]
    assert frame.iloc[0]["volume"] == 6_720_953.0


def test_history_ex_prefers_configured_local_dat(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from qmt_bridge.server.routers import market

    _write_day(tmp_path, "600000.SH", _expected_times())
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))

    def unexpected_xtdata_call(*_args, **_kwargs):
        raise AssertionError("configured local DAT reads must not call xtdata")

    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(get_market_data_ex=unexpected_xtdata_call),
    )

    app = FastAPI()
    app.include_router(market.router)
    with TestClient(app) as client:
        response = client.get(
            "/api/market/history_ex?stocks=600000.SH&period=1m"
            "&start_time=20260210&end_time=20260210&dividend_type=none"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "qmt_local_dat.1m"
    assert len(payload["data"]["600000.SH"]) == 241
    assert payload["data"]["600000.SH"][0]["time"] == "20260210093000"
    assert payload["data"]["600000.SH"][0]["volume"] == 1_078_400.0


def test_history_ex_falls_back_to_bigqmt_when_local_dat_has_no_requested_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from qmt_bridge.server.routers import market

    # Given the local QMT DAT root has no current-day minute rows.
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    calls: list[list[str]] = []

    def get_market_data_ex(*_args, **kwargs):
        calls.append(list(kwargs["stock_list"]))
        frame = pd.DataFrame(
            [
                {
                    "time": 1786411800000,
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.05,
                }
            ],
        )
        return {"000001.SZ": frame}

    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(get_market_data_ex=get_market_data_ex),
    )

    app = FastAPI()
    app.include_router(market.router)
    with TestClient(app) as client:
        # When the cutoff path requests that missing minute window.
        response = client.get(
            "/api/market/history_ex?stocks=000001.SZ&period=1m"
            "&start_time=20260811093000&end_time=20260811113000"
            "&dividend_type=none"
        )

    # Then the bridge falls through to Big-QMT instead of returning an empty cache hit.
    assert response.status_code == 200
    payload = response.json()
    assert calls == [["000001.SZ"]]
    assert payload["source"] == "qmt_rpc_fallback.1m"
    assert len(payload["data"]["000001.SZ"]) == 1
    assert payload["data"]["000001.SZ"][0]["time"] == 1786411800000
    assert "index" not in payload["data"]["000001.SZ"][0]


def test_history_ex_prefers_configured_local_daily_dat(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from qmt_bridge.server.routers import market

    root = tmp_path / "datadir"
    root.mkdir()
    _write_daily_history(root, "512890.SH")
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(root))

    def unexpected_xtdata_call(*_args, **_kwargs):
        raise AssertionError("configured local daily DAT reads must not call xtdata")

    monkeypatch.setattr(
        market,
        "xtdata",
        SimpleNamespace(get_market_data_ex=unexpected_xtdata_call),
    )

    app = FastAPI()
    app.include_router(market.router)
    with TestClient(app) as client:
        response = client.get(
            "/api/market/history_ex?stocks=512890.SH&period=1d"
            "&start_time=20190118&end_time=20260731&dividend_type=none"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "qmt_local_dat.1d"
    assert len(payload["data"]["512890.SH"]) == 2
    assert payload["data"]["512890.SH"][0]["time"] == "20190118"
