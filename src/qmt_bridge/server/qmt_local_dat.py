from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path
import re
import struct

import pandas as pd


_HEADER_BYTES = 8
_RECORD_BYTES = 64
_MARKET_CODE = re.compile(r"^(?P<code>\d{6})\.(?P<market>SH|SZ|BJ)$")
_COLUMNS = ("open", "high", "low", "close", "volume", "amount")


def _expected_times() -> tuple[int, ...]:
    values: list[int] = []
    for hour, start, end in ((9, 30, 59), (10, 0, 59), (11, 0, 30)):
        values.extend(hour * 10000 + minute * 100 for minute in range(start, end + 1))
    for hour, start, end in ((13, 1, 59), (14, 0, 59)):
        values.extend(hour * 10000 + minute * 100 for minute in range(start, end + 1))
    values.append(150000)
    return tuple(values)


_EXPECTED_TIMES = _expected_times()


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_COLUMNS)


def _request_bound(value: str, *, end: bool) -> datetime | None:
    digits = "".join(character for character in str(value or "") if character.isdigit())
    if not digits:
        return None
    if len(digits) >= 14:
        return datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
    if len(digits) >= 8:
        resolved = datetime.combine(
            date.fromisoformat(f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"),
            time.min,
        )
        return resolved + timedelta(days=1) if end else resolved
    raise ValueError("QMT local DAT time bounds must use YYYYMMDD or YYYYMMDDHHMMSS")


def _record_timestamp(handle, index: int) -> int:
    handle.seek(_HEADER_BYTES + index * _RECORD_BYTES)
    raw = handle.read(4)
    if len(raw) != 4:
        raise OSError("QMT local DAT record timestamp is truncated")
    return struct.unpack("<I", raw)[0]


def _lower_bound(handle, record_count: int, timestamp: int) -> int:
    low = 0
    high = record_count
    while low < high:
        middle = (low + high) // 2
        if _record_timestamp(handle, middle) < timestamp:
            low = middle + 1
        else:
            high = middle
    return low


def _read_stock(
    path: Path,
    *,
    start: datetime | None,
    end_exclusive: datetime | None,
    count: int,
) -> pd.DataFrame:
    if not path.is_file():
        return _empty_frame()
    file_size = path.stat().st_size
    record_count = max(0, (file_size - _HEADER_BYTES) // _RECORD_BYTES)
    if record_count == 0:
        return _empty_frame()
    start_timestamp = int(start.timestamp()) if start is not None else 0
    end_timestamp = int(end_exclusive.timestamp()) if end_exclusive is not None else 0xFFFFFFFF
    with path.open("rb") as handle:
        first = _lower_bound(handle, record_count, start_timestamp)
        last = _lower_bound(handle, record_count, end_timestamp)
        handle.seek(_HEADER_BYTES + first * _RECORD_BYTES)
        payload = handle.read(max(0, last - first) * _RECORD_BYTES)
    rows_by_date: dict[date, list[tuple[datetime, float, float, float, float, float, float]]] = defaultdict(list)
    for offset in range(0, len(payload), _RECORD_BYTES):
        raw = payload[offset : offset + _RECORD_BYTES]
        if len(raw) != _RECORD_BYTES:
            break
        values = struct.unpack("<16I", raw)
        timestamp = datetime.fromtimestamp(values[0])
        open_price, high_price, low_price, close_price = (
            float(value) / 1000.0 for value in values[1:5]
        )
        if (
            min(open_price, high_price, low_price, close_price) <= 0
            or high_price < max(open_price, close_price)
            or low_price > min(open_price, close_price)
        ):
            continue
        rows_by_date[timestamp.date()].append(
            (
                timestamp,
                open_price,
                high_price,
                low_price,
                close_price,
                float(values[6] * 100),
                float(struct.unpack_from("<Q", raw, 32)[0]),
            )
        )
    complete_rows: list[tuple[datetime, float, float, float, float, float, float]] = []
    for trade_date in sorted(rows_by_date):
        rows = sorted(rows_by_date[trade_date], key=lambda row: row[0])
        times = tuple(row[0].hour * 10000 + row[0].minute * 100 for row in rows)
        if times == _EXPECTED_TIMES:
            complete_rows.extend(rows)
    if count > 0:
        complete_rows = complete_rows[-count:]
    if not complete_rows:
        return _empty_frame()
    frame = pd.DataFrame(
        (
            {
                "time": row[0].strftime("%Y%m%d%H%M%S"),
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5],
                "amount": row[6],
            }
            for row in complete_rows
        )
    )
    return frame.set_index("time")


def _read_daily_stock(
    path: Path,
    *,
    start: datetime | None,
    end_exclusive: datetime | None,
    count: int,
) -> pd.DataFrame:
    if not path.is_file():
        return _empty_frame()
    file_size = path.stat().st_size
    record_count = max(0, (file_size - _HEADER_BYTES) // _RECORD_BYTES)
    if record_count == 0:
        return _empty_frame()
    start_timestamp = int(start.timestamp()) if start is not None else 0
    end_timestamp = int(end_exclusive.timestamp()) if end_exclusive is not None else 0xFFFFFFFF
    with path.open("rb") as handle:
        first = _lower_bound(handle, record_count, start_timestamp)
        last = _lower_bound(handle, record_count, end_timestamp)
        handle.seek(_HEADER_BYTES + first * _RECORD_BYTES)
        payload = handle.read(max(0, last - first) * _RECORD_BYTES)
    rows: list[dict[str, float | str]] = []
    for offset in range(0, len(payload), _RECORD_BYTES):
        raw = payload[offset : offset + _RECORD_BYTES]
        if len(raw) != _RECORD_BYTES:
            break
        values = struct.unpack("<16I", raw)
        timestamp = datetime.fromtimestamp(values[0])
        open_price, high_price, low_price, close_price = (
            float(value) / 1000.0 for value in values[1:5]
        )
        if (
            min(open_price, high_price, low_price, close_price) <= 0
            or high_price < max(open_price, close_price)
            or low_price > min(open_price, close_price)
        ):
            continue
        rows.append(
            {
                "time": timestamp.strftime("%Y%m%d"),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": float(values[6]),
                "amount": float(struct.unpack_from("<Q", raw, 32)[0]),
            }
        )
    if count > 0:
        rows = rows[-count:]
    if not rows:
        return _empty_frame()
    return pd.DataFrame(rows).set_index("time")


def read_qmt_local_dat_1m(
    root: Path,
    stocks: tuple[str, ...],
    *,
    start_time: str,
    end_time: str,
    count: int,
) -> dict[str, pd.DataFrame]:
    start = _request_bound(start_time, end=False)
    end_exclusive = _request_bound(end_time, end=True)
    result: dict[str, pd.DataFrame] = {}
    for stock in stocks:
        matched = _MARKET_CODE.fullmatch(stock.upper())
        if matched is None:
            result[stock] = _empty_frame()
            continue
        path = (
            root
            / matched.group("market")
            / "60"
            / f"{matched.group('code')}.DAT"
        )
        result[stock] = _read_stock(
            path,
            start=start,
            end_exclusive=end_exclusive,
            count=count,
        )
    return result


def read_qmt_local_dat_1d(
    root: Path,
    stocks: tuple[str, ...],
    *,
    start_time: str,
    end_time: str,
    count: int,
) -> dict[str, pd.DataFrame]:
    start = _request_bound(start_time, end=False)
    end_exclusive = _request_bound(end_time, end=True)
    roots = (root, root.parent / "userdata_mini" / "datadir")
    result: dict[str, pd.DataFrame] = {}
    for stock in stocks:
        matched = _MARKET_CODE.fullmatch(stock.upper())
        if matched is None:
            result[stock] = _empty_frame()
            continue
        relative_path = (
            Path(matched.group("market"))
            / "86400"
            / f"{matched.group('code')}.DAT"
        )
        path = next(
            (candidate for candidate in (base / relative_path for base in roots) if candidate.is_file()),
            root / relative_path,
        )
        result[stock] = _read_daily_stock(
            path,
            start=start,
            end_exclusive=end_exclusive,
            count=count,
        )
    return result


__all__ = ["read_qmt_local_dat_1d", "read_qmt_local_dat_1m"]
