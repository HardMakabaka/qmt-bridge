"""Bounded, RPC-only 1-minute history readiness probe."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
import math
from zoneinfo import ZoneInfo
import re
from threading import Lock
from typing import Any
from bigqmt_signal_trader.transports.base import TransportError, TransportTimeout


_FORMAT = "%Y%m%d%H%M%S"
def _is_exchange_minute(value: datetime) -> bool:
    """Whether a wall-clock minute is expected in the A-share session.

    This probe deliberately has no trading-calendar dependency.  Weekends and
    the lunch break are non-probe windows, rather than fabricated missing bars.
    Exchange holidays are handled the same safe way when the caller chooses a
    non-trading date: no expected minute means no data-gap assertion.
    """
    if value.weekday() >= 5:
        return False
    clock = value.time()
    return (time(9, 30) <= clock <= time(11, 30)) or (time(13, 1) <= clock <= time(15, 0))


def validate_probe(stock: str, start_time: str, end_time: str, timeout_seconds: float) -> tuple[str, str, str, float, list[str]]:
    normalized_stock = stock.strip().upper()
    if not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", normalized_stock):
        raise ValueError("invalid_stock")
    if not re.fullmatch(r"\d{14}", start_time) or not re.fullmatch(r"\d{14}", end_time) or start_time[-2:] != "00" or end_time[-2:] != "00":
        raise ValueError("invalid_time")
    try:
        start = datetime.strptime(start_time, _FORMAT)
        end = datetime.strptime(end_time, _FORMAT)
    except ValueError as exc:
        raise ValueError("invalid_time") from exc
    if start.date() != end.date() or end < start or (end - start).total_seconds() > 120:
        raise ValueError("probe_window_must_be_same_day_at_most_3_minutes")
    if not 0 < timeout_seconds <= 8:
        raise ValueError("invalid_timeout_seconds")
    expected_times = [
        (start + timedelta(minutes=index)).strftime(_FORMAT)
        for index in range(int((end - start).total_seconds() // 60) + 1)
        if _is_exchange_minute(start + timedelta(minutes=index))
    ]
    return normalized_stock, start_time, end_time, timeout_seconds, expected_times


def _rows(value: Any, stock: str) -> list[dict[str, Any]]:
    value = value.get(stock) if isinstance(value, dict) else value
    if value is None:
        return []
    if hasattr(value, "to_dict"):
        try:
            records = value.reset_index().to_dict("records")
        except Exception:
            records = value.to_dict("records")
    elif isinstance(value, list):
        records = value
    else:
        raise ValueError("invalid_rpc_payload")
    return [row for row in records if isinstance(row, dict)]


def _minute_key(row: dict[str, Any]) -> str | None:
    value = row.get("stime") if row.get("stime") is not None else (row.get("time") if row.get("time") is not None else row.get("datetime") or row.get("index"))
    if isinstance(value, str) and value.isdigit() and len(value) == 13:
        value = int(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 1_000_000_000_000:
        return datetime.fromtimestamp(value / 1000, tz=ZoneInfo("Asia/Shanghai")).strftime(_FORMAT)
    if hasattr(value, "strftime"):
        return value.strftime(_FORMAT)
    text = str(value or "").replace("-", "").replace(":", "").replace(" ", "")
    return text[:14] if len(text) >= 14 and text[:14].isdigit() else None


def _positive_finite(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError):
        return False


def _zero_volume(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return float(value) == 0
    except (TypeError, ValueError):
        return False


def probe(runtime: Any, *, stock: str, start_time: str, end_time: str, timeout_seconds: float) -> tuple[str, str, int, list[str], list[str], str | None, list[str], str]:
    """Return rpc/data statuses without FormulaServer or local-cache fallback."""
    _stock, _start, _end, timeout, expected = validate_probe(stock, start_time, end_time, timeout_seconds)
    if not expected:
        return "ok", "complete", 0, expected, [], None, ["non_trading_window"], "none"
    client = getattr(runtime, "client", None)
    if client is None:
        return "unavailable", "empty", 0, expected, [], "runtime_unavailable", [], "configuration"
    try:
        raw = client.call(
            "get_market_data_ex",
            params={"field_list": ["time", "open", "high", "low", "close", "volume"], "stock_list": [_stock], "period": "1m",
                    "start_time": _start, "end_time": _end, "count": len(expected),
                    "dividend_type": "none", "fill_data": False, "subscribe": False},
            timeout_seconds=timeout,
            force_rpc=True,
        )
        rows = _rows(raw, _stock)
    except (TimeoutError, TransportTimeout):
        return "timeout", "empty", 0, expected, [], "rpc_timeout", [], "transport"
    except (ConnectionError, OSError, TransportError):
        return "connection_error", "empty", 0, expected, [], "rpc_connection_error", [], "transport"
    except NotImplementedError:
        return "unavailable", "invalid", 0, expected, [], "rpc_not_implemented", [], "configuration"
    except Exception as exc:
        return "unavailable", "invalid", 0, expected, [], f"rpc_error:{exc.__class__.__name__}", [], "configuration"
    actual = [_minute_key(row) for row in rows]
    if any(value is None for value in actual):
        return "ok", "invalid", len(rows), expected, [], "invalid_bar_timestamp", [], "data"
    actual_times = [value for value in actual if value is not None]
    missing = [value for value in expected if value not in actual_times]
    if not rows:
        return "ok", "empty", 0, expected, [], None, [], "data"
    valid_prices = all(all(_positive_finite(row.get(field)) for field in ("open", "high", "low", "close")) for row in rows)
    if len(set(actual_times)) != len(actual_times) or any(value not in expected for value in actual_times) or not valid_prices:
        return "ok", "invalid", len(rows), expected, actual_times, "invalid_bar_payload", [], "data"
    zero_volume = any(_zero_volume(row.get("volume")) for row in rows)
    return "ok", "complete" if not missing else "partial", len(rows), expected, actual_times, None, (["zero_volume_not_tradability"] if zero_volume else []), "none" if not missing else "data"


@dataclass(frozen=True, slots=True)
class ProbeFence:
    key: tuple[str, str, str]
    generation: int
    request_id: int


class ProbeState:
    def __init__(self) -> None:
        self.lock = Lock()
        self.key: tuple[str, str, str] | None = None
        self.generation = 0
        self.request_id = 0
        self.last_success_at: str | None = None
        self.consecutive_failures = 0

    def begin(self, key: tuple[str, str, str]) -> ProbeFence:
        with self.lock:
            if self.key != key:
                self.key, self.last_success_at, self.consecutive_failures = key, None, 0
                self.generation += 1
            self.request_id += 1
            return ProbeFence(key=key, generation=self.generation, request_id=self.request_id)

    def update(self, fence: ProbeFence, rpc_status: str, data_status: str, failure_kind: str) -> tuple[str | None, int, bool]:
        with self.lock:
            if self.key != fence.key or self.generation != fence.generation or self.request_id != fence.request_id:
                return self.last_success_at, self.consecutive_failures, False
            if rpc_status == "ok" or failure_kind == "configuration":
                self.consecutive_failures = 0
            else:
                self.consecutive_failures += 1
            if rpc_status == "ok" and data_status == "complete":
                self.last_success_at = datetime.now(timezone.utc).isoformat()
            return self.last_success_at, self.consecutive_failures, True
