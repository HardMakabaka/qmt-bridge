"""Closed-minute reads; deliberately separate from the strict full-day reader."""
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from bigqmt_signal_trader.telemetry import emit, span

from .qmt_local_dat import read_qmt_local_dat_1m


def read_closed_minute_tail(root: Path, stocks: tuple[str, ...], *, start_time: str,
                            end_time: str, count: int, now: datetime | None = None):
    start = datetime.strptime(start_time, "%Y%m%d%H%M%S")
    end = datetime.strptime(end_time, "%Y%m%d%H%M%S")
    exchange_now = now or datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    if (not 1 <= len(stocks) <= 500 or not 1 <= count <= 60
            or start.date() != end.date() or start > end
            or end.second != 0 or end > exchange_now - timedelta(seconds=5)):
        emit("market.minute_tail.dat_lane", outcome="rejected", stock_count=len(stocks),
             requested_count=count)
        raise ValueError("invalid_or_unclosed_minute_window")
    with span("market.minute_tail.dat_lane", stock_count=len(stocks),
              requested_count=count) as result:
        value = read_qmt_local_dat_1m(root, stocks, start_time=start_time,
                                      end_time=end_time, count=count,
                                      complete_days_only=False)
        rows = sum(int(getattr(frame, "shape", (0,))[0]) for frame in value.values())
        result["returned_rows"] = rows
        result["outcome"] = "success" if rows else "empty"
        return value
