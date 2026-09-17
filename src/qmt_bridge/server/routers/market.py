"""Router — Market data endpoints /api/market/*."""

import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Body, Query
from bigqmt_signal_trader.telemetry import emit, span

from ..bigqmt import market_data

from ..binary_cache import get_binary_cache
from ..helpers import (
    XtdataTransportStuckError,
    _call_xtdata_serialized,
    _call_minute_xtdata_serialized_with_budget as _call_xtdata_serialized_with_budget,
    _dataframe_dict_to_records,
    _exception_status,
    _numpy_to_python,
    _status_payload,
)
from ..qmt_local_dat import read_qmt_local_dat_1d, read_qmt_local_dat_1m
from ..minute_incremental import read_closed_minute_tail

router = APIRouter(prefix="/api/market", tags=["market"])

# 主要指数列表（用于 /indices 端点）
MAJOR_INDICES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000300.SH",  # 沪深300
    "000016.SH",  # 上证50
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
]


def _split_stocks(stocks: str) -> list[str]:
    return [s.strip() for s in stocks.split(",") if s.strip()]


_A_SHARE_CODE = re.compile(r"^(?P<code>\d{6})\.(?P<market>SH|SZ|BJ)$", re.IGNORECASE)


def _is_a_share_stock(code: str) -> bool:
    """Limit minute-volume unit metadata to A-share equities, never ETFs/indexes."""
    matched = _A_SHARE_CODE.fullmatch(str(code or "").strip())
    if matched is None:
        return False
    digits = matched.group("code")
    market = matched.group("market").upper()
    if market == "SH":
        return digits.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "SZ":
        return digits.startswith(("000", "001", "002", "003", "300", "301"))
    return digits.startswith(("4", "8", "920"))


def _a_share_minute_volume_unit(code: str, source: str) -> str | None:
    """Report the raw unit, without changing public API numeric values."""
    if not _is_a_share_stock(code):
        return None
    normalized = str(source or "").strip().lower()
    if normalized == "qmt_local_dat.1m":
        return "shares"
    if normalized == "qmt_rpc_fallback.1m":
        return "lots"
    return None


def _local_dat_root() -> Path | None:
    raw = str(os.getenv("QMT_BRIDGE_LOCAL_DAT_ROOT") or "").strip()
    return Path(raw) if raw else None


def _cache_params(**kwargs) -> dict:
    params = dict(kwargs)
    if "stocks" in params:
        params["stocks"] = sorted(params["stocks"])
    return params


def _frame_has_rows(frame) -> bool:
    return frame is not None and not bool(getattr(frame, "empty", True))


def _frame_covers_request_end(frame, *, end_time: str, period: str) -> bool:
    if not _frame_has_rows(frame) or not end_time:
        return _frame_has_rows(frame)
    expected = "".join(character for character in str(end_time) if character.isdigit())
    if period == "1m":
        expected_end = _minute_session_boundary(end_time, end=True)
        latest = _frame_last_bar(frame, period=period)
        return bool(expected_end and latest and latest >= expected_end)
    width = 8
    if len(expected) < width:
        return True
    expected_end = expected[:width]
    latest = _frame_last_bar(frame, period=period)
    return bool(latest and latest >= expected_end)


def _frame_last_bar(frame, *, period: str) -> str | None:
    """Return the latest normalized exchange bar key from a frame."""
    if not _frame_has_rows(frame):
        return None
    width = 14 if period == "1m" else 8
    columns = getattr(frame, "columns", ())
    # Big QMT RPC returns epoch milliseconds in `time` and local exchange time
    # in `stime`; DAT reads use a date-time index. Compare the same time format.
    column = next((name for name in ("stime", "time") if name in columns), None)
    values = frame[column] if column is not None else frame.index
    observed = []
    for value in values:
        raw = str(value)
        if raw.isdigit() and len(raw) in {10, 13}:
            seconds = int(raw) / (1000 if len(raw) == 13 else 1)
            raw = datetime.fromtimestamp(seconds, timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
        observed.append("".join(character for character in raw if character.isdigit()))
    latest = max((value[:width] for value in observed if len(value) >= width), default="")
    return latest or None


def _frame_first_bar(frame, *, period: str) -> str | None:
    if not _frame_has_rows(frame):
        return None
    width = 14 if period == "1m" else 8
    columns = getattr(frame, "columns", ())
    column = next((name for name in ("stime", "time") if name in columns), None)
    values = frame[column] if column is not None else frame.index
    observed = []
    for value in values:
        raw = str(value)
        if raw.isdigit() and len(raw) in {10, 13}:
            seconds = int(raw) / (1000 if len(raw) == 13 else 1)
            raw = datetime.fromtimestamp(seconds, timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
        normalized = "".join(character for character in raw if character.isdigit())
        if len(normalized) >= width:
            observed.append(normalized[:width])
    return min(observed, default="") or None


def _minute_session_boundary(value: str, *, end: bool) -> str | None:
    digits = "".join(character for character in str(value or "") if character.isdigit())
    if len(digits) < 8:
        return None
    day = digits[:8]
    if len(digits) < 14:
        return f"{day}{'150000' if end else '093000'}"
    clock = digits[8:14]
    if end:
        if clock < "093000":
            return None
        if "113000" < clock < "130100":
            return f"{day}113000"
        return f"{day}{min(clock, '150000')}"
    if clock > "150000":
        return None
    if "113000" < clock < "130100":
        return f"{day}130100"
    return f"{day}{max(clock, '093000')}"


def _frame_covers_request_start(frame, *, start_time: str, period: str) -> bool:
    if not _frame_has_rows(frame) or not start_time:
        return False
    if period == "1m":
        expected_start = _minute_session_boundary(start_time, end=False)
    else:
        digits = "".join(character for character in str(start_time) if character.isdigit())
        expected_start = digits[:8] if len(digits) >= 8 else None
    first = _frame_first_bar(frame, period=period)
    return bool(expected_start and first and first <= expected_start)


def _confident_closed_window_coverage(frame, *, start_time: str, end_time: str,
                                      period: str, count: int) -> bool:
    """Only claim local/cache coverage when the whole requested range is provable.

    A positive count is a maximum, not a coverage contract: an old local tail
    can look valid while omitting the request's start.  Sparse native RPC data
    remains a valid response, but it is not promoted to a persistent cache hit.
    """
    if count != -1 or not start_time or not end_time:
        return False
    return (
        _frame_covers_request_start(frame, start_time=start_time, period=period)
        and _frame_covers_request_end(frame, end_time=end_time, period=period)
    )


def _is_fixed_closed_historical_window(end_time: str) -> bool:
    """DAT and disk cache are only authoritative for a closed prior date."""
    digits = "".join(character for character in str(end_time or "") if character.isdigit())
    if len(digits) < 8:
        return False
    try:
        requested_date = datetime.strptime(digits[:8], "%Y%m%d").date()
    except ValueError:
        return False
    return requested_date < datetime.now(timezone(timedelta(hours=8))).date()


def _coverage_metadata(raw: dict, stocks: list[str], *, period: str,
                       start_time: str, end_time: str, count: int) -> dict[str, dict[str, object]]:
    return {
        stock: {
            "last_bar": _frame_last_bar((raw or {}).get(stock), period=period),
            "covers_requested_end": _frame_covers_request_end(
                (raw or {}).get(stock), end_time=end_time, period=period
            ),
            "covers_requested_range": _confident_closed_window_coverage(
                (raw or {}).get(stock), start_time=start_time, end_time=end_time,
                period=period, count=count,
            ),
            "requested_start": start_time or None,
            "requested_end": end_time or None,
        }
        for stock in stocks
    }


def _all_requested_frames_have_rows(raw: dict, stock_list: list[str]) -> bool:
    if not isinstance(raw, dict) or not stock_list:
        return False
    return all(_frame_has_rows(raw.get(stock)) for stock in stock_list)


def _any_market_data_frame_has_rows(raw: dict) -> bool:
    if not isinstance(raw, dict):
        return False
    return any(_frame_has_rows(frame) for frame in raw.values())


def _is_mapping(raw) -> bool:
    return isinstance(raw, dict)


def _is_divid_factor_payload(raw) -> bool:
    return isinstance(raw, dict) or hasattr(raw, "to_dict")


def _read_market_payload(function_name, loader, *, validator=None):
    with span("market.router.provider_call", function=function_name) as result:
        try:
            raw = loader()
        except XtdataTransportStuckError as exc:
            result.update({"outcome": "unavailable", "error_type": exc.__class__.__name__})
            return None, _status_payload(
                "unavailable", reason="xtdata_transport_stuck", function=function_name,
                error_type=exc.__class__.__name__, detail=str(exc),
            )
        except Exception as exc:
            result.update({"outcome": _exception_status(exc), "error_type": exc.__class__.__name__})
            return None, _status_payload(
                _exception_status(exc), reason=str(exc), function=function_name,
                error_type=exc.__class__.__name__,
            )
        if raw is None:
            result["outcome"] = "empty"
            return None, _status_payload(
                "unavailable", reason=f"xtdata_{function_name}_returned_none", function=function_name,
            )
        if validator is not None and not validator(raw):
            result.update({"outcome": "unknown", "response_type": type(raw).__name__})
            return None, _status_payload(
                "error", reason=f"xtdata_{function_name}_invalid_response", function=function_name,
                error_type=type(raw).__name__,
            )
        result.update({"outcome": "success", "response_type": type(raw).__name__})
        return raw, None


def _call_market_optional(function_name, *args, **kwargs):
    function = getattr(market_data, function_name, None)
    if not callable(function):
        return _status_payload(
            "unsupported",
            reason=f"xtdata_{function_name}_missing",
            function=function_name,
        )
    raw, error = _read_market_payload(
        function_name,
        lambda: _call_xtdata_serialized(function, *args, **kwargs),
    )
    if error is not None:
        return error
    return _status_payload("ok", data=raw, function=function_name)


@router.get("/snapshot")
def get_market_snapshot(
    stocks: str = Query(..., description="股票/指数代码列表，逗号分隔，如 000001.SH,000001.SZ"),
):
    stock_list = _split_stocks(stocks)
    raw, error = _read_market_payload(
        "get_full_tick",
        lambda: _call_xtdata_serialized(market_data.get_full_tick, code_list=stock_list),
        validator=_is_mapping,
    )
    if error is not None:
        return error
    return {"data": _numpy_to_python(raw)}


@router.get("/indices")
def get_major_indices():
    raw, error = _read_market_payload(
        "get_full_tick",
        lambda: _call_xtdata_serialized(market_data.get_full_tick, code_list=MAJOR_INDICES),
        validator=_is_mapping,
    )
    if error is not None:
        return error
    return {"indices": MAJOR_INDICES, "data": _numpy_to_python(raw)}


@router.get("/history_ex")
def get_history_ex(
    stocks: str = Query(..., description="股票代码列表，逗号分隔，如 000001.SZ,600519.SH"),
    period: str = Query("1d", description="K线周期: tick/1m/5m/15m/30m/60m/1d"),
    start_time: str = Query("", description="开始时间 YYYYMMDD 或 YYYYMMDDHHmmss"),
    end_time: str = Query("", description="结束时间"),
    count: int = Query(-1, description="返回条数，-1 表示不限"),
    dividend_type: str = Query("none", description="除权类型: none/front/back/front_ratio/back_ratio"),
    fill_data: bool = Query(True, description="是否填充空数据"),
    use_cache: bool = Query(False, alias="cache", description="是否使用本地二进制缓存"),
):
    stock_list = _split_stocks(stocks)
    local_dat_root = _local_dat_root()
    fixed_closed_window = _is_fixed_closed_historical_window(end_time)
    use_local_dat = (
        local_dat_root is not None
        and period in {"1d", "1m"}
        and dividend_type == "none"
        and fixed_closed_window
    )
    emit("market.history.path_selected", outcome="started", period=period,
         stock_count=len(stock_list), requested_count=count,
         dat_eligible=use_local_dat, cache_requested=use_cache)
    params = _cache_params(
        stocks=stock_list,
        period=period,
        start_time=start_time,
        end_time=end_time,
        count=count,
        dividend_type=dividend_type,
        fill_data=fill_data,
        source_identity="qmt_rpc",
    )

    response_source: str | None = None
    response_source_by_stock: dict[str, str] | None = None

    def _load(requested_stocks: list[str] | None = None):
        return _call_xtdata_serialized(
            market_data.get_market_data_ex,
            field_list=[],
            stock_list=requested_stocks or stock_list,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )

    def _read():
        nonlocal response_source, response_source_by_stock
        if use_local_dat and local_dat_root is not None:
            reader = (
                read_qmt_local_dat_1d
                if period == "1d"
                else read_qmt_local_dat_1m
            )
            local_raw = reader(
                local_dat_root,
                tuple(stock_list),
                start_time=start_time,
                end_time=end_time,
                count=count,
            )
            missing_stocks = [
                stock
                for stock in stock_list
                if not _confident_closed_window_coverage(
                    local_raw.get(stock),
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    count=count,
                )
            ]
            emit("market.history.dat_coverage", outcome="success" if not missing_stocks else "partial",
                 period=period, stock_count=len(stock_list), missing_count=len(missing_stocks))
            if not missing_stocks:
                response_source = f"qmt_local_dat.{period}"
                if period == "1m":
                    response_source_by_stock = {
                        stock: "qmt_local_dat.1m" for stock in stock_list
                    }
                return local_raw
            rpc_raw = _load(missing_stocks)
            merged = dict(rpc_raw or {})
            missing_set = set(missing_stocks)
            accepted_local = False
            for stock, frame in local_raw.items():
                if stock not in missing_set and _frame_has_rows(frame):
                    merged[stock] = frame
                    accepted_local = True
            response_source = (
                f"qmt_local_dat.{period}+qmt_rpc"
                if accepted_local
                else f"qmt_rpc_fallback.{period}"
            )
            if period == "1m":
                response_source_by_stock = {
                    stock: (
                        "qmt_rpc_fallback.1m"
                        if stock in missing_set
                        else "qmt_local_dat.1m"
                    )
                    for stock in stock_list
                }
            emit("market.history.rpc_merge", outcome="partial" if accepted_local else "success",
                 period=period, rpc_stock_count=len(missing_stocks), local_stock_count=len(stock_list) - len(missing_stocks))
            return merged
        if not use_cache or not fixed_closed_window:
            raw = _load()
            response_source = f"qmt_rpc.{period}"
            if period == "1m":
                response_source_by_stock = {
                    stock: "qmt_rpc_fallback.1m" for stock in stock_list
                }
            return raw
        raw, _cache_meta = get_binary_cache().cached_call(
            "market_history_ex",
            params,
            _load,
            should_store=lambda data: (
                isinstance(data, dict)
                and all(
                    _confident_closed_window_coverage(
                        data.get(stock), start_time=start_time, end_time=end_time,
                        period=period, count=count,
                    )
                    for stock in stock_list
                )
            ),
            extra_metadata={"stock_count": len(stock_list), "period": period, "source_identity": "qmt_rpc"},
        )
        emit("market.history.binary_cache", outcome="success" if _cache_meta.get("hit") else "empty",
             period=period, hit=bool(_cache_meta.get("hit")), stored=bool(_cache_meta.get("stored")))
        response_source = f"qmt_rpc_cache.{period}" if _cache_meta.get("hit") else f"qmt_rpc.{period}"
        if period == "1m":
            response_source_by_stock = {
                stock: "qmt_rpc_fallback.1m" for stock in stock_list
            }
        return raw

    raw, error = _read_market_payload("get_market_data_ex", _read, validator=_is_mapping)
    if error is not None:
        emit("market.history.result", outcome=str(error.get("status") or "error"),
             period=period, stock_count=len(stock_list), source=response_source or "unavailable",
             reason_code=str(error.get("reason_code") or "market_provider_error"), critical=True)
        return error
    payload = {"data": _dataframe_dict_to_records(raw or {})}
    payload["coverage_by_stock"] = _coverage_metadata(
        raw or {}, stock_list, period=period, start_time=start_time, end_time=end_time, count=count
    )
    if response_source is not None:
        payload["source"] = response_source
    if period == "1m" and response_source_by_stock is not None:
        payload["source_by_stock"] = response_source_by_stock
        payload["volume_unit_by_stock"] = {
            stock: unit
            for stock, source in response_source_by_stock.items()
            if (unit := _a_share_minute_volume_unit(stock, source)) is not None
        }
    total_rows = sum(len(rows) for rows in payload["data"].values())
    emit("market.history.result", outcome="success" if total_rows else "empty", period=period,
         stock_count=len(stock_list), returned_rows=total_rows, source=response_source or "unknown")
    return payload


@router.get("/local_data")
def get_local_data(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    period: str = Query("1d", description="K线周期"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
    count: int = Query(-1, description="返回条数"),
    dividend_type: str = Query("none", description="除权类型"),
    fill_data: bool = Query(True, description="是否填充空数据"),
    use_cache: bool = Query(False, alias="cache", description="是否使用本地二进制缓存"),
):
    stock_list = _split_stocks(stocks)
    params = _cache_params(
        stocks=stock_list,
        period=period,
        start_time=start_time,
        end_time=end_time,
        count=count,
        dividend_type=dividend_type,
        fill_data=fill_data,
    )

    def _load():
        return _call_xtdata_serialized(
            market_data.get_local_data,
            field_list=[],
            stock_list=stock_list,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )

    def _read():
        if not use_cache:
            return _load()
        raw, _cache_meta = get_binary_cache().cached_call(
            "market_local_data",
            params,
            _load,
            should_store=lambda data: _all_requested_frames_have_rows(data, stock_list),
            extra_metadata={"stock_count": len(stock_list), "period": period},
        )
        emit("market.local_data.binary_cache", outcome="success" if _cache_meta.get("hit") else "empty",
             period=period, hit=bool(_cache_meta.get("hit")), stored=bool(_cache_meta.get("stored")))
        return raw

    raw, error = _read_market_payload("get_local_data", _read, validator=_is_mapping)
    if error is not None:
        return error
    payload = {"data": _dataframe_dict_to_records(raw)}
    emit("market.local_data.result", outcome="success" if any(payload["data"].values()) else "empty",
         period=period, stock_count=len(stock_list))
    return payload


@router.get("/minute_tail")
def get_minute_tail(
    stocks: str = Query(...), start_time: str = Query(...), end_time: str = Query(...),
    count: int = Query(3, ge=1, le=60), refresh_missing: bool = Query(True),
):
    """Return valid closed raw minutes without requiring a completed trading day."""
    stock_list = tuple(_split_stocks(stocks))
    emit("market.minute_tail.request", outcome="started", stock_count=len(stock_list),
         requested_count=count, refresh_missing=refresh_missing)
    root = _local_dat_root()
    if root is None:
        emit("market.minute_tail.result", outcome="unavailable", stock_count=len(stock_list),
             requested_count=count, reason_code="qmt_local_dat_root_unavailable", critical=True)
        return {"status": "unavailable", "reason": "qmt_local_dat_root_unavailable", "data": {}}
    try:
        local = read_closed_minute_tail(root, stock_list, start_time=start_time,
                                       end_time=end_time, count=count)
    except (ValueError, OSError) as exc:
        emit("market.minute_tail.result", outcome="error", stock_count=len(stock_list),
             requested_count=count, reason_code="minute_tail_local_dat_read_failed", critical=True)
        return {"status": "error", "reason": str(exc), "data": {}}
    sources = {code: "qmt_local_dat.1m" for code in stock_list}
    missing = [code for code in stock_list if not _frame_covers_request_end(
        local.get(code), end_time=end_time, period="1m")]
    rpc_error = None
    rpc_attempted = False
    if missing and refresh_missing:
        rpc_attempted = True
        emit("market.minute_tail.rpc_lane", outcome="started", missing_count=len(missing),
             lane="minute")
        fetched, rpc_error = _read_market_payload(
            "get_market_data_ex_scoped",
            lambda: _call_xtdata_serialized_with_budget(
                8.0,
                lambda remaining: market_data.get_market_data_ex_scoped(
                    stock_list=missing, start_time=start_time, end_time=end_time,
                    count=count, timeout_seconds=remaining,
                ),
            ), validator=_is_mapping,
        )
        if rpc_error is None:
            for code in missing:
                frame = (fetched or {}).get(code)
                if _frame_covers_request_end(frame, end_time=end_time, period="1m"):
                    local[code] = frame
                    sources[code] = "qmt_rpc_fallback.1m"
    missing = [code for code in stock_list if not _frame_covers_request_end(
        local.get(code), end_time=end_time, period="1m")]
    volume_unit_by_stock = {
        code: unit
        for code, source in sources.items()
        if (unit := _a_share_minute_volume_unit(code, source)) is not None
    }
    payload = {"status": "partial" if missing else "ok",
            "data": _dataframe_dict_to_records(local), "source_by_stock": sources,
            "volume_unit_by_stock": volume_unit_by_stock,
            "missing_stocks": missing, "rpc_error": rpc_error,
            "period": "1m", "adjustment_mode": "none", "end_time": end_time}
    emit("market.minute_tail.result", outcome="partial" if missing else "success",
         stock_count=len(stock_list), missing_count=len(missing),
         rpc_attempted=rpc_attempted)
    return payload


@router.get("/divid_factors")
def get_divid_factors(
    stock: str = Query(..., description="股票代码，如 000001.SZ"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
    use_cache: bool = Query(False, alias="cache", description="是否使用本地二进制缓存"),
):
    params = _cache_params(stock=stock, start_time=start_time, end_time=end_time)

    def _load():
        return _call_xtdata_serialized(
            market_data.get_divid_factors,
            stock,
            start_time=start_time,
            end_time=end_time,
        )

    def _read():
        if not use_cache:
            return _load()
        raw, _cache_meta = get_binary_cache().cached_call(
            "market_divid_factors",
            params,
            _load,
            should_store=_frame_has_rows,
            extra_metadata={"stock": stock},
        )
        return raw

    raw, error = _read_market_payload(
        "get_divid_factors",
        _read,
        validator=_is_divid_factor_payload,
    )
    if error is not None:
        return error
    data = (
        _dataframe_dict_to_records({stock: raw}).get(stock, [])
        if hasattr(raw, "to_dict")
        else _numpy_to_python(raw)
    )
    return {"stock": stock, "data": data}


# ---------------------------------------------------------------------------
# New endpoints (Step 5)
# ---------------------------------------------------------------------------


@router.get("/market_data")
def get_market_data(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    fields: str = Query("open,high,low,close,volume", description="字段列表，逗号分隔"),
    period: str = Query("1d", description="K线周期"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
    count: int = Query(-1, description="返回条数"),
    dividend_type: str = Query("none", description="除权类型"),
    fill_data: bool = Query(True, description="是否填充空数据"),
    use_cache: bool = Query(False, alias="cache", description="是否使用本地二进制缓存"),
):
    """Get market data via get_market_data (original API)."""
    from ..helpers import _market_data_to_records

    stock_list = _split_stocks(stocks)
    field_list = [f.strip() for f in fields.split(",")]
    params = _cache_params(
        stocks=stock_list,
        fields=field_list,
        period=period,
        start_time=start_time,
        end_time=end_time,
        count=count,
        dividend_type=dividend_type,
        fill_data=fill_data,
    )

    def _load():
        return _call_xtdata_serialized(
            market_data.get_market_data,
            field_list=field_list,
            stock_list=stock_list,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )

    def _read():
        if not use_cache:
            return _load()
        raw, _cache_meta = get_binary_cache().cached_call(
            "market_data",
            params,
            _load,
            should_store=_any_market_data_frame_has_rows,
            extra_metadata={"stock_count": len(stock_list), "period": period},
        )
        return raw

    raw, error = _read_market_payload("get_market_data", _read, validator=_is_mapping)
    if error is not None:
        return error
    records = _market_data_to_records(raw, stock_list, field_list)
    return {"data": records}


@router.get("/market_data3")
def get_market_data3(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    fields: str = Query("", description="字段列表，逗号分隔，为空取全部"),
    period: str = Query("1d", description="K线周期"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
    count: int = Query(-1, description="返回条数"),
    dividend_type: str = Query("none", description="除权类型"),
    fill_data: bool = Query(True, description="是否填充空数据"),
    use_cache: bool = Query(False, alias="cache", description="是否使用本地二进制缓存"),
):
    """Get market data via get_market_data3 (returns dict of DataFrames)."""
    stock_list = _split_stocks(stocks)
    field_list = [f.strip() for f in fields.split(",") if f.strip()] if fields else []
    params = _cache_params(
        stocks=stock_list,
        fields=field_list,
        period=period,
        start_time=start_time,
        end_time=end_time,
        count=count,
        dividend_type=dividend_type,
        fill_data=fill_data,
    )

    def _load():
        return _call_xtdata_serialized(
            market_data.get_market_data3,
            field_list=field_list,
            stock_list=stock_list,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )

    def _read():
        if not use_cache:
            return _load()
        raw, _cache_meta = get_binary_cache().cached_call(
            "market_data3",
            params,
            _load,
            should_store=lambda data: _all_requested_frames_have_rows(data, stock_list),
            extra_metadata={"stock_count": len(stock_list), "period": period},
        )
        return raw

    raw, error = _read_market_payload("get_market_data3", _read, validator=_is_mapping)
    if error is not None:
        return error
    return {"data": _dataframe_dict_to_records(raw)}


@router.get("/full_kline")
def get_full_kline(
    stock: str = Query(..., description="股票代码"),
    period: str = Query("1d", description="K线周期"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    """Get full K-line data for a single stock."""
    raw, error = _read_market_payload(
        "get_full_kline",
        lambda: _call_xtdata_serialized(
            market_data.get_full_kline,
            stock,
            period=period,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    if error is not None:
        return error
    return {"stock": stock, "data": _numpy_to_python(raw)}


@router.post("/subscribe_warmup")
def subscribe_warmup(req: dict = Body(default_factory=dict)):
    """Warm QMT realtime/minute data through a short subscription."""
    stocks = req.get("stocks") or req.get("stock") or req.get("code")
    if isinstance(stocks, str):
        stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    elif isinstance(stocks, list):
        stock_list = [str(s).strip() for s in stocks if str(s).strip()]
    else:
        stock_list = []
    if not stock_list:
        return {"status": "error", "reason": "no_stocks_requested", "data": None}
    stock = stock_list[0]
    period = str(req.get("period") or "1m")
    keep_subscription = bool(req.get("keep_subscription") or False)

    subscribe_payload = _call_market_optional(
        "subscribe_quote",
        stock,
        period=period,
        start_time=str(req.get("start_time") or ""),
        end_time=str(req.get("end_time") or ""),
        count=int(req.get("count") or 0),
    )
    if subscribe_payload["status"] != "ok":
        return subscribe_payload

    seq = subscribe_payload.get("data")
    unsubscribe_payload = None
    if not keep_subscription and seq not in (None, "", 0):
        unsubscribe_payload = _call_market_optional("unsubscribe_quote", seq)
    return {
        "status": "ok",
        "data": {
            "stock": stock,
            "period": period,
            "seq": seq,
            "keep_subscription": keep_subscription,
            "unsubscribe": unsubscribe_payload,
        },
    }


@router.post("/unsubscribe")
def unsubscribe_quote(req: dict = Body(default_factory=dict)):
    seq = req.get("seq")
    if seq in (None, ""):
        seq = req.get("subscription_id")
    if seq in (None, ""):
        seq = req.get("sub_id")
    if seq in (None, ""):
        return {"status": "error", "reason": "missing_subscription_id", "data": None}
    return _call_market_optional("unsubscribe_quote", seq)


@router.get("/subscriptions")
def get_all_subscription():
    return _call_market_optional("get_all_subscription")
