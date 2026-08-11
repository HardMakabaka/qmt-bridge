"""Router — Market data endpoints /api/market/*."""

import os
from pathlib import Path

from fastapi import APIRouter, Body, Query
from ..bigqmt import xtdata

from ..binary_cache import get_binary_cache
from ..helpers import (
    XtdataTransportStuckError,
    _call_xtdata_serialized,
    _dataframe_dict_to_records,
    _exception_status,
    _numpy_to_python,
    _status_payload,
)
from ..qmt_local_dat import read_qmt_local_dat_1d, read_qmt_local_dat_1m

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
    try:
        raw = loader()
    except XtdataTransportStuckError as exc:
        return None, _status_payload(
            "unavailable",
            reason="xtdata_transport_stuck",
            function=function_name,
            error_type=exc.__class__.__name__,
            detail=str(exc),
        )
    except Exception as exc:
        return None, _status_payload(
            _exception_status(exc),
            reason=str(exc),
            function=function_name,
            error_type=exc.__class__.__name__,
        )
    if raw is None:
        return None, _status_payload(
            "unavailable",
            reason=f"xtdata_{function_name}_returned_none",
            function=function_name,
        )
    if validator is not None and not validator(raw):
        return None, _status_payload(
            "error",
            reason=f"xtdata_{function_name}_invalid_response",
            function=function_name,
            error_type=type(raw).__name__,
        )
    return raw, None


def _call_market_optional(function_name, *args, **kwargs):
    function = getattr(xtdata, function_name, None)
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
        lambda: _call_xtdata_serialized(xtdata.get_full_tick, code_list=stock_list),
        validator=_is_mapping,
    )
    if error is not None:
        return error
    return {"data": _numpy_to_python(raw)}


@router.get("/indices")
def get_major_indices():
    raw, error = _read_market_payload(
        "get_full_tick",
        lambda: _call_xtdata_serialized(xtdata.get_full_tick, code_list=MAJOR_INDICES),
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
    use_local_dat = (
        local_dat_root is not None
        and period in {"1d", "1m"}
        and dividend_type == "none"
    )
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
            xtdata.get_market_data_ex,
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
        if use_local_dat:
            reader = (
                read_qmt_local_dat_1d
                if period == "1d"
                else read_qmt_local_dat_1m
            )
            return reader(
                local_dat_root,
                tuple(stock_list),
                start_time=start_time,
                end_time=end_time,
                count=count,
            )
        if not use_cache:
            return _load()
        raw, _cache_meta = get_binary_cache().cached_call(
            "market_history_ex",
            params,
            _load,
            should_store=lambda data: _all_requested_frames_have_rows(data, stock_list),
            extra_metadata={"stock_count": len(stock_list), "period": period},
        )
        return raw

    raw, error = _read_market_payload("get_market_data_ex", _read, validator=_is_mapping)
    if error is not None:
        return error
    payload = {"data": _dataframe_dict_to_records(raw)}
    if use_local_dat:
        payload["source"] = f"qmt_local_dat.{period}"
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
            xtdata.get_local_data,
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
        return raw

    raw, error = _read_market_payload("get_local_data", _read, validator=_is_mapping)
    if error is not None:
        return error
    return {"data": _dataframe_dict_to_records(raw)}


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
            xtdata.get_divid_factors,
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
            xtdata.get_market_data,
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
            xtdata.get_market_data3,
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
            xtdata.get_full_kline,
            stock,
            period=period,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    if error is not None:
        return error
    return {"stock": stock, "data": _numpy_to_python(raw)}


@router.get("/fullspeed_orderbook")
def get_fullspeed_orderbook(
    stock: str = Query(..., description="股票代码"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    """Get full-speed order book data."""
    raw, error = _read_market_payload(
        "get_fullspeed_orderbook",
        lambda: _call_xtdata_serialized(
            xtdata.get_fullspeed_orderbook,
            stock,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    if error is not None:
        return error
    return {"stock": stock, "data": _numpy_to_python(raw)}


@router.get("/transactioncount")
def get_transactioncount(
    stock: str = Query(..., description="股票代码"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    """Get transaction count data."""
    raw, error = _read_market_payload(
        "get_transactioncount",
        lambda: _call_xtdata_serialized(
            xtdata.get_transactioncount,
            stock,
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
