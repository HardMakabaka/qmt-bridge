"""QMT Bridge — data conversion helpers."""

import math
import threading

import numpy as np
import pandas as pd


_OBJECT_FIELDS = (
    "account_id",
    "cash",
    "frozen_cash",
    "market_value",
    "total_asset",
    "stock_code",
    "volume",
    "can_use_volume",
    "frozen_volume",
    "open_price",
    "order_id",
    "order_sysid",
    "order_type",
    "order_volume",
    "price_type",
    "price",
    "traded_id",
    "traded_time",
    "traded_volume",
    "traded_price",
    "status",
    "status_msg",
    "strategy_name",
    "order_remark",
    "error_id",
    "error_msg",
    "m_strAccountID",
    "m_strInstrumentID",
    "m_nVolume",
    "m_nCanUseVolume",
    "m_dCash",
    "m_dAvailable",
    "m_dFrozenCash",
    "m_dMarketValue",
    "m_dTotalAsset",
    "m_dPrice",
    "m_dTradedPrice",
)

_XTDATA_LOCK_POLL_SECONDS = 0.005


class XtdataCallCancelledError(RuntimeError):
    pass


class XtdataTransportStuckError(RuntimeError):
    pass


class _XtdataTransportCoordinator:
    def __init__(self) -> None:
        self._call_lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._stuck_reason: str | None = None

    def call(self, cancel_event, function, *args, **kwargs):
        while True:
            self._raise_if_unavailable(cancel_event)
            if self._call_lock.acquire(timeout=_XTDATA_LOCK_POLL_SECONDS):
                break
        try:
            self._raise_if_unavailable(cancel_event)
            return function(*args, **kwargs)
        finally:
            self._call_lock.release()

    def mark_stuck(self, reason: str) -> None:
        with self._state_lock:
            if self._stuck_reason is None:
                self._stuck_reason = reason

    def reset_for_tests(self) -> None:
        with self._state_lock:
            self._stuck_reason = None

    def _raise_if_unavailable(self, cancel_event) -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise XtdataCallCancelledError(
                "xtdata call canceled before transport entry"
            )
        with self._state_lock:
            stuck_reason = self._stuck_reason
        if stuck_reason is not None:
            raise XtdataTransportStuckError(
                f"xtdata transport unavailable until bridge restart: {stuck_reason}"
            )


_XTDATA_TRANSPORT = _XtdataTransportCoordinator()


def _call_xtdata_serialized(function, *args, **kwargs):
    return _XTDATA_TRANSPORT.call(None, function, *args, **kwargs)


def _call_xtdata_serialized_cancellable(cancel_event, function, *args, **kwargs):
    return _XTDATA_TRANSPORT.call(cancel_event, function, *args, **kwargs)


def _mark_xtdata_transport_stuck(reason: str) -> None:
    _XTDATA_TRANSPORT.mark_stuck(reason)


def _reset_xtdata_transport_for_tests() -> None:
    _XTDATA_TRANSPORT.reset_for_tests()


def _numpy_to_python(obj):
    """Recursively convert numpy types in a nested structure to Python types."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _numpy_to_python(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_numpy_to_python(i) for i in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        value = float(obj)
        return value if math.isfinite(value) else None
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if hasattr(obj, "_asdict"):
        return _numpy_to_python(obj._asdict())
    if hasattr(obj, "__dict__"):
        return {k: _numpy_to_python(v) for k, v in vars(obj).items() if not k.startswith("_")}
    object_fields = {field: _numpy_to_python(getattr(obj, field)) for field in _OBJECT_FIELDS if hasattr(obj, field)}
    if object_fields:
        return object_fields
    return obj


def _exception_status(exc: Exception) -> str:
    message = str(exc)
    if (
        isinstance(exc, AttributeError)
        or "function not realize" in message
        or "未支持此功能" in message
    ):
        return "unsupported"
    return "error"


def _status_payload(
    status: str,
    *,
    data=None,
    reason: str = "",
    function: str = "",
    **extra,
):
    """Build a stable JSON payload for optional QMT functions."""
    payload = {
        "status": status,
        "data": _numpy_to_python(data),
    }
    if reason:
        payload["reason"] = reason
    if function:
        payload["function"] = function
    payload.update(extra)
    return payload


def _call_xtdata_optional(
    xtdata_module,
    function_name: str,
    *args,
    missing_reason: str | None = None,
    unavailable_when_none: bool = False,
    **kwargs,
):
    """Call an optional xtdata function and never disguise support gaps as data."""
    func = getattr(xtdata_module, function_name, None)
    if not callable(func):
        return _status_payload(
            "unsupported",
            reason=missing_reason or f"xtdata_{function_name}_missing",
            function=function_name,
        )
    try:
        raw = _call_xtdata_serialized(func, *args, **kwargs)
    except Exception as exc:
        return _status_payload(
            _exception_status(exc),
            reason=str(exc),
            function=function_name,
            error_type=exc.__class__.__name__,
        )
    if raw is None and unavailable_when_none:
        return _status_payload(
            "unavailable",
            reason=f"xtdata_{function_name}_returned_none",
            function=function_name,
        )
    return _status_payload("ok", data=raw, function=function_name)


def _market_data_to_records(
    raw: dict, stock_list: list[str], field_list: list[str]
) -> dict[str, list[dict]]:
    """Convert xtdata.get_market_data() result to JSON-friendly records.

    raw is {field: DataFrame} where each DataFrame has stocks as rows and
    timestamps as columns.  We pivot into {stock: [{date, field1, field2, ...}]}.
    """
    result: dict[str, list[dict]] = {}
    for stock in stock_list:
        rows: dict[str, dict] = {}
        for field in field_list:
            df = raw.get(field)
            if df is None:
                continue
            if stock in df.index:
                for date, value in df.loc[stock].items():
                    entry = rows.setdefault(str(date), {"date": str(date)})
                    raw_value = value.item() if hasattr(value, "item") else value
                    entry[field] = _numpy_to_python(raw_value)
        result[stock] = [_numpy_to_python(row) for row in rows.values()]
    return result


def _dataframe_dict_to_records(data: dict) -> dict[str, list[dict]]:
    """Convert {stock: DataFrame} format (get_market_data_ex / get_local_data return value).

    Returns {stock: [row_dict, ...]} where each row_dict includes all columns.
    """
    result: dict[str, list[dict]] = {}
    for stock, df in data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            records_frame = (
                df
                if isinstance(df.index, pd.RangeIndex) and df.index.name is None
                else df.reset_index()
            )
            records = records_frame.to_dict(orient="records")
            result[stock] = [_numpy_to_python(r) for r in records]
        else:
            result[stock] = []
    return result


def _financial_data_to_records(data: dict) -> dict:
    """Convert {stock: {table: DataFrame}} format (get_financial_data return value).

    Returns {stock: {table: [row_dict, ...]}}.
    """
    result: dict = {}
    for stock, tables in data.items():
        stock_data: dict = {}
        if isinstance(tables, dict):
            for table_name, df in tables.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    records = df.reset_index().to_dict(orient="records")
                    stock_data[table_name] = [_numpy_to_python(r) for r in records]
                else:
                    stock_data[table_name] = []
        result[stock] = stock_data
    return result
