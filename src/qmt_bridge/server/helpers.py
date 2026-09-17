"""QMT Bridge — data conversion helpers."""

import math
import os
import threading
import time

import numpy as np
import pandas as pd

from bigqmt_signal_trader.request_budget import current_deadline, request_budget
from bigqmt_signal_trader import telemetry


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


class NativePayloadError(ValueError):
    """Native data could not be represented without discarding its content."""


class _XtdataTransportCoordinator:
    def __init__(self, lane="market") -> None:
        self.lane = lane
        self._call_lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._stuck_reason: str | None = None

    def call(self, cancel_event, function, *args, **kwargs):
        with telemetry.span("native.lane", lane=self.lane, method=getattr(function, "__name__", "callable")) as result:
            result["call_started"] = False
            return self._call_observed(result, cancel_event, function, *args, **kwargs)

    def _call_observed(self, result, cancel_event, function, *args, **kwargs):
        waiting = time.monotonic()
        budget = float(os.environ.get("QMT_BRIDGE_RPC_TIMEOUT_SECONDS", "6"))
        deadline = time.monotonic() + budget
        inherited = current_deadline()
        if inherited is not None:
            deadline = min(deadline, inherited)
        while True:
            try:
                self._raise_if_unavailable(cancel_event)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    result["outcome"] = "timeout"
                    result["phase"] = "lock_wait"
                    raise TimeoutError("native request deadline expired waiting for transport")
                if self._call_lock.acquire(timeout=min(_XTDATA_LOCK_POLL_SECONDS, remaining)):
                    break
            finally:
                result["queue_wait_ms"] = round((time.monotonic() - waiting) * 1000, 3)
        try:
            self._raise_if_unavailable(cancel_event)
            if time.monotonic() >= deadline:
                result.update(outcome="timeout", phase="before_call")
                raise TimeoutError("native request deadline expired before invocation")
            with request_budget(deadline):
                result.update(call_started=True, remaining_budget_ms=max(0, (deadline - time.monotonic()) * 1000))
                with telemetry.span("native.lane.invoke", lane=self.lane):
                    return function(*args, **kwargs)
        finally:
            self._call_lock.release()

    def call_with_budget(self, budget_seconds, callback):
        """Enter the serialized transport only while a total deadline remains.

        The callback receives the remaining budget and is responsible for
        passing it to the native RPC client.  It is never invoked after the
        lock wait has consumed the deadline.
        """
        with telemetry.span("native.lane", lane=self.lane, method=getattr(callback, "__name__", "callable")) as result:
            result["call_started"] = False
            return self._call_with_budget_observed(result, budget_seconds, callback)

    def _call_with_budget_observed(self, result, budget_seconds, callback):
        waiting = time.monotonic()
        deadline = waiting + max(float(budget_seconds), 0.0)
        while True:
            try:
                self._raise_if_unavailable(None)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    result.update(outcome="timeout", phase="lock_wait")
                    raise TimeoutError("xtdata call budget expired before transport entry")
                if self._call_lock.acquire(
                    timeout=min(_XTDATA_LOCK_POLL_SECONDS, remaining)
                ):
                    break
            finally:
                result["queue_wait_ms"] = round((time.monotonic() - waiting) * 1000, 3)
        try:
            self._raise_if_unavailable(None)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                result.update(outcome="timeout", phase="before_call")
                raise TimeoutError("xtdata call budget expired before native invocation")
            with request_budget(deadline):
                result.update(call_started=True, remaining_budget_ms=remaining * 1000)
                with telemetry.span("native.lane.invoke", lane=self.lane):
                    return callback(remaining)
        finally:
            self._call_lock.release()

    def mark_stuck(self, reason: str) -> None:
        with self._state_lock:
            if self._stuck_reason is None:
                self._stuck_reason = reason
                telemetry.emit("native.lane.blocked", critical=True, lane=self.lane, reason_code="transport_stuck")

    def status(self) -> dict:
        with self._state_lock:
            return {"status": "blocked" if self._stuck_reason is not None else "available",
                    "reason": self._stuck_reason}

    def reset_for_tests(self) -> None:
        with self._state_lock:
            self._stuck_reason = None

    def _raise_if_unavailable(self, cancel_event) -> None:
        if cancel_event is not None and cancel_event.is_set():
            telemetry.emit("native.lane.rejected", lane=self.lane, outcome="canceled", call_started=False)
            raise XtdataCallCancelledError(
                "xtdata call canceled before transport entry"
            )
        with self._state_lock:
            stuck_reason = self._stuck_reason
        if stuck_reason is not None:
            telemetry.emit("native.lane.rejected", lane=self.lane, outcome="rejected",
                           reason_code="transport_stuck", call_started=False)
            raise XtdataTransportStuckError(
                f"xtdata transport unavailable until bridge restart: {stuck_reason}"
            )


_XTDATA_TRANSPORT = _XtdataTransportCoordinator()
_MINUTE_XTDATA_TRANSPORT = _XtdataTransportCoordinator(lane="minute")


def _call_xtdata_serialized(function, *args, **kwargs):
    return _XTDATA_TRANSPORT.call(None, function, *args, **kwargs)


def _call_xtdata_serialized_cancellable(cancel_event, function, *args, **kwargs):
    return _XTDATA_TRANSPORT.call(cancel_event, function, *args, **kwargs)


def _call_xtdata_serialized_with_budget(budget_seconds, callback):
    return _XTDATA_TRANSPORT.call_with_budget(budget_seconds, callback)


def _call_minute_xtdata_serialized_with_budget(budget_seconds, callback):
    # Keep the shared native-unavailable guard, not the unrelated bulk-read lock.
    _XTDATA_TRANSPORT._raise_if_unavailable(None)
    return _MINUTE_XTDATA_TRANSPORT.call_with_budget(budget_seconds, callback)


def _mark_xtdata_transport_stuck(reason: str) -> None:
    _XTDATA_TRANSPORT.mark_stuck(reason)


def get_xtdata_transport_status() -> dict:
    return _XTDATA_TRANSPORT.status()


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
        return _numpy_to_python(obj.tolist())
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
    if getattr(exc, "code", None) == "OVERLOADED":
        return "overloaded"
    if getattr(exc, "code", None) == "DEADLINE_EXCEEDED":
        return "timeout"
    message = str(exc).lower()
    if isinstance(exc, (ConnectionError, OSError, TimeoutError)) or any(
        marker in message
        for marker in (
            "connection refused",
            "connection reset",
            "provider offline",
            "rpc timeout",
            "transport unavailable",
        )
    ):
        return "unavailable"
    if (
        isinstance(exc, (AttributeError, NotImplementedError))
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
    reason_code = reason or (f"xtdata_{function}_{status}" if function else status)
    payload = {
        "status": status,
        "data": _numpy_to_python(data),
        "reason_code": reason_code,
        "message": reason_code,
        "capability": function or None,
        "provider": "bigqmt",
        "retryable": status in {"unavailable", "overloaded", "timeout"},
        "details": {},
    }
    if reason:
        payload["reason"] = reason
    if function:
        payload["function"] = function
    payload.update(extra)
    # ``reason`` can be a provider exception containing arbitrary request data.
    # Keep the public response unchanged, but log only a stable capability code.
    telemetry.emit("data.result", outcome=status,
                   reason_code=f"xtdata_{function}_{status}" if function else status,
                   capability=function or None, error_type=extra.get("error_type"),
                   retryable=payload["retryable"])
    return payload


def _is_failure_payload(value) -> bool:
    return isinstance(value, dict) and value.get("status") in {
        "unsupported",
        "unavailable",
        "error",
        "overloaded",
        "timeout",
    }


def _optional_result_payload(result, **success):
    if _is_failure_payload(result):
        return _numpy_to_python(result)
    payload = {"data": _numpy_to_python(result)}
    payload.update(success)
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


@telemetry.traced("data.convert.market")
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
            if not isinstance(df, pd.DataFrame):
                raise NativePayloadError(f"invalid native field {field}: {type(df).__name__}")
            if stock in df.index:
                for date, value in df.loc[stock].items():
                    entry = rows.setdefault(str(date), {"date": str(date)})
                    raw_value = value.item() if hasattr(value, "item") else value
                    entry[field] = _numpy_to_python(raw_value)
        result[stock] = [_numpy_to_python(row) for row in rows.values()]
    return result


@telemetry.traced("data.convert.bars")
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
        elif df is None or (isinstance(df, pd.DataFrame) and df.empty):
            result[stock] = []
        elif isinstance(df, list) and all(isinstance(row, dict) for row in df):
            result[stock] = [_numpy_to_python(row) for row in df]
        else:
            raise NativePayloadError(f"invalid native bars for {stock}: {type(df).__name__}")
    return result


@telemetry.traced("data.convert.financial")
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
                elif df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    stock_data[table_name] = []
                elif isinstance(df, list) and all(isinstance(row, dict) for row in df):
                    stock_data[table_name] = [_numpy_to_python(row) for row in df]
                else:
                    raise NativePayloadError(f"invalid native financial table {stock}/{table_name}")
        elif tables is not None:
            raise NativePayloadError(f"invalid native financial data for {stock}")
        result[stock] = stock_data
    return result
