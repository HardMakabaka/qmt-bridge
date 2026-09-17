"""Router — System metadata endpoints /api/meta/*."""

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from ..bigqmt import BIGQMT_UPSTREAM_SHA, BIGQMT_UPSTREAM_VERSION, market_data
from ..binary_cache import get_binary_cache
from ..capabilities import build_capability_registry, capability_summary
from ..helpers import _call_xtdata_serialized, _numpy_to_python, get_xtdata_transport_status
from ..history_readiness import ProbeState, probe, validate_probe
from ..security import optional_api_key
from time import monotonic
from datetime import datetime, timezone
from bigqmt_signal_trader import telemetry

router = APIRouter(prefix="/api/meta", tags=["meta"])


def _history_readiness_state(request: Request) -> ProbeState:
    state = getattr(request.app.state, "history_readiness_state", None)
    if state is None:
        state = ProbeState()
        request.app.state.history_readiness_state = state
    return state


def _active_write_counter(request: Request) -> tuple[int | None, str]:
    lock = getattr(request.app.state, "active_write_requests_lock", None)
    if lock is None:
        return None, "unknown"
    try:
        with lock:
            value = getattr(request.app.state, "active_write_requests", None)
    except Exception:
        return None, "unknown"
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None, "unknown"
    return value, "ok"


@router.get("/capabilities")
def get_capabilities(request: Request):
    runtime = getattr(request.app.state, "bigqmt_runtime", None)
    registry = build_capability_registry(request.app, runtime)
    return {
        "status": "ok",
        "counts": capability_summary(registry),
        "capabilities": [item.to_payload() for item in registry],
    }


@router.get("/markets")
def get_markets():
    raw = _call_xtdata_serialized(market_data.get_markets)
    return {"markets": _numpy_to_python(raw)}


@router.get("/periods")
def get_periods():
    raw = _call_xtdata_serialized(market_data.get_period_list)
    return {"periods": _numpy_to_python(raw)}


@router.get("/stock_list")
def get_stock_list(
    category: str = Query(
        ...,
        description="证券类别，如 沪深A股 / 上证A股 / 深证A股 / 北证A股 / 沪深ETF / 沪深指数",
    ),
):
    stock_list = _call_xtdata_serialized(
        market_data.get_stock_list_in_sector,
        category,
    )
    return {"category": category, "count": len(stock_list), "stocks": stock_list}


@router.get("/last_trade_date")
def get_last_trade_date(
    market: str = Query(..., description="市场代码，如 SH / SZ"),
):
    date = _call_xtdata_serialized(market_data.get_market_last_trade_date, market)
    return {"market": market, "last_trade_date": date}


# ---------------------------------------------------------------------------
# New status monitoring endpoints (Step 5)
# ---------------------------------------------------------------------------


@router.get("/version")
def get_server_version():
    """Get QMT Bridge server version."""
    from ..._version import __version__

    return {"version": __version__}


@router.get("/runtime_version")
def get_runtime_version():
    return {
        "runtime_version": BIGQMT_UPSTREAM_VERSION,
        "runtime": "bigqmt",
        "upstream_sha": BIGQMT_UPSTREAM_SHA,
    }


@router.get("/connection_status")
def get_connection_status(request: Request):
    controller = getattr(request.app.state, "runtime_controller", None)
    runtime = getattr(request.app.state, "bigqmt_runtime", None)
    if runtime is None:
        return {
            "connected": False,
            "error": getattr(request.app.state, "bigqmt_runtime_error", None),
            "recovery": controller.status() if controller is not None else None,
        }
    try:
        runtime.probe()
        return {"connected": True, **runtime.readiness()}
    except Exception as e:
        if controller is not None:
            controller.mark_transport_unavailable(e)
        return {"connected": False, "error": str(e)}


@router.get("/health")
def health_check(request: Request):
    """Simple health check endpoint."""
    return {
        "status": "ok",
        "runtime": "bigqmt",
        "runtime_initialized": getattr(
            request.app.state, "bigqmt_runtime", None
        )
        is not None,
        "telemetry": {key: value for key, value in telemetry.stats().items() if key in {
            "enabled", "writer_alive", "queue_depth", "dropped", "write_failures",
            "critical_dropped", "critical_write_failures", "loss_events",
        }},
    }


@router.get("/history-readiness")
@telemetry.traced("probe.history", source="bigqmt_zmq_rpc", cache_used=False)
def history_readiness(
    request: Request,
    stock: str = Query(...),
    start_time: str = Query(...),
    end_time: str = Query(...),
    timeout_seconds: float = Query(8),
    _api_key: str | None = Depends(optional_api_key),
):
    state = _history_readiness_state(request)
    try:
        stock, start_time, end_time, timeout_seconds, expected = validate_probe(
            stock, start_time, end_time, timeout_seconds
        )
    except ValueError as exc:
        telemetry.emit("probe.history.result", outcome="rejected", failure_kind="configuration", row_count=0)
        fence = state.begin((str(stock), str(start_time), str(end_time)))
        last_success_at, consecutive_failures, _applied = state.update(
            fence, "unavailable", "invalid", "configuration"
        )
        active_write_requests, _counter_status = _active_write_counter(request)
        return {
            "schema_version": "history_readiness_v1", "status": "unavailable",
            "rpc_status": "unavailable", "data_status": "invalid", "source": "bigqmt_zmq_rpc",
            "failure_kind": "configuration",
            "cache_used": False, "row_count": 0, "latency_ms": 0, "quality_flags": [],
            "checked_at": datetime.now(timezone.utc).isoformat(), "last_success_at": last_success_at,
            "consecutive_failures": consecutive_failures, "error": {"code": str(exc), "message": "Invalid probe request."},
            "probe": {"stock": stock, "start_time": start_time, "end_time": end_time, "period": "1m", "count": 0, "dividend_type": "none", "fill_data": False, "subscribe": False},
            "active_write_requests": active_write_requests,
        }
    fence = state.begin((stock, start_time, end_time))
    started = monotonic()
    runtime = getattr(request.app.state, "bigqmt_runtime", None)
    transport = get_xtdata_transport_status()
    if transport["status"] == "blocked":
        rpc_status, data_status, row_count, actual = "connection_error", "empty", 0, []
        error_code, quality_flags, failure_kind = "xtdata_transport_stuck", [], "transport"
    else:
        rpc_status, data_status, row_count, expected, actual, error_code, quality_flags, failure_kind = probe(
            runtime, stock=stock, start_time=start_time, end_time=end_time, timeout_seconds=timeout_seconds
        )
        transport = get_xtdata_transport_status()
        if transport["status"] == "blocked":
            rpc_status, error_code, failure_kind = "connection_error", "xtdata_transport_stuck", "transport"
    if rpc_status == "ok":
        failure_kind = "none" if data_status == "complete" else "data"
    last_success_at, consecutive_failures, _applied = state.update(
        fence, rpc_status, data_status, failure_kind
    )
    status = "healthy" if rpc_status == "ok" and data_status == "complete" else (
        "data_gap" if rpc_status == "ok" else "unavailable"
    )
    active_write_requests, _counter_status = _active_write_counter(request)
    error = None if error_code is None else {"code": error_code, "message": "History readiness probe did not complete."}
    telemetry.emit("probe.history.result", outcome=status, rpc_status=rpc_status, data_status=data_status,
                   failure_kind=failure_kind, row_count=row_count, expected_count=len(expected),
                   actual_count=len(actual), state_update_applied=_applied)
    return {
        "schema_version": "history_readiness_v1", "status": status, "rpc_status": rpc_status,
        "data_status": data_status, "source": "bigqmt_zmq_rpc", "cache_used": False,
        "failure_kind": failure_kind,
        "download_transport": transport,
        "row_count": row_count, "latency_ms": round((monotonic() - started) * 1000, 2),
        "quality_flags": quality_flags,
        "checked_at": datetime.now(timezone.utc).isoformat(), "last_success_at": last_success_at,
        "consecutive_failures": consecutive_failures, "error": error,
        "probe": {"stock": stock, "start_time": start_time, "end_time": end_time, "period": "1m", "count": len(expected), "dividend_type": "none", "fill_data": False, "subscribe": False, "expected_minutes": expected, "actual_minutes": actual},
        "active_write_requests": active_write_requests,
    }


@router.get("/recovery-status")
def recovery_status(request: Request, _api_key: str | None = Depends(optional_api_key)):
    active, counter_status = _active_write_counter(request)
    return {
        "schema_version": "recovery_status_v1", "active_write_requests": active,
        "counter_status": counter_status, "restart_safe": counter_status == "ok" and active == 0,
        "download_transport": get_xtdata_transport_status(),
        "download_jobs_error": getattr(request.app.state, "download_jobs_error", None),
        "initialized": getattr(request.app.state, "bigqmt_runtime", None) is not None,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/readiness")
def readiness_check(request: Request):
    controller = getattr(request.app.state, "runtime_controller", None)
    runtime = getattr(request.app.state, "bigqmt_runtime", None)
    if runtime is None:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unavailable",
                "ready": False,
                "runtime": "bigqmt",
                "error": getattr(
                    request.app.state, "bigqmt_runtime_error", None
                ),
                "recovery": controller.status() if controller is not None else None,
            },
        )
    try:
        runtime.probe()
    except Exception as exc:
        if controller is not None:
            controller.mark_transport_unavailable(exc)
        return JSONResponse(
            status_code=503,
            content={
                "status": "unavailable",
                "ready": False,
                **runtime.readiness(),
                "error": f"{exc.__class__.__name__}: {exc}",
            },
        )
    manager = getattr(request.app.state, "trader_manager", None)
    payload = {
        "status": "ready",
        **runtime.readiness(),
        "account_queries_enabled": manager is not None,
        "order_writes_enabled": bool(
            getattr(manager, "writes_enabled", False)
        ),
        "write_blockers": list(
            getattr(manager, "write_blockers", ["account_manager_unavailable"])
        ),
        "execution_events": getattr(
            manager,
            "event_status",
            {
                "transport": "zmq",
                "listener_alive": False,
                "replay_gap": False,
                "cursor": None,
            },
        ),
    }
    registry = build_capability_registry(request.app, runtime)
    payload["capability_counts"] = capability_summary(registry)
    return payload


@router.get("/binary_cache")
def get_binary_cache_stats():
    """Return local binary cache configuration and size counters."""
    return get_binary_cache().stats()


@router.get("/quote_server_status")
def get_quote_server_status():
    """Get detailed quote server connection status."""
    try:
        status = _call_xtdata_serialized(market_data.get_markets)
        return {"status": "ok", "data": _numpy_to_python(status)}
    except Exception as e:
        return {"error": str(e)}
