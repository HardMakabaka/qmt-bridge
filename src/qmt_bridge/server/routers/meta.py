"""Router — System metadata endpoints /api/meta/*."""

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from ..bigqmt import BIGQMT_UPSTREAM_SHA, BIGQMT_UPSTREAM_VERSION, xtdata
from ..binary_cache import get_binary_cache
from ..capabilities import build_capability_registry, capability_summary
from ..helpers import _call_xtdata_serialized, _numpy_to_python

router = APIRouter(prefix="/api/meta", tags=["meta"])


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
    raw = _call_xtdata_serialized(xtdata.get_markets)
    return {"markets": _numpy_to_python(raw)}


@router.get("/periods")
def get_periods():
    raw = _call_xtdata_serialized(xtdata.get_period_list)
    return {"periods": _numpy_to_python(raw)}


@router.get("/stock_list")
def get_stock_list(
    category: str = Query(
        ...,
        description="证券类别，如 沪深A股 / 上证A股 / 深证A股 / 北证A股 / 沪深ETF / 沪深指数",
    ),
):
    stock_list = _call_xtdata_serialized(
        xtdata.get_stock_list_in_sector,
        category,
    )
    return {"category": category, "count": len(stock_list), "stocks": stock_list}


@router.get("/last_trade_date")
def get_last_trade_date(
    market: str = Query(..., description="市场代码，如 SH / SZ"),
):
    date = _call_xtdata_serialized(xtdata.get_market_last_trade_date, market)
    return {"market": market, "last_trade_date": date}


# ---------------------------------------------------------------------------
# New status monitoring endpoints (Step 5)
# ---------------------------------------------------------------------------


@router.get("/version")
def get_server_version():
    """Get QMT Bridge server version."""
    from ..._version import __version__

    return {"version": __version__}


@router.get("/xtdata_version")
def get_xtdata_version():
    return {
        "xtdata_version": BIGQMT_UPSTREAM_VERSION,
        "runtime": "bigqmt",
        "upstream_sha": BIGQMT_UPSTREAM_SHA,
    }


@router.get("/connection_status")
def get_connection_status(request: Request):
    runtime = getattr(request.app.state, "bigqmt_runtime", None)
    if runtime is None:
        return {
            "connected": False,
            "error": getattr(request.app.state, "bigqmt_runtime_error", None),
        }
    try:
        runtime.probe()
        return {"connected": True, **runtime.readiness()}
    except Exception as e:
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
    }


@router.get("/readiness")
def readiness_check(request: Request):
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
            },
        )
    try:
        runtime.probe()
    except Exception as exc:
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
        status = _call_xtdata_serialized(xtdata.get_markets)
        return {"status": "ok", "data": _numpy_to_python(status)}
    except Exception as e:
        return {"error": str(e)}
