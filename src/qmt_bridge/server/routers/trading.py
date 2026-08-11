"""Router — Trading endpoints /api/trading/* (requires API Key)."""

import logging

from fastapi import APIRouter, Depends, Request

from ..config import get_settings
from ..deps import get_trader_manager
from ..helpers import _numpy_to_python
from ..models import (
    AsyncCancelRequest,
    AsyncOrderRequest,
    CancelRequest,
    ExportDataRequest,
    OrderRequest,
    QueryAssetRequest,
    QueryOrderRequest,
    QueryPositionRequest,
    SyncTransactionRequest,
)
from ..security import require_api_key

router = APIRouter(prefix="/api/trading", tags=["trading"], dependencies=[Depends(require_api_key)])
logger = logging.getLogger("qmt_bridge.trading")


def _broker_order_receipt(value) -> str | None:
    normalized = str(value or "").strip()
    if normalized.lower() in {"", "-1", "0", "none", "null", "nan"}:
        return None
    return normalized


def _place_order_payload(req: OrderRequest, manager) -> dict:
    logger.info("QMT order submit request client_submit_id=%s", req.client_submit_id)
    try:
        result = manager.order(
            stock_code=req.stock_code,
            order_type=req.order_type,
            order_volume=req.order_volume,
            price_type=req.price_type,
            price=req.price,
            strategy_name=req.strategy_name,
            order_remark=req.client_submit_id,
            account_id=req.account_id,
        )
    except Exception:
        logger.warning(
            "QMT order response unknown client_submit_id=%s",
            req.client_submit_id,
            exc_info=True,
        )
        raise
    broker_order_id = _broker_order_receipt(result)
    if broker_order_id is None:
        payload = {
            "client_submit_id": req.client_submit_id,
            "order_remark": req.client_submit_id,
            "status": "submit_unknown",
            "reason": "broker_receipt_missing",
        }
    else:
        payload = {
            "client_submit_id": req.client_submit_id,
            "order_remark": req.client_submit_id,
            "broker_order_id": result,
            "order_id": result,
            "status": "submitted",
        }
    logger.info(
        "QMT order submit result client_submit_id=%s broker_order_id=%s status=%s",
        req.client_submit_id,
        broker_order_id,
        payload["status"],
    )
    return payload


def _manager_account_id(manager) -> str:
    """Return the configured funds account id from the runtime manager."""
    if manager is None:
        return ""
    for attr in ("account_id", "trading_account_id"):
        value = getattr(manager, attr, "")
        if value:
            return str(value)
    account = getattr(manager, "_account", None)
    for attr in ("account_id", "m_strAccountID", "accountID"):
        value = getattr(account, attr, "")
        if value:
            return str(value)
    return ""


@router.get("/health")
def trading_health(request: Request):
    """Return MeCoStock-compatible trading write readiness."""
    settings = getattr(request.app.state, "settings", None) or get_settings()
    manager = getattr(request.app.state, "trader_manager", None)
    account_config_enabled = bool(getattr(settings, "account_enabled", False))
    account_id = _manager_account_id(manager) or str(getattr(settings, "trading_account_id", "") or "")

    supports = {
        "order_stock": manager is not None,
        "cancel_order_stock": manager is not None,
        "/api/trading/order": manager is not None,
        "/api/trading/cancel": manager is not None,
    }
    payload = {
        "status": "ok" if manager is not None else "unavailable",
        "enabled": account_config_enabled and manager is not None,
        "authenticated": manager is not None,
        "account_authenticated": manager is not None and bool(account_id),
        "order_supported": manager is not None,
        "cancel_supported": manager is not None,
        "write_enabled": False,
        "write_blockers": [],
        "mode": "bigqmt_account_query_guarded_writes" if manager is not None else "bigqmt_account_unavailable",
        "account_id": account_id,
        "broker_account_id": account_id,
        "funds_account_id": account_id,
        "supports": supports,
    }
    if manager is None:
        payload["code"] = "QMT_TRADING_CONNECT_FAILED" if account_config_enabled else "QMT_TRADING_MODULE_DISABLED"
        payload["reason"] = (
            "Big QMT account manager is not connected."
            if account_config_enabled
            else "QMT bridge was started without account query routes enabled."
        )
        payload["write_blockers"] = (
            ["bigqmt_rpc_connect_failed"]
            if account_config_enabled
            else ["qmt_trading_module_disabled"]
        )
        return payload
    if not account_id:
        payload["code"] = "QMT_TRADING_ACCOUNT_MISSING"
        payload["reason"] = "QMT trading account id is not configured."
        payload["write_blockers"] = ["qmt_trading_account_missing"]
        return payload
    payload["write_enabled"] = bool(getattr(manager, "writes_enabled", False))
    payload["write_blockers"] = list(
        getattr(manager, "write_blockers", ["bridge_order_writes_disabled"])
    )
    runtime = getattr(manager, "runtime", None)
    if runtime is not None:
        payload["rpc_revision"] = runtime.ping_payload.get("rpc_revision")
        payload["qmt_trade_mode"] = str(
            runtime.ping_payload.get("qmt_trade_mode") or "unknown"
        )
        payload["terminal_real_mode"] = (
            runtime.ping_payload.get("terminal_real_mode") is True
        )
        payload["terminal_mode_source"] = runtime.ping_payload.get(
            "terminal_mode_source"
        )
        payload["upstream_order_writes_enabled"] = bool(
            runtime.ping_payload.get("allow_order_methods", False)
        )
    return payload


@router.post("/order")
def place_order(req: OrderRequest, manager=Depends(get_trader_manager)):
    """Place a new order."""
    return _place_order_payload(req, manager)


@router.post("/cancel")
def cancel_order(req: CancelRequest, manager=Depends(get_trader_manager)):
    """Cancel an existing order."""
    if req.order_sysid:
        result = manager.cancel_order_sysid(
            order_sysid=req.order_sysid,
            market=req.market,
            account_id=req.account_id,
        )
        return {
            "status": "ok",
            "data": _numpy_to_python(result),
            "cancel_method": "order_sysid",
            "order_sysid": req.order_sysid,
            "market": req.market,
        }
    result = manager.cancel_order(
        order_id=req.order_id,
        account_id=req.account_id,
    )
    return {"status": "ok", "data": _numpy_to_python(result), "cancel_method": "order_id"}


@router.get("/orders")
def query_orders(
    account_id: str = "",
    cancelable_only: bool = False,
    client_submit_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query current orders."""
    result = manager.query_orders(
        account_id=account_id,
        cancelable_only=cancelable_only,
        client_submit_id=client_submit_id,
    )
    if client_submit_id:
        logger.info(
            "QMT order reconcile query client_submit_id=%s match_count=%s",
            client_submit_id,
            len(result or []),
        )
    return {"client_submit_id": client_submit_id or None, "data": _numpy_to_python(result)}


@router.get("/positions")
def query_positions(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query current positions."""
    result = manager.query_positions(account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/asset")
def query_asset(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query account asset information."""
    result = manager.query_asset(account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/assets")
def query_assets(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query account asset information using MeCoStock's plural contract."""
    return query_asset(account_id=account_id, manager=manager)


@router.get("/trades")
def query_trades(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query trade records."""
    result = manager.query_trades(account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/order_detail")
def query_order_detail(
    order_id: int = 0,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query details for a specific order."""
    result = manager.query_order_detail(order_id=order_id, account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.post("/batch_order")
def batch_order(orders: list[OrderRequest], manager=Depends(get_trader_manager)):
    """Place multiple orders at once."""
    results = []
    for req in orders:
        try:
            results.append(_place_order_payload(req, manager))
        except Exception as exc:
            results.append(
                {
                    "stock_code": req.stock_code,
                    "client_submit_id": req.client_submit_id,
                    "order_remark": req.client_submit_id,
                    "status": "submit_unknown",
                    "reason": str(exc),
                }
            )
    statuses = {item["status"] for item in results}
    return {
        "status": "completed" if statuses == {"submitted"} else "partial",
        "results": results,
    }


@router.post("/batch_cancel")
def batch_cancel(cancel_requests: list[CancelRequest], manager=Depends(get_trader_manager)):
    """Cancel multiple orders at once."""
    results = []
    for req in cancel_requests:
        if req.order_sysid:
            result = manager.cancel_order_sysid(
                order_sysid=req.order_sysid,
                market=req.market,
                account_id=req.account_id,
            )
            results.append(
                {
                    "order_id": req.order_id,
                    "order_sysid": req.order_sysid,
                    "market": req.market,
                    "cancel_method": "order_sysid",
                    "result": _numpy_to_python(result),
                }
            )
        else:
            result = manager.cancel_order(order_id=req.order_id, account_id=req.account_id)
            results.append(
                {
                    "order_id": req.order_id,
                    "cancel_method": "order_id",
                    "result": _numpy_to_python(result),
                }
            )
    return {"data": results}


@router.get("/account_status")
def get_account_status(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Get trading account connection status."""
    result = manager.get_account_status(account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/account_info")
def get_account_info(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Get trading account basic information."""
    result = manager.get_account_info(account_id=account_id)
    return {"data": _numpy_to_python(result)}


# ------------------------------------------------------------------
# Async order/cancel
# ------------------------------------------------------------------


@router.post("/order_async")
def place_order_async(req: AsyncOrderRequest, manager=Depends(get_trader_manager)):
    """Place an order asynchronously (result via WebSocket callback)."""
    result = manager.order_async(
        stock_code=req.stock_code,
        order_type=req.order_type,
        order_volume=req.order_volume,
        price_type=req.price_type,
        price=req.price,
        strategy_name=req.strategy_name,
        order_remark=req.order_remark,
        account_id=req.account_id,
    )
    return {"seq": result, "status": "async_submitted"}


@router.post("/cancel_async")
def cancel_order_async(req: AsyncCancelRequest, manager=Depends(get_trader_manager)):
    """Cancel an order asynchronously (result via WebSocket callback)."""
    if req.order_sysid:
        result = manager.cancel_order_sysid_async(
            order_sysid=req.order_sysid,
            market=req.market,
            account_id=req.account_id,
        )
        return {
            "seq": result,
            "status": "async_submitted",
            "cancel_method": "order_sysid",
            "order_sysid": req.order_sysid,
            "market": req.market,
        }
    result = manager.cancel_order_async(
        order_id=req.order_id,
        account_id=req.account_id,
    )
    return {"seq": result, "status": "async_submitted", "cancel_method": "order_id"}


# ------------------------------------------------------------------
# Single-item queries
# ------------------------------------------------------------------


@router.get("/order/{order_id}")
def query_single_order(
    order_id: int,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query a single order by order_id."""
    result = manager.query_single_order(order_id=order_id, account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/trade/{trade_id}")
def query_single_trade(
    trade_id: int,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query a single trade by trade_id."""
    result = manager.query_single_trade(trade_id=trade_id, account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/position/{stock_code}")
def query_single_position(
    stock_code: str,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query position for a single stock."""
    result = manager.query_single_position(stock_code=stock_code, account_id=account_id)
    return {"data": _numpy_to_python(result)}


# ------------------------------------------------------------------
# Position statistics
# ------------------------------------------------------------------


@router.get("/position_statistics")
def query_position_statistics(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query position statistics summary."""
    result = manager.query_position_statistics(account_id=account_id)
    return {"data": _numpy_to_python(result)}


# ------------------------------------------------------------------
# IPO queries
# ------------------------------------------------------------------


@router.get("/new_purchase_limit")
def query_new_purchase_limit(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query IPO new purchase limit."""
    result = manager.query_new_purchase_limit(account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/ipo_data")
def query_ipo_data(manager=Depends(get_trader_manager)):
    """Query IPO calendar data."""
    result = manager.query_ipo_data()
    return {"data": _numpy_to_python(result)}


# ------------------------------------------------------------------
# Account infos (all accounts)
# ------------------------------------------------------------------


@router.get("/account_infos")
def query_account_infos(manager=Depends(get_trader_manager)):
    """Query info for all registered trading accounts."""
    result = manager.query_account_infos()
    return {"data": _numpy_to_python(result)}


# ------------------------------------------------------------------
# COM queries (期权/期货)
# ------------------------------------------------------------------


@router.get("/com_fund")
def query_com_fund(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query COM fund (option/future account funds)."""
    result = manager.query_com_fund(account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/com_position")
def query_com_position(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query COM positions (option/future account positions)."""
    result = manager.query_com_position(account_id=account_id)
    return {"data": _numpy_to_python(result)}


# ------------------------------------------------------------------
# Data export / external sync
# ------------------------------------------------------------------


@router.post("/export_data")
def export_data(req: ExportDataRequest, manager=Depends(get_trader_manager)):
    """Export trading data to file."""
    result = manager.export_data(
        data_type=req.data_type,
        file_path=req.file_path,
        account_id=req.account_id,
    )
    return {"status": "ok", "data": _numpy_to_python(result)}


@router.get("/query_data")
def query_data(
    data_type: str = "orders",
    result_path: str = "",
    start_time: str | None = None,
    end_time: str | None = None,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query exported trading data."""
    result = manager.query_data(
        data_type=data_type,
        result_path=result_path,
        start_time=start_time,
        end_time=end_time,
        account_id=account_id,
    )
    return {"data": _numpy_to_python(result)}


@router.post("/sync_transaction")
def sync_transaction(req: SyncTransactionRequest, manager=Depends(get_trader_manager)):
    """Sync external transaction records into the system."""
    result = manager.sync_transaction_from_external(
        data=req.data,
        account_id=req.account_id,
    )
    return {"status": "ok", "data": _numpy_to_python(result)}
