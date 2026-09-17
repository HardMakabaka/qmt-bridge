"""Router — Trading endpoints /api/trading/* (requires API Key)."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from bigqmt_signal_trader.telemetry import emit, span

from ..config import get_settings
from ..deps import get_trader_manager
from ..helpers import (
    _is_failure_payload,
    _numpy_to_python,
    _optional_result_payload,
)
from ..models import (
    CancelRequest,
    OrderRequest,
    QueryAssetRequest,
    QueryOrderRequest,
    QueryPositionRequest,
    SyncTransactionRequest,
)
from ..security import require_api_key

router = APIRouter(prefix="/api/trading", tags=["trading"], dependencies=[Depends(require_api_key)])
logger = logging.getLogger("qmt_bridge.trading")
CANCEL_ACKNOWLEDGED_STATUSES = {51, 52, 53, 54}
ORDER_TERMINAL_NOT_CANCELABLE_STATUSES = {56, 57}


def _broker_order_receipt(value) -> str | None:
    normalized = str(value or "").strip()
    if normalized.lower() in {"", "-1", "0", "none", "null", "nan"}:
        return None
    return normalized


def _cancel_receipt_missing(value) -> bool:
    if isinstance(value, bool):
        return not value
    if value is None or value in (-1, "", "-1"):
        return True
    return False


def _order_value(order, *names):
    for name in names:
        if isinstance(order, dict) and name in order:
            return order[name]
        value = getattr(order, name, None)
        if value not in (None, ""):
            return value
    return None


def _query_by_client_submit_id(req: OrderRequest, manager):
    query = getattr(manager, "query_orders", None)
    if not callable(query):
        return None
    emit("trading.http.identity_precheck", critical=True, account_id=req.account_id,
         client_submit_id=req.client_submit_id, outcome="started")
    result = query(
        account_id=req.account_id,
        cancelable_only=False,
        client_submit_id=req.client_submit_id,
    ) or []
    emit("trading.http.identity_precheck", critical=True, account_id=req.account_id,
         client_submit_id=req.client_submit_id, outcome="empty" if not result else "success",
         match_count=len(result))
    return result


def _reconciled_order_payload(req: OrderRequest, orders, *, idempotent: bool):
    if not orders:
        return None
    requested_terms = {
        "stock_code": req.stock_code,
        "order_type": req.order_type,
        "order_volume": req.order_volume,
        "price": req.price,
        "strategy_name": req.strategy_name,
    }
    conflicts = []
    for order in orders:
        known_terms = {
            "stock_code": _order_value(order, "stock_code"),
            "order_type": _order_value(order, "order_type"),
            "order_volume": _order_value(order, "order_volume", "volume"),
            "price": _order_value(order, "price", "order_price"),
            "strategy_name": _order_value(order, "strategy_name"),
        }
        mismatched = []
        for name, known_value in known_terms.items():
            if name == "price" and known_value not in (None, ""):
                known_price = float(known_value)
                requested_price = float(requested_terms[name])
                differs = (
                    known_price > 0
                    and requested_price > 0
                    and abs(known_price - requested_price) > 1e-8
                )
            else:
                differs = (
                    known_value not in (None, "")
                    and str(known_value).upper() != str(requested_terms[name]).upper()
                )
            if differs:
                mismatched.append(name)
        order_id = _order_value(order, "order_id", "order_sysid", "order_sys_id")
        if not mismatched and _broker_order_receipt(order_id) is not None:
            return {
                "client_submit_id": req.client_submit_id,
                "order_remark": req.client_submit_id,
                "broker_order_id": order_id,
                "order_id": order_id,
                "status": "submitted",
                "reconciled": True,
                "idempotent": idempotent,
            }
        conflicts.extend(mismatched)
    if conflicts:
        emit("trading.http.identity_conflict", critical=True, account_id=req.account_id,
             client_submit_id=req.client_submit_id, outcome="rejected",
             mismatched_fields=sorted(set(conflicts)))
        raise HTTPException(
            status_code=409,
            detail={
                "code": "CLIENT_SUBMIT_ID_CONFLICT",
                "client_submit_id": req.client_submit_id,
                "mismatched_fields": sorted(set(conflicts)),
            },
        )
    return None


def _place_order_payload(req: OrderRequest, manager) -> dict:
    logger.info("QMT order submit request client_submit_id=%s", req.client_submit_id)
    try:
        existing = _query_by_client_submit_id(req, manager)
    except (ConnectionError, OSError, TimeoutError) as exc:
        emit("trading.http.identity_precheck", critical=True, account_id=req.account_id,
             client_submit_id=req.client_submit_id, outcome="unknown", error_type=type(exc).__name__)
        return {
            "client_submit_id": req.client_submit_id,
            "order_remark": req.client_submit_id,
            "status": "not_submitted",
            "reason": "idempotency_check_unavailable",
            "error_type": exc.__class__.__name__,
        }
    existing_payload = _reconciled_order_payload(req, existing, idempotent=True)
    if existing_payload is not None:
        emit("trading.http.submit.idempotent", critical=True, account_id=req.account_id,
             client_submit_id=req.client_submit_id, order_sys_id=existing_payload.get("order_id"), outcome="success")
        return existing_payload
    try:
        with span("trading.http.submit", account_id=req.account_id,
                  client_submit_id=req.client_submit_id, stock_code=req.stock_code,
                  order_type=req.order_type, volume=req.order_volume, price=req.price) as trace:
            result = manager.order(
                stock_code=req.stock_code, order_type=req.order_type,
                order_volume=req.order_volume, price_type=req.price_type, price=req.price,
                strategy_name=req.strategy_name, order_remark=req.client_submit_id,
                account_id=req.account_id,
            )
            trace["outcome"] = "success"
    except (ConnectionError, OSError, TimeoutError) as exc:
        emit("trading.http.submit.return", critical=True, account_id=req.account_id,
             client_submit_id=req.client_submit_id, outcome="unknown", error_type=type(exc).__name__)
        logger.warning(
            "QMT order response unknown client_submit_id=%s",
            req.client_submit_id,
            exc_info=True,
        )
        try:
            recovered_orders = _query_by_client_submit_id(req, manager)
        except (ConnectionError, OSError, TimeoutError):
            recovered_orders = None
        reconciled = _reconciled_order_payload(
            req,
            recovered_orders,
            idempotent=False,
        )
        if reconciled is not None:
            emit("trading.http.strict_reconcile", critical=True, account_id=req.account_id,
                 client_submit_id=req.client_submit_id, order_sys_id=reconciled.get("order_id"), outcome="success")
            return reconciled
        return {
            "client_submit_id": req.client_submit_id,
            "order_remark": req.client_submit_id,
            "status": "submit_unknown",
            "reason": "transport_error_unreconciled",
            "error_type": exc.__class__.__name__,
        }
    broker_order_id = _broker_order_receipt(result)
    if broker_order_id is None:
        try:
            recovered_orders = _query_by_client_submit_id(req, manager)
        except (ConnectionError, OSError, TimeoutError):
            recovered_orders = None
        payload = _reconciled_order_payload(
            req,
            recovered_orders,
            idempotent=False,
        )
        if payload is None:
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
    emit("trading.http.submit.receipt", critical=True, account_id=req.account_id,
         client_submit_id=req.client_submit_id, order_sys_id=broker_order_id,
         outcome="success" if payload["status"] == "submitted" else "unknown",
         status=payload["status"])
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
        "submit_order": manager is not None,
        "cancel_order": manager is not None,
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
    def query_current_order():
        if req.order_id:
            query_detail = getattr(manager, "query_order_detail", None)
            if callable(query_detail):
                return query_detail(order_id=req.order_id, account_id=req.account_id)
            return None
        query_orders = getattr(manager, "query_orders", None)
        if callable(query_orders):
            orders = query_orders(
                account_id=req.account_id,
                cancelable_only=False,
                client_submit_id="",
            )
            for order in orders or []:
                order_sysid = _order_value(order, "order_sysid", "order_sys_id", "order_id")
                if str(order_sysid or "") == req.order_sysid:
                    return order
        return None

    cancel_method = "order_sysid" if req.order_sysid else "order_id"
    emit("trading.http.cancel.precheck", critical=True, account_id=req.account_id,
         order_sys_id=req.order_sysid or req.order_id, cancel_method=cancel_method,
         outcome="started")
    try:
        before = query_current_order()
    except (ConnectionError, OSError, TimeoutError):
        logger.warning(
            "QMT cancel preflight query failed order_id=%s order_sysid=%s",
            req.order_id,
            req.order_sysid,
        )
        before = None
    before_status = _order_value(before, "order_status", "status")
    if before_status is not None and int(before_status) in CANCEL_ACKNOWLEDGED_STATUSES:
        emit("trading.http.cancel.terminal", critical=True, account_id=req.account_id,
             order_sys_id=req.order_sysid or req.order_id, cancel_method=cancel_method,
             order_status=int(before_status), outcome="success")
        return {
            "status": "ok",
            "data": _numpy_to_python(before),
            "cancel_method": cancel_method,
            "order_status": int(before_status),
            "idempotent": True,
            "reconciled": True,
        }
    if before_status is not None and int(before_status) in ORDER_TERMINAL_NOT_CANCELABLE_STATUSES:
        emit("trading.http.cancel.terminal", critical=True, account_id=req.account_id,
             order_sys_id=req.order_sysid or req.order_id, cancel_method=cancel_method,
             order_status=int(before_status), outcome="rejected")
        return {
            "status": "not_cancelable",
            "data": _numpy_to_python(before),
            "cancel_method": cancel_method,
            "order_status": int(before_status),
            "idempotent": True,
            "reconciled": True,
        }
    try:
        if req.order_sysid:
            result = manager.cancel_order_sysid(
                order_sysid=req.order_sysid,
                market=req.market,
                account_id=req.account_id,
            )
        else:
            result = manager.cancel_order(
                order_id=req.order_id,
                account_id=req.account_id,
            )
    except (ConnectionError, OSError, TimeoutError) as exc:
        emit("trading.http.cancel.return", critical=True, account_id=req.account_id,
             order_sys_id=req.order_sysid or req.order_id, cancel_method=cancel_method,
             outcome="unknown", error_type=type(exc).__name__)
        try:
            after = query_current_order()
        except (ConnectionError, OSError, TimeoutError):
            after = None
        after_status = _order_value(after, "order_status", "status")
        if after_status is not None and int(after_status) in CANCEL_ACKNOWLEDGED_STATUSES:
            emit("trading.http.cancel.strict_reconcile", critical=True, account_id=req.account_id,
                 order_sys_id=req.order_sysid or req.order_id, outcome="success")
            return {
                "status": "ok",
                "data": _numpy_to_python(after),
                "cancel_method": cancel_method,
                "order_status": int(after_status),
                "idempotent": False,
                "reconciled": True,
            }
        return {
            "status": "cancel_unknown",
            "cancel_method": cancel_method,
            "order_id": req.order_id,
            "order_sysid": req.order_sysid or None,
            "reason": "transport_error_unreconciled",
            "error_type": exc.__class__.__name__,
        }
    if _cancel_receipt_missing(result):
        try:
            after = query_current_order()
        except (ConnectionError, OSError, TimeoutError):
            after = None
        after_status = _order_value(after, "order_status", "status")
        if after_status is not None and int(after_status) in CANCEL_ACKNOWLEDGED_STATUSES:
            emit("trading.http.cancel.strict_reconcile", critical=True, account_id=req.account_id,
                 order_sys_id=req.order_sysid or req.order_id, outcome="success")
            return {
                "status": "ok",
                "data": _numpy_to_python(after),
                "cancel_method": cancel_method,
                "order_status": int(after_status),
                "idempotent": False,
                "reconciled": True,
            }
        return {
            "status": "cancel_unknown",
            "cancel_method": cancel_method,
            "order_id": req.order_id,
            "order_sysid": req.order_sysid or None,
            "market": req.market if req.order_sysid else None,
            "reason": "broker_receipt_missing",
        }
    payload = {
        "status": "ok",
        "data": _numpy_to_python(result),
        "cancel_method": cancel_method,
    }
    if req.order_sysid:
        payload["order_sysid"] = req.order_sysid
        payload["market"] = req.market
    emit("trading.http.cancel.receipt", critical=True, account_id=req.account_id,
         order_sys_id=req.order_sysid or req.order_id, cancel_method=cancel_method, outcome="success")
    return payload


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
    for item_index, req in enumerate(orders):
        try:
            item = _place_order_payload(req, manager)
            results.append(item)
            emit("trading.http.batch_item", critical=True, item_index=item_index,
                 account_id=req.account_id, client_submit_id=req.client_submit_id,
                 outcome="success" if item.get("status") == "submitted" else "unknown",
                 status=item.get("status"))
        except Exception as exc:
            emit("trading.http.batch_item", critical=True, item_index=item_index,
                 account_id=req.account_id, client_submit_id=req.client_submit_id,
                 outcome="unknown", error_type=type(exc).__name__)
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
    result = manager.query_extension("query_account_status", account_id=account_id)
    return {"data": _numpy_to_python(result)}


@router.get("/account_info")
def get_account_info(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Get trading account basic information."""
    result = manager.query_extension("query_account_infos", account_id=account_id)
    return {"data": _numpy_to_python(result)}


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
    result = manager.query_order_detail(order_id=order_id, account_id=account_id)
    return _optional_result_payload(result)


@router.get("/trade/{trade_id}")
def query_single_trade(
    trade_id: int,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query a single trade by trade_id."""
    result = manager.query_trade(trade_id=trade_id, account_id=account_id)
    return _optional_result_payload(result)


@router.get("/position/{stock_code}")
def query_single_position(
    stock_code: str,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query position for a single stock."""
    result = manager.query_position(stock_code=stock_code, account_id=account_id)
    return _optional_result_payload(result)


# ------------------------------------------------------------------
# Position statistics
# ------------------------------------------------------------------


@router.get("/position_statistics")
def query_position_statistics(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query position statistics summary."""
    result = manager.query_extension("query_position_statistics", account_id=account_id)
    return _optional_result_payload(result)


# ------------------------------------------------------------------
# IPO queries
# ------------------------------------------------------------------


@router.get("/new_purchase_limit")
def query_new_purchase_limit(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query IPO new purchase limit."""
    result = manager.query_extension("get_new_purchase_limit", account_id=account_id)
    return _optional_result_payload(result)


@router.get("/ipo_data")
def query_ipo_data(manager=Depends(get_trader_manager)):
    """Query IPO calendar data."""
    result = manager.query_extension("get_ipo_data")
    return _optional_result_payload(result)


# ------------------------------------------------------------------
# Account infos (all accounts)
# ------------------------------------------------------------------


@router.get("/account_infos")
def query_account_infos(manager=Depends(get_trader_manager)):
    """Query info for all registered trading accounts."""
    result = manager.query_extension("query_account_infos")
    return _optional_result_payload(result)


@router.post("/sync_transaction")
def sync_transaction(req: SyncTransactionRequest, manager=Depends(get_trader_manager)):
    """Sync external transaction records into the system."""
    result = manager.sync_transaction_from_external(
        operation=req.operation,
        data_type=req.data_type,
        data=req.data,
        account_type=req.account_type,
        account_id=req.account_id,
    )
    return _optional_result_payload(result, status="ok")
