"""Router — ETF & convertible bond endpoints /api/etf/*, /api/fund/* and /api/cb/*."""

from fastapi import APIRouter, Query
from ..bigqmt import xtdata

from ..helpers import (
    _call_xtdata_optional,
    _call_xtdata_serialized,
    _numpy_to_python,
)

etf_router = APIRouter(prefix="/api/etf", tags=["etf"])
fund_data_router = APIRouter(prefix="/api/fund", tags=["fund-data"])
cb_router = APIRouter(prefix="/api/cb", tags=["cb"])


@etf_router.get("/list")
def get_etf_list():
    stock_list = _call_xtdata_serialized(
        xtdata.get_stock_list_in_sector,
        "沪深ETF",
    )
    return {"count": len(stock_list), "stocks": stock_list}


@etf_router.get("/info")
def get_etf_info():
    raw = _call_xtdata_serialized(xtdata.get_etf_info)
    return {"data": _numpy_to_python(raw)}


@fund_data_router.get("/etf-info")
def get_fund_etf_info(
    stocks: str = Query("", description="ETF 代码列表，逗号分隔"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()] if stocks else []
    payload = _call_xtdata_optional(xtdata, "get_etf_info")
    if payload["status"] != "ok":
        return payload
    data = payload.get("data")
    if stock_list and isinstance(data, dict):
        data = {stock: data.get(stock) for stock in stock_list if stock in data}
    return {
        "status": "ok",
        "data": data,
        "unit_contract": {"shares": "not_provided_by_qmt_etf_info"},
    }


@fund_data_router.get("/iopv")
def get_fund_iopv(
    stocks: str = Query(..., description="ETF 代码列表，逗号分隔"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    data = {}
    status = "ok"
    reason = ""
    for stock in stock_list:
        payload = _call_xtdata_optional(xtdata, "get_etf_iopv", stock)
        data[stock] = payload.get("data")
        if payload["status"] != "ok":
            data[stock] = payload
            status = payload["status"]
            reason = payload.get("reason", reason)
    return {
        "status": status,
        "reason": reason,
        "data": data,
        "unit_contract": {
            "iopv": "yuan_per_share_request_time",
            "shares": "not_provided_by_qmt_iopv",
        },
    }


@cb_router.get("/info")
def get_cb_info(
    stock: str = Query(..., description="可转债代码"),
):
    raw = _call_xtdata_serialized(xtdata.get_cb_info, stock)
    return {"stock": stock, "data": _numpy_to_python(raw)}


# ---------------------------------------------------------------------------
# New convertible bond endpoints (Step 5)
# ---------------------------------------------------------------------------


@cb_router.get("/list")
def get_cb_list():
    """Get all convertible bond codes."""
    stock_list = _call_xtdata_serialized(
        xtdata.get_stock_list_in_sector,
        "沪深转债",
    )
    return {"count": len(stock_list), "stocks": stock_list}


@cb_router.get("/detail")
def get_cb_detail(
    stock: str = Query(..., description="可转债代码"),
):
    """Get detailed convertible bond information."""
    raw = _call_xtdata_serialized(xtdata.get_cb_info, stock)
    return {"stock": stock, "data": _numpy_to_python(raw)}


@cb_router.get("/conversion_price")
def get_cb_conversion_price(
    stock: str = Query(..., description="可转债代码"),
):
    """Get convertible bond conversion price info."""
    raw = _call_xtdata_serialized(xtdata.get_cb_info, stock)
    data = _numpy_to_python(raw)
    return {"stock": stock, "data": data}


@cb_router.get("/bond_info")
def get_bond_info(
    stock: str = Query(..., description="可转债代码"),
):
    """Get bond-specific information for a convertible bond."""
    raw = _call_xtdata_serialized(xtdata.get_cb_info, stock)
    return {"stock": stock, "data": _numpy_to_python(raw)}
