"""Router — Hong Kong market endpoints /api/hk/*."""

from fastapi import APIRouter, Query
from ..bigqmt import market_data

from ..helpers import (
    _call_xtdata_optional,
    _call_xtdata_serialized,
)

router = APIRouter(prefix="/api/hk", tags=["hk"])


@router.get("/stock_list")
def get_hk_stock_list():
    """Get list of HK-connected stocks (港股通)."""
    stock_list = _call_xtdata_serialized(
        market_data.get_stock_list_in_sector,
        "沪港通",
    ) + _call_xtdata_serialized(
        market_data.get_stock_list_in_sector,
        "深港通",
    )
    return {"count": len(stock_list), "stocks": stock_list}


@router.get("/connect_stocks")
def get_hk_connect_stocks(
    connect_type: str = Query("north", description="通道类型: north(北向)/south(南向)"),
):
    """Get HK-connect stock list by direction."""
    if connect_type == "south":
        stock_list = _call_xtdata_serialized(
            market_data.get_stock_list_in_sector,
            "港股通",
        )
    else:
        stock_list = _call_xtdata_serialized(
            market_data.get_stock_list_in_sector,
            "沪股通",
        ) + _call_xtdata_serialized(
            market_data.get_stock_list_in_sector,
            "深股通",
        )
    return {"connect_type": connect_type, "count": len(stock_list), "stocks": stock_list}


@router.get("/north_finance_change")
def get_north_finance_change(
    period: str = Query("1d", description="周期"),
):
    return _call_xtdata_optional(market_data, "get_north_finance_change", period)


@router.get("/statistics")
def get_hkt_statistics(
    stock: str = Query(..., description="港股通标的代码"),
):
    return _call_xtdata_optional(market_data, "get_hkt_statistics", stock)


@router.get("/details")
def get_hkt_details(
    stock: str = Query(..., description="港股通标的代码"),
):
    return _call_xtdata_optional(market_data, "get_hkt_details", stock)


@router.get("/exchange_rate")
def get_hkt_exchange_rate(
    account_id: str = Query("", description="账户 ID"),
    account_type: str = Query("", description="账户类型"),
):
    return _call_xtdata_optional(
        market_data,
        "get_hkt_exchange_rate",
        account_id,
        account_type,
    )
