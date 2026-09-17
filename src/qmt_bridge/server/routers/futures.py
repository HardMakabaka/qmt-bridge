"""Router — Futures endpoints /api/futures/*."""

from fastapi import APIRouter, Query
from ..bigqmt import market_data

from ..helpers import _call_xtdata_optional

router = APIRouter(prefix="/api/futures", tags=["futures"])


@router.get("/main_contract")
def get_main_contract(
    code_market: str = Query(..., description="品种市场代码，如 IF.CFE"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    payload = _call_xtdata_optional(
        market_data,
        "get_main_contract",
        code_market,
        start_time=start_time,
        end_time=end_time,
    )
    payload["code_market"] = code_market
    return payload


@router.get("/contract_multiplier")
def get_contract_multiplier(
    contract_code: str = Query(..., description="合约代码"),
):
    payload = _call_xtdata_optional(market_data, "get_contract_multiplier", contract_code)
    payload["unit_contract"] = {"data": "contract multiplier as returned by QMT"}
    return payload


@router.get("/contract_expire_date")
def get_contract_expire_date(
    code_market: str = Query(..., description="合约或品种市场代码"),
):
    return _call_xtdata_optional(market_data, "get_contract_expire_date", code_market)


@router.get("/his_contract_list")
def get_his_contract_list(
    market: str = Query(..., description="市场代码"),
):
    return _call_xtdata_optional(market_data, "get_his_contract_list", market)
