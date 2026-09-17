"""Router — Option data endpoints /api/option/*."""

from fastapi import APIRouter, Query
from ..bigqmt import market_data

from ..helpers import _call_xtdata_optional

router = APIRouter(prefix="/api/option", tags=["option"])


def _normalize_option_payload(payload: dict):
    reason = str(payload.get("reason") or "")
    if payload.get("status") == "error" and "NoneType" in reason:
        payload["status"] = "unavailable"
        payload["reason"] = f"qmt_option_sector_data_unavailable: {reason}"
    return payload


@router.get("/detail")
def get_option_detail(
    option_code: str = Query(..., description="期权合约代码"),
):
    payload = _call_xtdata_optional(market_data, "get_option_detail_data", option_code)
    payload["option_code"] = option_code
    return _normalize_option_payload(payload)


@router.get("/chain")
def get_option_chain(
    undl_code: str = Query(..., description="标的代码，如 000300.SH"),
):
    payload = _call_xtdata_optional(market_data, "get_option_undl_data", undl_code)
    payload["undl_code"] = undl_code
    return _normalize_option_payload(payload)


@router.get("/list")
def get_option_list(
    undl_code: str = Query(..., description="标的代码，如 000300.SH"),
    dedate: str = Query(..., description="到期日"),
    opttype: str = Query("", description="期权类型"),
    isavailable: bool = Query(False, description="是否仅返回可交易合约"),
):
    payload = _call_xtdata_optional(
        market_data,
        "get_option_list",
        undl_code,
        dedate,
        opttype=opttype,
        isavailavle=isavailable,
    )
    return _normalize_option_payload(payload)


@router.get("/history_list")
def get_history_option_list(
    undl_code: str = Query(..., description="标的代码，如 000300.SH"),
    dedate: str = Query(..., description="历史日期"),
):
    payload = _call_xtdata_optional(market_data, "get_his_option_list", undl_code, dedate)
    return _normalize_option_payload(payload)


@router.get("/iv")
def get_option_iv(
    option_code: str = Query(..., description="期权合约代码"),
):
    return _call_xtdata_optional(market_data, "get_option_iv", option_code)
