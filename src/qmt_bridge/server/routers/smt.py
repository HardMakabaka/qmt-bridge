"""Router — SMT (约定式交易) endpoints /api/smt/* (requires API Key)."""

from fastapi import APIRouter, Depends

from ..deps import get_trader_manager
from ..helpers import _optional_result_payload
from ..security import require_api_key

router = APIRouter(prefix="/api/smt", tags=["smt"], dependencies=[Depends(require_api_key)])


@router.get("/appointment")
def query_appointment_info(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query SMT appointment info (约定式预约信息)."""
    result = manager.query_extension(
        "query_appointment_info", account_id=account_id
    )
    return _optional_result_payload(result)


@router.get("/secu_info")
def query_smt_secu_info(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query SMT security info (约定式证券信息)."""
    result = manager.query_extension("query_smt_secu_info", account_id=account_id)
    return _optional_result_payload(result)


@router.get("/secu_rate")
def query_smt_secu_rate(
    stock_code: str = "",
    max_term: int = 0,
    fare_way: int = 0,
    credit_type: int = 0,
    trade_type: int = 0,
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query SMT security rates (约定式证券费率)."""
    result = manager.query_extension(
        "query_smt_secu_rate",
        {
            "stock_code": stock_code,
            "max_term": max_term,
            "fare_way": fare_way,
            "credit_type": credit_type,
            "trade_type": trade_type,
        },
        account_id=account_id,
    )
    return _optional_result_payload(result)
