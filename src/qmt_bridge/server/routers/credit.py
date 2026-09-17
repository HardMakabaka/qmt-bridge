"""Router — Credit trading endpoints /api/credit/* (requires API Key)."""

from fastapi import APIRouter, Depends

from ..deps import get_trader_manager
from ..helpers import _optional_result_payload
from ..models import CreditQueryRequest
from ..security import require_api_key

router = APIRouter(prefix="/api/credit", tags=["credit"], dependencies=[Depends(require_api_key)])


@router.get("/positions")
def query_credit_positions(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query credit trading positions."""
    result = manager.query_positions(account_id=account_id)
    return _optional_result_payload(result)


@router.get("/asset")
def query_credit_asset(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query credit trading account asset."""
    result = manager.query_asset(account_id=account_id)
    return _optional_result_payload(result)


@router.get("/debt")
def query_credit_debt(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query credit debt information."""
    result = manager.query_extension("query_stk_compacts", account_id=account_id)
    return _optional_result_payload(result)


@router.get("/slo_stocks")
def query_slo_stocks(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query stocks available for short selling (融券标的)."""
    result = manager.query_extension("query_credit_slo_code", account_id=account_id)
    return _optional_result_payload(result)


@router.get("/fin_stocks")
def query_fin_stocks(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query stocks available for margin buying (融资标的)."""
    result = manager.query_extension("query_credit_subjects", account_id=account_id)
    return _optional_result_payload(result)


@router.get("/subjects")
def query_credit_subjects(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query credit subject list (标的证券)."""
    result = manager.query_extension("query_credit_subjects", account_id=account_id)
    return _optional_result_payload(result)


@router.get("/assure")
def query_credit_assure(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query credit assurance / collateral info (担保品)."""
    result = manager.query_extension("query_credit_assure", account_id=account_id)
    return _optional_result_payload(result)
