"""Router — Fund transfer endpoints /api/fund/* (requires API Key)."""

from fastapi import APIRouter, Depends

from ..deps import get_trader_manager
from ..helpers import _optional_result_payload
from ..security import require_api_key

router = APIRouter(prefix="/api/fund", tags=["fund"], dependencies=[Depends(require_api_key)])


@router.get("/available")
def query_available_fund(
    account_id: str = "",
    manager=Depends(get_trader_manager),
):
    """Query available fund balance."""
    result = manager.query_asset(account_id=account_id)
    return _optional_result_payload(result)
