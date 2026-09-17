"""Router — Formula/Model API endpoints /api/formula/*."""

from fastapi import APIRouter
from ..bigqmt import market_data

from ..helpers import _call_xtdata_serialized, _numpy_to_python
from ..models import (
    CallFormulaBatchRequest,
    CallFormulaRequest,
)

router = APIRouter(prefix="/api/formula", tags=["formula"])


@router.post("/call")
def call_formula(req: CallFormulaRequest):
    """Call a formula/indicator on a single stock."""
    result = _call_xtdata_serialized(
        market_data.call_formula,
        req.formula_name,
        req.stock_code,
        req.period,
        req.start_time,
        req.end_time,
        req.count,
        req.dividend_type,
        **req.params,
    )
    return {"data": _numpy_to_python(result)}


@router.post("/call_batch")
def call_formula_batch(req: CallFormulaBatchRequest):
    """Call a formula/indicator on multiple stocks."""
    result = _call_xtdata_serialized(
        market_data.call_formula_batch,
        req.formula_name,
        req.stock_codes,
        req.period,
        req.start_time,
        req.end_time,
        req.count,
        req.dividend_type,
        **req.params,
    )
    return {"data": _numpy_to_python(result)}
