"""Router — Financial data endpoints /api/financial/*."""

from fastapi import APIRouter, Query
from ..bigqmt import market_data

from ..helpers import (
    _call_xtdata_optional,
    _call_xtdata_serialized,
    _exception_status,
    _financial_data_to_records,
    _status_payload,
)

router = APIRouter(prefix="/api/financial", tags=["financial"])


@router.get("/data")
def get_financial_data(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    tables: str = Query("", description="财务表名列表，逗号分隔，为空取全部"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
    report_type: str = Query("report_time", description="报告类型: report_time/announce_time"),
):
    stock_list = [s.strip() for s in stocks.split(",")]
    table_list = [t.strip() for t in tables.split(",") if t.strip()] if tables else []
    try:
        raw = _call_xtdata_serialized(
            market_data.get_financial_data,
            stock_list,
            table_list=table_list,
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )
    except Exception as exc:
        return _status_payload(
            _exception_status(exc),
            reason=str(exc),
            function="get_financial_data",
            error_type=exc.__class__.__name__,
        )
    return {"status": "ok", "data": _financial_data_to_records(raw)}


@router.get("/raw_data")
def get_raw_financial_data(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    fields: str = Query("", description="字段列表，逗号分隔"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
    report_type: str = Query("report_time", description="报告类型"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    field_list = [f.strip() for f in fields.split(",") if f.strip()] if fields else []
    return _call_xtdata_optional(
        market_data,
        "get_raw_financial_data",
        field_list,
        stock_list,
        start_time,
        end_time,
        report_type=report_type,
    )


@router.get("/findata")
def get_findata(
    table: str = Query(..., description="财务表名"),
    field: str = Query("", description="字段名"),
    session: str = Query("", description="可选 session"),
):
    if session:
        return _call_xtdata_optional(market_data, "getfindata", table, field, session)
    if field:
        return _call_xtdata_optional(market_data, "getfindata", table, field)
    return _call_xtdata_optional(market_data, "getfindata", table)
