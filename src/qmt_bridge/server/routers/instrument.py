"""Router — Instrument info endpoints /api/instrument/*."""

from fastapi import APIRouter, Query
from ..bigqmt import xtdata

from ..helpers import (
    _call_xtdata_optional,
    _call_xtdata_serialized,
    _numpy_to_python,
)

router = APIRouter(prefix="/api/instrument", tags=["instrument"])


@router.get("/batch_detail")
def get_batch_instrument_detail(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    iscomplete: bool = Query(False, description="是否返回完整信息"),
):
    stock_list = [s.strip() for s in stocks.split(",")]
    raw = _call_xtdata_serialized(
        xtdata.get_instrument_detail_list,
        stock_list,
        iscomplete=iscomplete,
    )
    return {"data": _numpy_to_python(raw)}


@router.get("/type")
def get_instrument_type(
    stock: str = Query(..., description="股票代码，如 600000.SH"),
):
    raw = _call_xtdata_serialized(xtdata.get_instrument_type, stock)
    return {"stock": stock, "type": raw}


@router.get("/ipo_info")
def get_ipo_info(
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    raw = _call_xtdata_serialized(
        xtdata.get_ipo_info,
        start_time=start_time,
        end_time=end_time,
    )
    return {"data": _numpy_to_python(raw)}


@router.get("/index_weight")
def get_index_weight(
    index_code: str = Query(..., description="指数代码，如 000300.SH"),
):
    raw = _call_xtdata_serialized(xtdata.get_index_weight, index_code)
    return {"index_code": index_code, "data": _numpy_to_python(raw)}


@router.get("/st_history")
def get_st_history(
    stock: str = Query(..., description="股票代码"),
):
    raw = _call_xtdata_serialized(xtdata.get_his_st_data, stock)
    return {"stock": stock, "data": _numpy_to_python(raw)}


@router.get("/open_date")
def get_open_date(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    data = {}
    status = "ok"
    reason = ""
    for stock in stock_list:
        payload = _call_xtdata_optional(xtdata, "get_open_date", stock)
        data[stock] = {
            "status": payload["status"],
            "open_date": payload.get("data"),
            "reason": payload.get("reason", ""),
        }
        if payload["status"] != "ok":
            status = payload["status"]
            reason = payload.get("reason", reason)
    return {"status": status, "reason": reason, "data": data}


@router.get("/total_share")
def get_total_share(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    data = {}
    status = "ok"
    reason = ""
    for stock in stock_list:
        payload = _call_xtdata_optional(xtdata, "get_total_share", stock)
        data[stock] = payload
        if payload["status"] != "ok":
            status = payload["status"]
            reason = payload.get("reason", reason)
    return {
        "status": status,
        "reason": reason,
        "data": data,
        "unit_contract": {
            "data": "shares",
            "warning": "stock share capital; never treat as ETF fund shares",
        },
    }


@router.get("/last_volume")
def get_last_volume(
    stock: str = Query(..., description="股票代码"),
):
    return _call_xtdata_optional(xtdata, "get_last_volume", stock)


@router.get("/turnover_rate")
def get_turnover_rate(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    payload = _call_xtdata_optional(
        xtdata,
        "get_turnover_rate",
        stock_list,
        start_time,
        end_time,
    )
    payload["unit_contract"] = {"data": "QMT native turnover rate"}
    return payload


@router.get("/svol")
def get_svol(
    stock: str = Query(..., description="股票代码"),
):
    payload = _call_xtdata_optional(xtdata, "get_svol", stock)
    payload["unit_contract"] = {"data": "inner sell volume as returned by QMT"}
    return payload


@router.get("/bvol")
def get_bvol(
    stock: str = Query(..., description="股票代码"),
):
    payload = _call_xtdata_optional(xtdata, "get_bvol", stock)
    payload["unit_contract"] = {"data": "outer buy volume as returned by QMT"}
    return payload


@router.get("/longhubang")
def get_longhubang(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    return _call_xtdata_optional(
        xtdata,
        "get_longhubang",
        stock_list,
        start_time,
        end_time,
    )


@router.get("/top10_share_holder")
def get_top10_share_holder(
    stocks: str = Query(..., description="股票代码列表，逗号分隔"),
    data_name: str = Query("", description="数据集名称"),
    start_time: str = Query("", description="开始时间"),
    end_time: str = Query("", description="结束时间"),
):
    stock_list = [s.strip() for s in stocks.split(",") if s.strip()]
    return _call_xtdata_optional(
        xtdata,
        "get_top10_share_holder",
        stock_list,
        data_name,
        start_time,
        end_time,
    )


@router.get("/risk_free_rate")
def get_risk_free_rate(
    index: int = Query(0, description="利率索引"),
):
    return _call_xtdata_optional(xtdata, "get_risk_free_rate", index)


@router.get("/his_index_data")
def get_his_index_data(
    stock: str = Query(..., description="指数代码"),
):
    return _call_xtdata_optional(xtdata, "get_his_index_data", stock)


@router.get("/suspended")
def is_suspended_stock(
    stock: str = Query(..., description="股票代码"),
):
    return _call_xtdata_optional(xtdata, "is_suspended_stock", stock)
