"""Router — Sector endpoints /api/sector/*."""

import os
from pathlib import Path

from fastapi import APIRouter, Query
from ..bigqmt import xtdata

from ..helpers import _call_xtdata_optional, _call_xtdata_serialized, _numpy_to_python
from ..models import (
    AddSectorStocksRequest,
    CreateSectorFolderRequest,
    CreateSectorRequest,
    RemoveSectorStocksRequest,
    ResetSectorRequest,
)
from ..qmt_sector_cache import (
    read_qmt_sector_names,
    read_qmt_sector_stocks,
    resolve_qmt_sector_raw_name,
)

router = APIRouter(prefix="/api/sector", tags=["sector"])


def _local_sector_root() -> Path | None:
    raw = str(os.getenv("QMT_BRIDGE_LOCAL_DAT_ROOT") or "").strip()
    return Path(raw) if raw else None


def _current_sector_names() -> tuple[list[str], str]:
    root = _local_sector_root()
    if root is not None:
        sectors = read_qmt_sector_names(root)
        if sectors:
            return sectors, "qmt_local_sector"
    sectors = [
        str(sector)
        for sector in _call_xtdata_serialized(xtdata.get_sector_list) or []
    ]
    return sectors, "qmt_rpc_sector"


def _sector_stocks(
    sector_name: str,
    real_timetag: int | str,
) -> tuple[list[str], str]:
    root = _local_sector_root()
    if real_timetag == -1 and root is not None:
        stocks = read_qmt_sector_stocks(root, sector_name)
        if stocks is not None:
            return stocks, "qmt_local_sector"
    rpc_sector_name = (
        resolve_qmt_sector_raw_name(root, sector_name)
        if root is not None
        else sector_name
    )
    stocks = _call_xtdata_serialized(
        xtdata.get_stock_list_in_sector,
        rpc_sector_name,
        real_timetag=real_timetag,
    )
    return list(stocks or []), "qmt_rpc_sector"


def _normalize_real_timetag(value: int | str | None) -> int | str:
    if value in (None, "", "-1", -1):
        return -1
    text = str(value).strip()
    if text.isdigit() and len(text) == 8:
        return text
    try:
        return int(text)
    except (TypeError, ValueError):
        return text


def _stock_matches(candidate: str, target: str) -> bool:
    candidate_text = str(candidate or "").strip().upper()
    target_text = str(target or "").strip().upper()
    if not candidate_text or not target_text:
        return False
    if candidate_text == target_text:
        return True
    return candidate_text.split(".", 1)[0] == target_text.split(".", 1)[0]


@router.get("/list")
def get_sector_list(
    keyword: str | None = Query(None, description="可选板块名称关键词，如 英伟达 / 算力 / CPO"),
    limit: int = Query(0, ge=0, le=10000, description="最多返回条数；0 表示不限制"),
):
    sectors, source = _current_sector_names()
    total_count = len(sectors)
    keyword_text = str(keyword or "").strip()
    if keyword_text:
        sectors = [sector for sector in sectors if keyword_text in sector]
    filtered_count = len(sectors)
    if limit:
        sectors = sectors[:limit]
    return {
        "sectors": sectors,
        "count": len(sectors),
        "total_count": total_count,
        "filtered_count": filtered_count,
        "keyword": keyword_text or None,
        "truncated": bool(limit and filtered_count > len(sectors)),
        "source": source,
    }


@router.get("/stocks")
def get_sector_stocks(
    sector: str = Query(..., description="板块名称，如 沪深A股 / 上证A股 / 深证A股 / 沪深ETF / 上证50 / 沪深300"),
    real_timetag: str = Query(
        "-1",
        description="历史日期，支持毫秒时间戳或 YYYYMMDD 字符串；-1 表示最新",
    ),
):
    normalized_real_timetag = _normalize_real_timetag(real_timetag)
    stock_list, source = _sector_stocks(sector, normalized_real_timetag)
    return {
        "sector": sector,
        "real_timetag": normalized_real_timetag,
        "count": len(stock_list),
        "stocks": stock_list,
        "source": source,
    }


@router.get("/stock-memberships")
def get_stock_sector_memberships(
    stock: str = Query(..., description="股票代码，支持 000001 或 000001.SZ"),
    real_timetag: str = Query(
        "-1",
        description="历史日期，支持毫秒时间戳或 YYYYMMDD 字符串；-1 表示最新",
    ),
    keyword: str | None = Query(None, description="可选板块名称关键词，如 英伟达 / 算力 / CPO"),
):
    normalized_real_timetag = _normalize_real_timetag(real_timetag)
    sectors, source = _current_sector_names()
    keyword_text = str(keyword or "").strip()
    if keyword_text:
        sectors = [sector for sector in sectors if keyword_text in str(sector)]
    if not sectors:
        reason = "qmt_sector_keyword_no_match" if keyword_text else "qmt_sector_list_empty"
        return {
            "status": "unavailable",
            "reason": reason,
            "stock": stock,
            "real_timetag": normalized_real_timetag,
            "keyword": keyword_text or None,
            "sector_count": 0,
            "matched_count": 0,
            "sectors": [],
            "failures": [],
        }

    matched: list[str] = []
    failures: list[dict[str, str]] = []
    for sector in sectors:
        try:
            stock_list, _stock_source = _sector_stocks(
                str(sector),
                normalized_real_timetag,
            )
        except Exception as exc:  # pragma: no cover - xtdata runtime boundary
            failures.append({"sector": str(sector), "error": str(exc)})
            continue
        if any(_stock_matches(item, stock) for item in stock_list or []):
            matched.append(str(sector))

    status = "partial" if failures else "ok"
    return {
        "status": status,
        "stock": stock,
        "real_timetag": normalized_real_timetag,
        "keyword": keyword_text or None,
        "sector_count": len(sectors),
        "matched_count": len(matched),
        "sectors": matched,
        "failures": failures,
        "source": source,
    }


@router.get("/info")
def get_sector_info(
    sector: str = Query("", description="板块名称，为空返回所有板块信息"),
):
    raw = _call_xtdata_serialized(xtdata.get_sector_info, sector_name=sector)
    return {"data": _numpy_to_python(raw)}


# ---------------------------------------------------------------------------
# Write operations (Step 5)
# ---------------------------------------------------------------------------


@router.post("/create_folder")
def create_sector_folder(req: CreateSectorFolderRequest):
    """Create a new sector folder."""
    return _call_xtdata_optional(xtdata, "create_sector_folder", req.folder_name)


@router.post("/create")
def create_sector(req: CreateSectorRequest):
    """Create a new sector under a folder."""
    result = _call_xtdata_serialized(
        xtdata.create_sector,
        req.sector_name,
        req.parent_node,
    )
    return {"status": "ok", "data": _numpy_to_python(result)}


@router.post("/add_stocks")
def add_sector_stocks(req: AddSectorStocksRequest):
    """Add stocks to a sector."""
    result = _call_xtdata_serialized(
        xtdata.add_sector,
        req.sector_name,
        req.stocks,
    )
    return {"status": "ok", "data": _numpy_to_python(result)}


@router.post("/remove_stocks")
def remove_sector_stocks(req: RemoveSectorStocksRequest):
    """Remove stocks from a sector."""
    return _call_xtdata_optional(
        xtdata,
        "remove_stock_from_sector",
        req.sector_name,
        req.stocks,
    )


@router.delete("/remove")
def remove_sector(
    sector_name: str = Query(..., description="板块名称"),
):
    """Remove an entire sector."""
    result = _call_xtdata_serialized(xtdata.remove_sector, sector_name)
    return {"status": "ok", "data": _numpy_to_python(result)}


@router.post("/reset")
def reset_sector(req: ResetSectorRequest):
    """Reset sector stocks (replace all stocks)."""
    return _call_xtdata_optional(
        xtdata,
        "reset_sector",
        req.sector_name,
        req.stocks,
    )
