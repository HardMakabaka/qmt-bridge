import sys
from types import ModuleType, SimpleNamespace

xtquant_stub = ModuleType("xtquant")
xtquant_stub.xtdata = SimpleNamespace()
sys.modules.setdefault("xtquant", xtquant_stub)

from qmt_bridge.server.routers import sector


def test_sector_stocks_preserves_yyyymmdd_real_timetag(monkeypatch):
    calls = []

    def get_stock_list_in_sector(sector_name, real_timetag=-1):
        calls.append((sector_name, real_timetag))
        return ["000001.SZ"]

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(get_stock_list_in_sector=get_stock_list_in_sector),
    )

    payload = sector.get_sector_stocks(sector="英伟达概念", real_timetag="20240102")

    assert calls == [("英伟达概念", "20240102")]
    assert payload["real_timetag"] == "20240102"
    assert payload["stocks"] == ["000001.SZ"]


def test_sector_stock_memberships_filters_keyword_and_preserves_date(monkeypatch):
    calls = []

    def get_sector_list():
        return ["英伟达概念", "CPO", "沪深A股"]

    def get_stock_list_in_sector(sector_name, real_timetag=-1):
        calls.append((sector_name, real_timetag))
        if sector_name == "英伟达概念":
            return ["000001.SZ", "300001.SZ"]
        if sector_name == "CPO":
            return ["300001.SZ"]
        return ["000001.SZ", "600000.SH"]

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(
            get_sector_list=get_sector_list,
            get_stock_list_in_sector=get_stock_list_in_sector,
        ),
    )

    payload = sector.get_stock_sector_memberships(
        stock="000001",
        real_timetag="20240102",
        keyword="英伟达",
    )

    assert calls == [("英伟达概念", "20240102")]
    assert payload["status"] == "ok"
    assert payload["sector_count"] == 1
    assert payload["matched_count"] == 1
    assert payload["sectors"] == ["英伟达概念"]


def test_sector_stock_memberships_reports_keyword_cache_miss(monkeypatch):
    calls = []

    def get_sector_list():
        return ["沪深A股", "创业板"]

    def get_stock_list_in_sector(sector_name, real_timetag=-1):
        calls.append((sector_name, real_timetag))
        return ["000001.SZ"]

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(
            get_sector_list=get_sector_list,
            get_stock_list_in_sector=get_stock_list_in_sector,
        ),
    )

    payload = sector.get_stock_sector_memberships(
        stock="000001.SZ",
        real_timetag="20240102",
        keyword="英伟达",
    )

    assert calls == []
    assert payload["status"] == "unavailable"
    assert payload["reason"] == "qmt_sector_keyword_no_match"
    assert payload["matched_count"] == 0
