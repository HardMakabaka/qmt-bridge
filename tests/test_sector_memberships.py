import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

xtquant_stub = ModuleType("xtquant")
setattr(xtquant_stub, "xtdata", SimpleNamespace())
sys.modules.setdefault("xtquant", xtquant_stub)

from qmt_bridge.server.routers import sector


def _write_sector_cache(root: Path) -> None:
    sector_root = root / "Sector" / "申万一级行业板块"
    sector_root.mkdir(parents=True)
    (sector_root / "sectorConfig.xml").write_text(
        '<?xml version="1.0" encoding="utf-8"?>'
        '<CustomSector><Item name="申万一级行业板块" type="0">'
        '<Item name="SW1银行" type="2" />'
        '<Item name="SW1食品饮料" type="2" />'
        "</Item></CustomSector>",
        encoding="utf-8",
    )
    (sector_root / "SW1银行").write_text(
        "600000.SH,000001.SZ,600000.SH",
        encoding="utf-8",
    )
    (sector_root / "SW1食品饮料").write_text("600519.SH", encoding="utf-8")


def _write_concept_sector_cache(root: Path) -> None:
    fixtures = {
        "D概念": ("大盘", "600000.SH"),
        "T概念": ("算力", "000001.SZ"),
    }
    for folder, (sector_name, stock_code) in fixtures.items():
        sector_root = root / "Sector" / folder
        sector_root.mkdir(parents=True)
        (sector_root / "sectorConfig.xml").write_text(
            '<?xml version="1.0" encoding="utf-8"?>'
            f'<CustomSector><Item name="{folder}" type="0">'
            f'<Item name="{sector_name}" type="2" />'
            "</Item></CustomSector>",
            encoding="utf-8",
        )
        (sector_root / sector_name).write_text(stock_code, encoding="utf-8")


def test_sector_list_prefers_local_full_qmt_cache_without_rpc(
    monkeypatch,
    tmp_path: Path,
) -> None:
    # Given: a full-QMT sector cache and an RPC facade that must not be touched.
    _write_sector_cache(tmp_path)
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))

    def unexpected_rpc_call():
        raise AssertionError("local sector catalog must not call Big-QMT RPC")

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(get_sector_list=unexpected_rpc_call),
    )

    # When: the HTTP sector-list handler reads the current catalog.
    payload = sector.get_sector_list(keyword=None, limit=0)

    # Then: it returns the disk-backed catalog without entering ZMQ.
    assert payload["source"] == "qmt_local_sector"
    assert payload["sectors"] == ["SW1银行", "SW1食品饮料"]
    assert payload["total_count"] == 2


def test_latest_sector_stocks_prefers_local_full_qmt_cache_without_rpc(
    monkeypatch,
    tmp_path: Path,
) -> None:
    # Given: a latest membership file containing a duplicate stock code.
    _write_sector_cache(tmp_path)
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))

    def unexpected_rpc_call(*_args, **_kwargs):
        raise AssertionError("latest local sector membership must not call Big-QMT RPC")

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(get_stock_list_in_sector=unexpected_rpc_call),
    )

    # When: the latest sector membership is requested.
    payload = sector.get_sector_stocks(sector="SW1银行", real_timetag="-1")

    # Then: codes are read and deduplicated from the QMT cache.
    assert payload["source"] == "qmt_local_sector"
    assert payload["stocks"] == ["600000.SH", "000001.SZ"]


def test_local_concept_sector_names_restore_legacy_namespaces(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_concept_sector_cache(tmp_path)
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))

    payload = sector.get_sector_list(keyword=None, limit=0)

    assert payload["sectors"] == ["TDGN大盘", "TGN算力"]


def test_prefixed_local_concept_sector_resolves_membership_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_concept_sector_cache(tmp_path)
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))

    def unexpected_rpc_call(*_args, **_kwargs):
        raise AssertionError("prefixed local concept must not call Big-QMT RPC")

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(get_stock_list_in_sector=unexpected_rpc_call),
    )

    payload = sector.get_sector_stocks(sector="TGN算力", real_timetag="-1")

    assert payload["source"] == "qmt_local_sector"
    assert payload["stocks"] == ["000001.SZ"]


def test_prefixed_concept_sector_uses_raw_name_for_historical_rpc(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _write_concept_sector_cache(tmp_path)
    monkeypatch.setenv("QMT_BRIDGE_LOCAL_DAT_ROOT", str(tmp_path))
    calls = []

    def get_stock_list_in_sector(sector_name, real_timetag=-1):
        calls.append((sector_name, real_timetag))
        return ["000001.SZ"]

    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(get_stock_list_in_sector=get_stock_list_in_sector),
    )

    payload = sector.get_sector_stocks(
        sector="TGN算力",
        real_timetag="20260811",
    )

    assert calls == [("算力", "20260811")]
    assert payload["source"] == "qmt_rpc_sector"
    assert payload["stocks"] == ["000001.SZ"]


def test_sector_list_filters_keyword_and_limit(monkeypatch):
    monkeypatch.setattr(
        sector,
        "xtdata",
        SimpleNamespace(
            get_sector_list=lambda: [
                "TGN英伟达概念",
                "TDGN英伟达",
                "TGN算力租赁",
                "沪深A股",
            ]
        ),
    )

    payload = sector.get_sector_list(keyword="英伟达", limit=1)

    assert payload["sectors"] == ["TGN英伟达概念"]
    assert payload["count"] == 1
    assert payload["total_count"] == 4
    assert payload["filtered_count"] == 2
    assert payload["keyword"] == "英伟达"
    assert payload["truncated"] is True


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
