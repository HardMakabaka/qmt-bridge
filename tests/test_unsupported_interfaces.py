from types import SimpleNamespace

from qmt_bridge.server.models import (
    CreateSectorFolderRequest,
    GenerateIndexDataRequest,
    ResetSectorRequest,
)
from qmt_bridge.server.routers import download, formula, sector, tick


def test_unverified_l2_routes_return_unsupported_instead_of_attribute_error(
    monkeypatch,
) -> None:
    # Given: the installed facade has no thousand-level L2 methods.
    monkeypatch.setattr(tick, "xtdata", SimpleNamespace())

    # When: every compatibility route is invoked.
    results = [
        tick.get_l2_thousand_quote("000001.SZ"),
        tick.get_l2_thousand_orderbook("000001.SZ"),
        tick.get_l2_thousand_trade("000001.SZ"),
    ]

    # Then: all routes report the exact native support gap.
    assert [payload["status"] for payload in results] == ["unsupported"] * 3
    assert [payload["reason_code"] for payload in results] == [
        "xtdata_get_l2_thousand_quote_missing",
        "xtdata_get_l2_thousand_orderbook_missing",
        "xtdata_get_l2_thousand_trade_missing",
    ]


def test_unverified_sector_mutations_never_call_an_unrelated_native_method(
    monkeypatch,
) -> None:
    # Given: the installed facade exposes no folder/reset compatibility methods.
    monkeypatch.setattr(sector, "xtdata", SimpleNamespace())

    # When: compatibility mutations are requested.
    folder = sector.create_sector_folder(CreateSectorFolderRequest(folder_name="demo"))
    reset = sector.reset_sector(
        ResetSectorRequest(sector_name="demo", stocks=["000001.SZ"])
    )

    # Then: both are rejected explicitly without semantic substitution.
    assert folder["status"] == "unsupported"
    assert folder["reason_code"] == "xtdata_create_sector_folder_missing"
    assert reset["status"] == "unsupported"
    assert reset["reason_code"] == "xtdata_reset_sector_missing"


def test_unverified_downloads_and_index_generation_are_explicitly_unsupported(
    monkeypatch,
) -> None:
    # Given: no native IPO, option, or matching index-generation contract.
    monkeypatch.setattr(download, "xtdata", SimpleNamespace())
    monkeypatch.setattr(formula, "xtdata", SimpleNamespace())

    # When: compatibility endpoints are invoked.
    ipo = download.download_ipo_data()
    option = download.download_option_data()
    generated = formula.generate_index_data(
        GenerateIndexDataRequest(
            index_code="TEST",
            stocks=["000001.SZ"],
            weights=[1.0],
            period="1d",
        )
    )

    # Then: no unrelated provider API is used as a fallback.
    assert ipo["status"] == "unsupported"
    assert option["status"] == "unsupported"
    assert generated["status"] == "unsupported"
