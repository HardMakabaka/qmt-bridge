import importlib.util

from bigqmt_signal_trader import BigQmtDataClient, BigQmtTradingClient
from qmt_bridge import QMTClient
from qmt_bridge.server.app import create_app
from qmt_bridge.server.config import Settings


def test_no_miniqmt_compatibility_module_or_fake_interfaces():
    assert importlib.util.find_spec("bigqmt_signal_trader.xtquant_compat") is None
    for method in ("download_history_data2", "subscribe_quote2", "subscribe_whole_quote", "_next_seq"):
        assert not hasattr(BigQmtDataClient, method)
    for method in ("order_stock", "query_stock_asset", "query_stock_positions_async", "subscribe"):
        assert not hasattr(BigQmtTradingClient, method)
    assert not hasattr(QMTClient, "get_xtdata_version")
    assert hasattr(QMTClient, "get_runtime_version")


def test_runtime_metadata_does_not_advertise_a_miniqmt_sdk():
    paths = create_app(Settings()).openapi()["paths"]
    assert "/api/meta/runtime_version" in paths
    assert "/api/meta/xtdata_version" not in paths


def test_provenance_text_digest_survives_git_line_ending_conversion(tmp_path):
    from qmt_bridge.install_runtime import _sha256

    path = tmp_path / "fixture.py"
    path.write_bytes(b"value = 1\r\n")
    crlf = _sha256(path, normalize_newlines=True)
    path.write_bytes(b"value = 1\n")
    assert _sha256(path, normalize_newlines=True) == crlf
    path.write_bytes(b"value = 2\n")
    assert _sha256(path, normalize_newlines=True) != crlf
