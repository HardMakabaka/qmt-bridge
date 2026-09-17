from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def test_wheel_force_includes_all_embedded_runtime_assets() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    includes = project["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]
    expected = {
        "src/BIGQMT_REDIS_DRYRUN.py",
        "src/MECOSTOCK_BIGQMT_ZMQ.py",
        "src/mecostock_bigqmt_mode_overlay.py",
        "src/bigqmt_signal_trader_strategy.py",
        "src/bigqmt_signal_trader_redis_rpc_runtime.py",
        "src/bigqmt_signal_trader",
        "third_party/xtquant_big_convert.UPSTREAM.json",
    }
    assert expected <= set(includes)
    for source, destination in includes.items():
        assert (ROOT / source).exists(), source
        assert destination.startswith("qmt_bridge/_runtime_assets/")
    assert (ROOT / "src" / "bigqmt_signal_trader" / "telemetry.py").is_file()


def test_runtime_installer_is_a_packaged_console_entry() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["scripts"]["qmt-install-runtime"] == "qmt_bridge.install_runtime:main"
