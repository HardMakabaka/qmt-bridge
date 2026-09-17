import json
from pathlib import Path
import shutil
import subprocess
import hashlib
import os
import threading

import pytest

from qmt_bridge.install_runtime import InstallRequest, _write_config
from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER = REPO_ROOT / "scripts" / "install-bigqmt-runtime.ps1"
POWERSHELL = shutil.which("pwsh") or shutil.which("powershell")


def _write_runtime_source(root: Path) -> Path:
    """Build a minimal, checksum-pinned source fixture for a WhatIf install."""
    source = root / "runtime-source"
    source_src = source / "src"
    source_src.mkdir(parents=True)
    for name in (
        "BIGQMT_REDIS_DRYRUN.py",
        "MECOSTOCK_BIGQMT_ZMQ.py",
        "mecostock_bigqmt_mode_overlay.py",
        "bigqmt_signal_trader_strategy.py",
        "bigqmt_signal_trader_redis_rpc_runtime.py",
    ):
        shutil.copy2(REPO_ROOT / "src" / name, source_src / name)
    shutil.copytree(REPO_ROOT / "src" / "bigqmt_signal_trader", source_src / "bigqmt_signal_trader")
    entry = source_src / "BIGQMT_REDIS_DRYRUN.py"
    manifest = {
        "repository": "https://github.com/litaolemo/xtquant_big_convert",
        "upstream_sha": "40f7275b15843bd167b7ad424a51d3d547be88df",
        "file_sha256": {
            "src/BIGQMT_REDIS_DRYRUN.py": hashlib.sha256(entry.read_bytes()).hexdigest(),
        },
    }
    provenance = source / "third_party" / "xtquant_big_convert.UPSTREAM.json"
    provenance.parent.mkdir()
    provenance.write_text(json.dumps(manifest), encoding="utf-8")
    return source


def test_generated_runtime_config_defers_every_rpc_to_strategy_adjust(tmp_path: Path) -> None:
    config = tmp_path / "bigqmt_signal_trader_local_config.py"
    request = InstallRequest(
        qmt_root=tmp_path,
        account_id="acct-test",
        asset_root=tmp_path,
        zmq_endpoint="tcp://127.0.0.1:15560",
        event_zmq_endpoint="tcp://127.0.0.1:15561",
        order_methods_enabled=True,
    )
    _write_config(request, config)
    namespace: dict[str, object] = {}
    exec(compile(config.read_text(encoding="utf-8"), str(config), "exec"), namespace)
    runtime = namespace["BIGQMT_REDIS_CONFIG"]
    assert runtime["rpc_background_threads"] is True
    assert runtime["rpc_process_in_listener"] is False
    assert runtime["rpc_listener_methods"] == ()
    assert runtime["schedule_adjust"] is True
    assert runtime["schedule_adjust_interval"] == "200nMilliSecond"


def test_deferred_receiver_never_executes_rpc_outside_adjust_thread() -> None:
    executed_on: list[int] = []

    class Handlers:
        def handle(self, _method: str, _params: object) -> dict[str, bool]:
            executed_on.append(threading.get_ident())
            return {"ok": True}

    class Transport:
        def send_response(self, _request: object, _response: object) -> None:
            return None

    service = RedisPubSubRpcService(
        redis_client=object(),
        handlers=Handlers(),
        account_id="acct-test",
        transport=Transport(),
        process_in_listener=False,
        listener_methods=(),
    )
    receiver = threading.Thread(
        target=service.enqueue_payload,
        args=({"request_id": "request-1", "method": "get_asset", "account_id": "acct-test"},),
    )
    receiver.start()
    receiver.join(timeout=1)
    assert not receiver.is_alive()
    assert executed_on == []

    assert service.drain_pending(max_items=1) == 1
    assert executed_on == [threading.get_ident()]


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is required")
def test_installer_what_if_validates_fake_qmt_root_without_mutating_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    qmt_root = tmp_path / "qmt"
    python_root = qmt_root / "python"
    binary_root = qmt_root / "bin.x64"
    config_root = qmt_root / "config"
    python_root.mkdir(parents=True)
    binary_root.mkdir()
    config_root.mkdir()
    (binary_root / "python36.dll").write_bytes(b"test")
    (python_root / "MECOSTOCK_BIGQMT_ZMQ.py").write_text("# compiled model\n", encoding="utf-8")
    catalog = config_root / "indexUserConfig.xml"
    catalog_text = (
        '<catalog scriptType="1" formulaCatalogModelType="4" '
        'systemProvidedStrategy="1" strategymall="0" name="我的策略" '
        'type="1" simpleRun="0"></catalog>'
    )
    catalog.write_text(catalog_text, encoding="utf-8")

    source_root = _write_runtime_source(tmp_path)
    trace_dir = tmp_path / "traces"
    monkeypatch.setenv("QMT_BRIDGE_TRACE_ENABLED", "true")
    monkeypatch.setenv("QMT_BRIDGE_TRACE_DIR", str(trace_dir))
    monkeypatch.setenv("QMT_BRIDGE_TRACE_ID", "installer-test-trace")
    stdout_path = tmp_path / "stdout.txt"
    stderr_path = tmp_path / "stderr.txt"
    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_file:
        completed = subprocess.run(
            [
                POWERSHELL,
                "-NoProfile",
                "-NonInteractive",
                "-File",
                str(INSTALLER),
                "-QmtRoot",
                str(qmt_root),
                "-AccountId",
                "acct-test",
                "-SourceRoot",
                str(source_root),
                "-WhatIf",
            ],
            stdin=subprocess.DEVNULL,
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
                timeout=30,
                check=False,
                env=os.environ.copy(),
        )
    stdout = stdout_path.read_text(encoding="utf-8", errors="replace")
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace")

    assert completed.returncode == 0, stderr
    json_start = stdout.find("{")
    assert json_start >= 0, stdout
    payload = json.loads(stdout[json_start:])
    assert payload["order_methods_enabled"] is True
    assert payload["dry_run"] is True
    assert payload["terminal_mode_attestation"] is True
    assert payload["terminal_mode_attestation_source"] == "qmt_request_id_and_terminal_log"
    assert not (python_root / "MECOSTOCK_BIGQMT_BRIDGE.py").exists()
    assert not (python_root / "MECOSTOCK_BIGQMT_BRIDGE.manifest.json").exists()
    assert catalog.read_text(encoding="utf-8") == catalog_text
    events = [json.loads(line) for path in trace_dir.glob("*.jsonl") for line in path.read_text(encoding="utf-8").splitlines()]
    assert {"installer.start", "installer.manifest_verified", "installer.install"} <= {
        event["event_name"] for event in events
    }
    assert {event["trace_id"] for event in events} == {"installer-test-trace"}
