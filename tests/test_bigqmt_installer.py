import json
from pathlib import Path
import shutil
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER = REPO_ROOT / "scripts" / "install-bigqmt-runtime.ps1"
POWERSHELL = shutil.which("pwsh") or shutil.which("powershell")


@pytest.mark.skipif(POWERSHELL is None, reason="PowerShell is required")
def test_installer_defaults_order_methods_enabled_and_installs_mode_attestation(
    tmp_path: Path,
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
    (config_root / "indexUserConfig.xml").write_text(
        '<catalog scriptType="1" formulaCatalogModelType="4" '
        'systemProvidedStrategy="1" strategymall="0" name="我的策略" '
        'type="1" simpleRun="0"></catalog>',
        encoding="utf-8",
    )

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
                str(REPO_ROOT),
            ],
            stdin=subprocess.DEVNULL,
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
            timeout=30,
            check=False,
        )
    stdout = stdout_path.read_text(encoding="utf-8", errors="replace")
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace")

    assert completed.returncode == 0, stderr
    json_start = stdout.find("{")
    assert json_start >= 0, stdout
    payload = json.loads(stdout[json_start:])
    assert payload["order_methods_enabled"] is True
    assert payload["terminal_mode_attestation"] is True
    assert payload["terminal_mode_attestation_source"] == "qmt_request_id_and_terminal_log"
    installed_entry = (python_root / "MECOSTOCK_BIGQMT_BRIDGE.py").read_text(
        encoding="utf-8"
    )
    assert "qmt_request_id" in installed_entry
    manifest = json.loads(
        (python_root / "MECOSTOCK_BIGQMT_BRIDGE.manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["terminal_mode_attestation"] is True
    assert manifest["terminal_mode_attestation_source"] == "qmt_request_id_and_terminal_log"
