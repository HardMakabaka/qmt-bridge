"""Install the pinned Big QMT embedded runtime from source or a wheel.

The embedded files have one canonical source tree (``src/``).  Wheels carry an
immutable copy below package resources solely so ``qmt-install-runtime`` works
after a normal package installation.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.resources
import json
import os
import shutil
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


EXPECTED_UPSTREAM_SHA = "40f7275b15843bd167b7ad424a51d3d547be88df"
_ASSET_FILES = (
    "src/bigqmt_signal_trader",
    "src/bigqmt_signal_trader_strategy.py",
    "src/bigqmt_signal_trader_redis_rpc_runtime.py",
    "src/BIGQMT_REDIS_DRYRUN.py",
    "src/MECOSTOCK_BIGQMT_ZMQ.py",
    "src/mecostock_bigqmt_mode_overlay.py",
)


def _telemetry():
    try:
        from bigqmt_signal_trader import telemetry

        return telemetry
    except Exception:
        return None


def _emit_lifecycle(event_name: str, *, critical: bool = False, **fields: object) -> None:
    telemetry = _telemetry()
    if telemetry is not None:
        telemetry.emit(event_name, critical=critical, **fields)


@dataclass(frozen=True)
class InstallRequest:
    qmt_root: Path
    account_id: str
    asset_root: Path
    zmq_endpoint: str
    event_zmq_endpoint: str
    order_methods_enabled: bool
    dry_run: bool = False


def _resource_asset_root() -> Path:
    """Materialize package resources when installation comes from a wheel."""
    resource = importlib.resources.files("qmt_bridge").joinpath("_runtime_assets")
    with importlib.resources.as_file(resource) as path:
        # ``as_file`` may be temporary for zipped importers; retain a stable copy
        # for the duration of this command only.
        target = Path(path)
        if target.is_dir():
            return target
    raise RuntimeError("packaged Big QMT runtime assets are unavailable")


def resolve_asset_root(source_root: str | Path | None) -> Path:
    if source_root:
        root = Path(source_root).expanduser().resolve()
        if (root / "src" / "BIGQMT_REDIS_DRYRUN.py").is_file():
            return root
        raise ValueError(f"Big QMT runtime sources are missing under: {root}")
    return _resource_asset_root()


def _validate_loopback_endpoint(value: str, name: str) -> None:
    import re

    if not re.fullmatch(r"tcp://(?:127\.0\.0\.1|localhost|\[::1\]):[0-9]+", value):
        raise ValueError(f"{name} must use a TCP loopback endpoint")


def _sha256(path: Path, *, normalize_newlines: bool = False) -> str:
    if normalize_newlines:
        # Git checkouts may use CRLF on Windows and LF in the build pipeline.
        # The provenance manifest declares this text-source normalization.
        return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_provenance(asset_root: Path) -> dict[str, object]:
    manifest_path = asset_root / "third_party" / "xtquant_big_convert.UPSTREAM.json"
    try:
        provenance = json.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RuntimeError(f"Big QMT provenance manifest is missing: {manifest_path}") from exc
    if provenance.get("upstream_sha") != EXPECTED_UPSTREAM_SHA:
        raise RuntimeError("Big QMT provenance has an unexpected upstream SHA")
    hashes = provenance.get("file_sha256")
    if not isinstance(hashes, dict) or not hashes:
        raise RuntimeError("Big QMT provenance has no file checksums")
    for name, expected in hashes.items():
        if not isinstance(name, str) or not isinstance(expected, str):
            raise RuntimeError("Big QMT provenance has an invalid checksum entry")
        path = asset_root / name
        if not path.is_file():
            raise RuntimeError(f"Pinned Big QMT file is missing: {path}")
        if _sha256(path, normalize_newlines=provenance.get("hash_normalization") == "lf").lower() != expected.lower():
            raise RuntimeError(f"Pinned Big QMT checksum mismatch: {path}")
    _emit_lifecycle(
        "installer.manifest_verified",
        critical=True,
        outcome="success",
        manifest_sha256=_sha256(manifest_path),
        verified_file_count=len(hashes),
    )
    return provenance


def _copy(source: Path, target: Path) -> None:
    if source.is_dir():
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def _write_config(request: InstallRequest, target: Path) -> None:
    order_methods = "True" if request.order_methods_enabled else "False"
    content = f"""BIGQMT_ACCOUNT_ID = {request.account_id!r}

BIGQMT_REDIS_CONFIG = {{
    'transport': 'zmq',
    'zmq': {{'bind_address': {request.zmq_endpoint!r}, 'redis_discovery_enabled': False}},
    'rpc_allow_order_methods': {order_methods},
    # The receiver thread owns transport only. Every QMT API call, including
    # passorder and ContextInfo reads, is drained from strategy adjust.
    'rpc_process_in_listener': False,
    'rpc_listener_methods': (),
    'rpc_background_threads': True,
    'schedule_adjust': True,
    'schedule_adjust_interval': '200nMilliSecond',
    'full_tick_cache_enabled': False,
    'download_jobs_enabled': False,
    'exec_events_enabled': True,
    'exec_events_transport': 'zmq',
    'exec_events_zmq': {{'bind_address': {request.event_zmq_endpoint!r}, 'maxlen': 2000}},
}}
"""
    target.write_text(content, encoding="utf-8", newline="\n")


def _catalog_with_entry(text: str) -> str:
    import re

    entry = (
        '            <catalog scriptType="1" formulaCatalogModelType="4" '
        'systemProvidedStrategy="0" strategymall="0" name="MECOSTOCK_BIGQMT_ZMQ" '
        'type="2" simpleRun="0"/>'
    )
    text = re.sub(r'\s*<catalog\s+[^>]*name="MECOSTOCK_BIGQMT_BRIDGE"[^>]*/>', "", text, count=1)
    if re.search(r'<catalog\s+[^>]*name="MECOSTOCK_BIGQMT_ZMQ"[^>]*/>', text):
        return re.sub(
            r'<catalog\s+[^>]*name="MECOSTOCK_BIGQMT_ZMQ"[^>]*/>', entry, text, count=1
        )
    parent = (
        r'(<catalog\s+scriptType="1"\s+formulaCatalogModelType="4"\s+'
        r'systemProvidedStrategy="1"\s+strategymall="0"\s+name="我的策略"\s+type="1"\s+simpleRun="0">)'
    )
    if not re.search(parent, text):
        raise RuntimeError("QMT strategy catalog does not contain the expected 我的策略 section")
    newline = "\r\n" if "\r\n" in text else "\n"
    return re.sub(parent, lambda match: match.group(1) + newline + entry, text, count=1)


def _backup_existing(qmt_root: Path, targets: Iterable[Path]) -> None:
    present = [path for path in targets if path.exists()]
    if not present:
        return
    backup_root = qmt_root / "bigqmt_runtime_backups" / datetime.now().strftime("%Y%m%d-%H%M%S")
    _emit_lifecycle("installer.backup.start", outcome="started", target_count=len(present))
    backup_root.mkdir(parents=True)
    for target in present:
        destination = backup_root / target.name
        _copy(target, destination)
    _emit_lifecycle("installer.backup.end", critical=True, outcome="success", target_count=len(present))


def _install_runtime(request: InstallRequest) -> dict[str, object]:
    if not request.account_id.strip():
        raise ValueError("AccountId is required")
    _validate_loopback_endpoint(request.zmq_endpoint, "ZmqEndpoint")
    _validate_loopback_endpoint(request.event_zmq_endpoint, "EventZmqEndpoint")
    if request.zmq_endpoint == request.event_zmq_endpoint:
        raise ValueError("EventZmqEndpoint must differ from ZmqEndpoint")

    qmt_root = request.qmt_root.expanduser().resolve()
    python_root = qmt_root / "python"
    if not python_root.is_dir():
        raise RuntimeError(f"QMT python directory was not found: {python_root}")
    if not (qmt_root / "bin.x64" / "python36.dll").is_file():
        raise RuntimeError(f"QMT embedded Python runtime was not found: {qmt_root / 'bin.x64' / 'python36.dll'}")
    provenance = validate_provenance(request.asset_root)
    _emit_lifecycle(
        "installer.validated",
        outcome="success",
        dry_run=request.dry_run,
        manifest_sha256=_sha256(request.asset_root / "third_party" / "xtquant_big_convert.UPSTREAM.json"),
    )

    package_target = python_root / "bigqmt_signal_trader"
    entry_target = python_root / "MECOSTOCK_BIGQMT_BRIDGE.py"
    runner_target = python_root / "MECOSTOCK_BIGQMT_ZMQ.source.py"
    config_target = python_root / "bigqmt_signal_trader_local_config.py"
    manifest_target = python_root / "MECOSTOCK_BIGQMT_BRIDGE.manifest.json"
    catalog = qmt_root / "config" / "indexUserConfig.xml"
    if not catalog.is_file():
        raise RuntimeError(f"QMT strategy catalog was not found: {catalog}")
    compiled_model = python_root / "MECOSTOCK_BIGQMT_ZMQ.py"
    targets = [
        package_target,
        python_root / "bigqmt_signal_trader_strategy.py",
        python_root / "bigqmt_signal_trader_redis_rpc_runtime.py",
        entry_target,
        runner_target,
        config_target,
        manifest_target,
        catalog,
    ]
    result = {
        "qmt_root": str(qmt_root),
        "python_root": str(python_root),
        "entry": str(entry_target),
        "runner_source": str(runner_target),
        "compiled_model": str(compiled_model),
        "compiled_model_ready": compiled_model.is_file(),
        "config": str(config_target),
        "upstream_sha": provenance["upstream_sha"],
        "transport": "zmq",
        "endpoint": request.zmq_endpoint,
        "event_endpoint": request.event_zmq_endpoint,
        "order_methods_enabled": request.order_methods_enabled,
        "terminal_mode_attestation": True,
        "terminal_mode_attestation_source": "qmt_request_id_and_terminal_log",
        "catalog": str(catalog),
        "embedded_python_mode": compiled_model.is_file(),
        "dry_run": request.dry_run,
    }
    if request.dry_run:
        _emit_lifecycle("installer.install", critical=True, outcome="dry_run")
        return result

    _backup_existing(qmt_root, targets)
    _emit_lifecycle("installer.copy.start", outcome="started")
    _copy(request.asset_root / "src" / "bigqmt_signal_trader", package_target)
    _copy(request.asset_root / "src" / "bigqmt_signal_trader_strategy.py", python_root / "bigqmt_signal_trader_strategy.py")
    _copy(request.asset_root / "src" / "bigqmt_signal_trader_redis_rpc_runtime.py", python_root / "bigqmt_signal_trader_redis_rpc_runtime.py")
    entry = (request.asset_root / "src" / "BIGQMT_REDIS_DRYRUN.py").read_bytes()
    overlay = (request.asset_root / "src" / "mecostock_bigqmt_mode_overlay.py").read_bytes()
    entry_target.write_bytes(entry + b"\n\n" + overlay)
    _copy(request.asset_root / "src" / "MECOSTOCK_BIGQMT_ZMQ.py", runner_target)
    _write_config(request, config_target)
    _emit_lifecycle("installer.copy.end", critical=True, outcome="success")
    manifest_target.write_text(
        json.dumps(
            {
                "schema_version": "mecostock_bigqmt_runtime_v1",
                "upstream_repository": provenance["repository"],
                "upstream_sha": provenance["upstream_sha"],
                "installed_at": datetime.now(timezone.utc).isoformat(),
                "transport": "zmq",
                "endpoint": request.zmq_endpoint,
                "event_endpoint": request.event_zmq_endpoint,
                "order_methods_enabled": request.order_methods_enabled,
                "terminal_mode_attestation": True,
                "terminal_mode_attestation_source": "qmt_request_id_and_terminal_log",
                "terminal_mode_overlay_sha256": _sha256(request.asset_root / "src" / "mecostock_bigqmt_mode_overlay.py"),
                "entry": entry_target.name,
                "runner_source": runner_target.name,
                "compiled_model": compiled_model.name,
            },
            indent=2,
            ensure_ascii=False,
        ) + "\n",
        encoding="utf-8",
    )
    if compiled_model.is_file():
        _emit_lifecycle("installer.catalog.start", outcome="started")
        catalog.write_text(_catalog_with_entry(catalog.read_text(encoding="utf-8")), encoding="utf-8", newline="")
        _emit_lifecycle("installer.catalog.end", critical=True, outcome="success")
    _emit_lifecycle("installer.install", critical=True, outcome="success")
    return result


def install_runtime(request: InstallRequest) -> dict[str, object]:
    """Install while preserving a caller trace or creating a standalone one."""
    telemetry = _telemetry()
    if telemetry is None:
        return _install_runtime(request)
    context = telemetry.current_context()
    trace_id = str(context.get("trace_id") or uuid.uuid4().hex)
    with telemetry.bind_context(trace_id=trace_id):
        with telemetry.span("installer.install", dry_run=request.dry_run) as result:
            try:
                payload = _install_runtime(request)
            except Exception as exc:
                result.update(outcome="error", error_type=type(exc).__name__)
                raise
            result["outcome"] = "dry_run" if request.dry_run else "success"
            return payload


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Install the packaged Big QMT embedded runtime")
    parser.add_argument("--qmt-root", "-QmtRoot", required=True)
    parser.add_argument("--account-id", "-AccountId", required=True)
    parser.add_argument("--source-root", "-SourceRoot", default=None)
    parser.add_argument("--zmq-endpoint", "-ZmqEndpoint", default="tcp://127.0.0.1:15560")
    parser.add_argument("--event-zmq-endpoint", "-EventZmqEndpoint", default="tcp://127.0.0.1:15561")
    parser.add_argument("--order-methods-enabled", "-OrderMethodsEnabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--what-if", "-WhatIf", action="store_true", help="validate only; do not write QMT files")
    args = parser.parse_args(argv)
    telemetry = _telemetry()
    trace_id = os.environ.get("QMT_BRIDGE_TRACE_ID", "") or uuid.uuid4().hex
    if telemetry is not None:
        telemetry.configure(
            enabled=os.environ.get("QMT_BRIDGE_TRACE_ENABLED", "true").strip().lower()
            not in {"0", "false", "no", "off"},
            directory=os.environ.get("QMT_BRIDGE_TRACE_DIR") or None,
            role="installer",
        )
    context = telemetry.bind_context(trace_id=trace_id) if telemetry is not None else None
    try:
        if context is not None:
            context.__enter__()
        _emit_lifecycle("installer.start", critical=True, outcome="started", dry_run=args.what_if)
        payload = install_runtime(
            InstallRequest(
                qmt_root=Path(args.qmt_root),
                account_id=args.account_id,
                asset_root=resolve_asset_root(args.source_root),
                zmq_endpoint=args.zmq_endpoint,
                event_zmq_endpoint=args.event_zmq_endpoint,
                order_methods_enabled=args.order_methods_enabled,
                dry_run=args.what_if,
            )
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    except Exception as exc:
        _emit_lifecycle("installer.install", critical=True, outcome="error", error_type=type(exc).__name__)
        raise
    finally:
        if context is not None:
            context.__exit__(None, None, None)
        if telemetry is not None:
            telemetry.flush(0.5)
            telemetry.shutdown(0.5)


if __name__ == "__main__":
    main()
