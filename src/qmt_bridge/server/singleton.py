"""Single-instance guard for the Windows-host qmt-server process."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from typing import Any

logger = logging.getLogger("qmt_bridge")


def _truthy(value: str | None, *, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _powershell_json(script: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        if result.stderr.strip():
            logger.warning("qmt-server singleton process scan failed: %s", result.stderr.strip())
        return []
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        logger.warning("qmt-server singleton process scan returned invalid JSON: %s", exc)
        return []
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _current_process_family_pids(current_pid: int) -> set[int]:
    """Return the current process and its ancestors to avoid self-termination."""
    protected_pids = {current_pid}
    try:
        protected_pids.add(os.getppid())
    except Exception:
        pass
    if os.name != "nt":
        return protected_pids

    process_query = """
Get-CimInstance Win32_Process |
  Select-Object ProcessId, ParentProcessId |
  ConvertTo-Json -Compress
"""
    processes = _powershell_json(process_query)
    parents: dict[int, int] = {}
    for process in processes:
        try:
            pid = int(process.get("ProcessId") or 0)
            parent_pid = int(process.get("ParentProcessId") or 0)
        except Exception:
            continue
        if pid > 0 and parent_pid > 0:
            parents[pid] = parent_pid

    pid = current_pid
    visited = set()
    for _ in range(32):
        if pid in visited:
            break
        visited.add(pid)
        parent_pid = parents.get(pid)
        if not parent_pid:
            break
        if parent_pid not in protected_pids:
            protected_pids.add(parent_pid)
        pid = parent_pid
    return protected_pids


def stop_existing_qmt_servers(*, port: int, enabled: bool | None = None) -> list[dict[str, Any]]:
    """Stop older qmt-server processes before binding the fixed bridge port.

    The bridge runs beside an interactive QMT GUI process. This guard only
    targets process command lines that look like qmt-server or
    qmt_bridge.server.cli; it never targets XtMiniQmt.exe.
    """
    if enabled is None:
        enabled = _truthy(os.environ.get("QMT_BRIDGE_SINGLETON"), default=True)
    if not enabled:
        return []
    if os.name != "nt":
        logger.info("qmt-server singleton guard skipped on non-Windows platform")
        return []

    current_pid = os.getpid()
    protected_pids = _current_process_family_pids(current_pid)
    process_query = rf"""
$CurrentPid = {current_pid}
Get-CimInstance Win32_Process |
  Where-Object {{
    $_.ProcessId -ne $CurrentPid -and
    $_.CommandLine -and
    ($_.CommandLine -match 'qmt-server' -or $_.CommandLine -match 'qmt_bridge\.server\.cli')
  }} |
  Select-Object ProcessId, Name, CommandLine |
  ConvertTo-Json -Compress
"""
    candidates = _powershell_json(process_query)
    stopped: list[dict[str, Any]] = []
    for candidate in candidates:
        pid = int(candidate.get("ProcessId") or 0)
        if pid <= 0 or pid in protected_pids:
            continue
        command_line = str(candidate.get("CommandLine") or "")
        # Keep the guard narrowly scoped to bridge processes; the fixed port is
        # logged for audit even when an old bridge still runs on a previous port.
        logger.warning(
            "Stopping existing qmt-server process before binding port %s: pid=%s",
            port,
            pid,
        )
        stop = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    f"Stop-Process -Id {pid} -Force; "
                    f"Wait-Process -Id {pid} -Timeout 10 -ErrorAction SilentlyContinue"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        stopped.append(
            {
                "pid": pid,
                "name": candidate.get("Name"),
                "command_line": command_line,
                "stopped": stop.returncode == 0,
                "error": stop.stderr.strip(),
            }
        )
    return stopped
