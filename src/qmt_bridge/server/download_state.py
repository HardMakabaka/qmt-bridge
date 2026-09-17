"""Small, local persistence for resumable history-download jobs.

This deliberately is not a database: download jobs are host-local QMT work and
the state only protects a bridge restart between native calls.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def _emit(name: str, critical: bool = False, **fields: Any) -> None:
    try:
        from bigqmt_signal_trader.telemetry import emit
        emit(name, critical=critical, **fields)
    except Exception:
        pass


class DownloadStateStore:
    def __init__(self, directory: str | Path | None) -> None:
        self.directory = Path(directory) if directory else None
        self._lock_file = None
        if self.directory is not None:
            self.directory.mkdir(parents=True, exist_ok=True)
            self._acquire_owner_lock()

    def _acquire_owner_lock(self) -> None:
        path = self.directory / ".owner.lock"
        handle = path.open("a+b")
        try:
            handle.seek(0)
            if not handle.read(1):
                handle.seek(0)
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            try:
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except ImportError:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(
                "another qmt-bridge process owns this download state directory"
            ) from exc
        self._lock_file = handle
        _emit("download_state_owner_acquired", critical=True,
              state_dir=str(self.directory))

    def close(self) -> None:
        if self._lock_file is None:
            return
        handle = self._lock_file
        self._lock_file = None
        try:
            handle.seek(0)
            try:
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            except ImportError:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
        _emit("download_state_owner_released", critical=True,
              state_dir=str(self.directory))

    def load(self) -> list[dict[str, Any]]:
        if self.directory is None:
            return []
        jobs: list[dict[str, Any]] = []
        for path in sorted(self.directory.glob("*.json")):
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(value, dict) and isinstance(value.get("job_id"), str):
                    jobs.append(value)
            except (OSError, ValueError, TypeError):
                # One corrupt state file must not prevent the bridge from starting.
                continue
        return jobs

    def save(self, job: dict[str, Any]) -> None:
        if self.directory is None:
            return
        target = self.directory / (str(job["job_id"]) + ".json")
        payload = json.dumps(job, ensure_ascii=False, sort_keys=True, default=str)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=self.directory,
            prefix=target.name + ".", suffix=".tmp", delete=False,
        ) as temporary:
            temporary.write(payload)
            temporary_name = temporary.name
        os.replace(temporary_name, target)
        _emit("download_state_saved", job_id=job.get("job_id"),
              status=job.get("status"))

    def remove(self, job_id: str) -> None:
        if self.directory is None:
            return
        try:
            (self.directory / (job_id + ".json")).unlink()
        except FileNotFoundError:
            pass
