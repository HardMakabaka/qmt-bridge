"""Router — Data download endpoints /api/download/*."""

from __future__ import annotations

import logging
import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from queue import Queue
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from xtquant import xtdata

from ..helpers import _numpy_to_python
from ..models import (
    FinancialDownload2Request,
    FinancialDownloadRequest,
    HistoryDownloadJobRequest,
    SectorDownloadRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/download", tags=["download"])

DOWNLOAD_JOB_TTL_SECONDS = 24 * 60 * 60
DOWNLOAD_BATCH_TIMEOUT_SECONDS = 120.0
DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS = 60.0
DOWNLOAD_STOP_GRACE_SECONDS = 5.0
SECTOR_DOWNLOAD_TIMEOUT_SECONDS = 60.0
SECTOR_DOWNLOAD_STOP_GRACE_SECONDS = 5.0
SECTOR_DOWNLOAD_PERIOD = (2009, 86400000)
SECTOR_DOWNLOAD_USE_SUBPROCESS = True
SECTOR_DOWNLOAD_METHOD = "xtdata.download_sector_data"


class _DownloadCallTimeout(RuntimeError):
    pass


def _frame_has_usable_rows(frame) -> bool:
    if frame is None or bool(getattr(frame, "empty", True)):
        return False
    raw_columns = getattr(frame, "columns", [])
    columns = list(raw_columns) if raw_columns is not None else []
    price_columns = [column for column in ("open", "high", "low", "close") if column in columns]
    if not price_columns:
        return True
    try:
        return bool(frame[price_columns].notna().any(axis=1).any())
    except Exception:
        logger.debug("Unable to inspect downloaded history frame", exc_info=True)
        return True


class _DownloadJobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, dict] = {}
        self._queue: deque[str] = deque()
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._worker: threading.Thread | None = None

    def submit(self, req: HistoryDownloadJobRequest) -> dict:
        stocks = [stock.strip() for stock in req.stocks if str(stock).strip()]
        if not stocks:
            raise HTTPException(status_code=400, detail="stocks must not be empty")
        batch_size = max(1, int(req.batch_size or 10))
        max_attempts = max(1, int(req.max_attempts or 2))
        job_id = uuid4().hex
        now = time.time()
        job = {
            "job_id": job_id,
            "status": "queued",
            "stocks": stocks,
            "period": req.period,
            "start_time": req.start_time,
            "end_time": req.end_time,
            "batch_size": batch_size,
            "max_attempts": max_attempts,
            "total": len(stocks),
            "processed": 0,
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "elapsed_seconds": 0.0,
            "current_batch": [],
            "symbol_results": {},
            "slow_symbols": [],
            "failed_symbols": [],
            "last_progress": None,
            "stop_requested": False,
            "error": None,
        }
        with self._condition:
            self._gc_locked()
            self._jobs[job_id] = job
            self._queue.append(job_id)
            self._ensure_worker_locked()
            self._condition.notify()
        logger.info(
            "Queued history download job: job_id=%s stocks=%s period=%s start=%s end=%s batch_size=%s",
            job_id,
            len(stocks),
            req.period,
            req.start_time,
            req.end_time,
            batch_size,
        )
        return self.get(job_id)

    def get(self, job_id: str) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise HTTPException(status_code=404, detail="Download job not found")
            return self._snapshot_locked(job)

    def cancel(self, job_id: str) -> dict:
        with self._condition:
            job = self._jobs.get(job_id)
            if job is None:
                raise HTTPException(status_code=404, detail="Download job not found")
            job["stop_requested"] = True
            if job["status"] == "queued":
                try:
                    self._queue.remove(job_id)
                except ValueError:
                    pass
                self._finish_locked(job, "canceled")
            self._stop_xtdata_download()
            self._condition.notify_all()
            return self._snapshot_locked(job)

    def reset_for_tests(self) -> None:
        with self._condition:
            self._jobs = {}
            self._queue = deque()
            if self._worker is not None and not self._worker.is_alive():
                self._worker = None
            self._condition.notify_all()

    def _ensure_worker_locked(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        self._worker = threading.Thread(
            target=self._worker_loop,
            name="qmt-history-download-jobs",
            daemon=True,
        )
        self._worker.start()

    def _worker_loop(self) -> None:
        while True:
            with self._condition:
                while not self._queue:
                    self._condition.wait()
                job_id = self._queue.popleft()
                job = self._jobs.get(job_id)
            if job is None:
                continue
            try:
                self._run_job(job_id)
            except Exception as exc:
                logger.exception("History download job crashed: job_id=%s", job_id)
                with self._condition:
                    job = self._jobs.get(job_id)
                    if job is not None:
                        job["error"] = str(exc)
                        self._finish_locked(job, "failed")
                        self._condition.notify_all()

    def _run_job(self, job_id: str) -> None:
        with self._condition:
            job = self._jobs[job_id]
            job["status"] = "running"
            job["started_at"] = time.time()
            self._condition.notify_all()

        stocks = list(job["stocks"])
        batch_size = int(job["batch_size"])
        logger.info("Started history download job: job_id=%s total=%s", job_id, len(stocks))

        for offset in range(0, len(stocks), batch_size):
            batch = stocks[offset : offset + batch_size]
            if self._is_stop_requested(job_id):
                self._mark_canceled(job_id)
                return
            with self._condition:
                job = self._jobs[job_id]
                job["current_batch"] = batch
                self._condition.notify_all()

            batch_status, batch_error = self._run_batch(job_id, batch, attempt=1)
            if batch_status == "ok":
                self._mark_symbols(job_id, batch, status="ok", error="")
                continue
            if batch_status == "target_missing":
                self._mark_symbols(job_id, batch, status=batch_status, error=batch_error)
                continue
            if self._is_stop_requested(job_id):
                self._mark_canceled(job_id)
                return

            self._mark_slow(job_id, batch, batch_error)
            if len(batch) == 1:
                self._retry_symbol(
                    job_id,
                    batch[0],
                    first_status=batch_status,
                    first_error=batch_error,
                    next_attempt=2,
                )
                continue

            for stock in batch:
                if self._is_stop_requested(job_id):
                    self._mark_canceled(job_id)
                    return
                self._retry_symbol(job_id, stock)

        with self._condition:
            job = self._jobs[job_id]
            job["current_batch"] = []
            failed_count = len(job.get("failed_symbols") or [])
            total = int(job.get("total") or 0)
            if total > 0 and failed_count >= total:
                job["error"] = "all requested symbols failed history download validation"
                final_status = "failed"
            else:
                final_status = "completed"
            self._finish_locked(job, final_status)
            self._condition.notify_all()
        logger.info(
            "Completed history download job: job_id=%s processed=%s failed=%s slow=%s elapsed=%.2fs",
            job_id,
            self.get(job_id)["processed"],
            len(self.get(job_id)["failed_symbols"]),
            len(self.get(job_id)["slow_symbols"]),
            self.get(job_id)["elapsed_seconds"],
        )

    def _retry_symbol(
        self,
        job_id: str,
        stock: str,
        *,
        first_status: str = "error",
        first_error: str = "",
        next_attempt: int = 1,
    ) -> None:
        max_attempts = int(self.get(job_id)["max_attempts"])
        last_status = first_status
        last_error = first_error
        for attempt in range(max(1, next_attempt), max_attempts + 1):
            status, error = self._run_batch(job_id, [stock], attempt=attempt)
            last_status, last_error = status, error
            if status == "ok":
                self._mark_symbols(job_id, [stock], status="ok", error="")
                return
            if self._is_stop_requested(job_id):
                return
        self._mark_symbols(job_id, [stock], status=last_status, error=last_error)

    def _run_batch(self, job_id: str, batch: list[str], *, attempt: int) -> tuple[str, str]:
        started = time.time()
        last_progress_at = started
        result_queue: Queue = Queue(maxsize=1)

        def on_progress(data):
            nonlocal last_progress_at
            last_progress_at = time.time()
            clean = _numpy_to_python(data)
            with self._condition:
                job = self._jobs.get(job_id)
                if job is not None:
                    job["last_progress"] = clean
                    self._condition.notify_all()

        def target() -> None:
            try:
                result = xtdata.download_history_data2(
                    batch,
                    period=self.get(job_id)["period"],
                    start_time=self.get(job_id)["start_time"],
                    end_time=self.get(job_id)["end_time"],
                    callback=on_progress,
                )
                result_queue.put(("ok", result))
            except Exception as exc:
                result_queue.put(("error", exc))

        thread = threading.Thread(
            target=target,
            name=f"qmt-history-download-{job_id[:8]}",
            daemon=True,
        )
        thread.start()

        try:
            while thread.is_alive():
                if self._is_stop_requested(job_id):
                    self._stop_xtdata_download()
                    thread.join(DOWNLOAD_STOP_GRACE_SECONDS)
                    raise _DownloadCallTimeout("download canceled")
                now = time.time()
                if now - started > DOWNLOAD_BATCH_TIMEOUT_SECONDS:
                    self._stop_xtdata_download()
                    thread.join(DOWNLOAD_STOP_GRACE_SECONDS)
                    raise _DownloadCallTimeout(
                        f"batch timeout after {DOWNLOAD_BATCH_TIMEOUT_SECONDS:.0f}s"
                    )
                if now - last_progress_at > DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS:
                    self._stop_xtdata_download()
                    thread.join(DOWNLOAD_STOP_GRACE_SECONDS)
                    raise _DownloadCallTimeout(
                        f"no progress for {DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS:.0f}s"
                    )
                thread.join(0.1)
        except _DownloadCallTimeout as exc:
            logger.warning(
                "History download batch timed out: job_id=%s stocks=%s attempt=%s error=%s",
                job_id,
                batch,
                attempt,
                exc,
            )
            return "download_timeout", str(exc)

        status, payload = result_queue.get() if not result_queue.empty() else ("ok", None)
        elapsed = time.time() - started
        if status == "ok":
            missing_symbols = self._missing_downloaded_symbols(job_id, batch)
            if missing_symbols:
                message = (
                    "download completed but target history rows are missing for "
                    + ",".join(missing_symbols)
                )
                logger.warning(
                    "History download validation failed: job_id=%s stocks=%s attempt=%s error=%s",
                    job_id,
                    missing_symbols,
                    attempt,
                    message,
                )
                return "target_missing", message
            logger.info(
                "History download batch completed: job_id=%s count=%s attempt=%s elapsed=%.2fs",
                job_id,
                len(batch),
                attempt,
                elapsed,
            )
            return "ok", ""
        logger.warning(
            "History download batch failed: job_id=%s stocks=%s attempt=%s error=%s",
            job_id,
            batch,
            attempt,
            payload,
        )
        return "error", str(payload)

    def _missing_downloaded_symbols(self, job_id: str, stocks: list[str]) -> list[str]:
        getter = getattr(xtdata, "get_market_data_ex", None)
        if not callable(getter):
            return []
        job = self.get(job_id)
        try:
            raw = getter(
                field_list=[],
                stock_list=stocks,
                period=job.get("period") or "1d",
                start_time=job.get("start_time") or "",
                end_time=job.get("end_time") or "",
                count=-1,
                dividend_type="none",
                fill_data=True,
            )
        except Exception:
            logger.warning(
                "History download validation read failed: job_id=%s stocks=%s",
                job_id,
                stocks,
                exc_info=True,
            )
            return list(stocks)
        if not isinstance(raw, dict):
            return list(stocks)
        return [stock for stock in stocks if not _frame_has_usable_rows(raw.get(stock))]

    def _mark_symbols(self, job_id: str, stocks: list[str], *, status: str, error: str) -> None:
        now = time.time()
        with self._condition:
            job = self._jobs[job_id]
            for stock in stocks:
                existing = job["symbol_results"].get(stock)
                if existing and existing.get("status") == "ok":
                    continue
                job["symbol_results"][stock] = {
                    "stock": stock,
                    "status": status,
                    "error": error,
                    "finished_at": now,
                }
                if status != "ok" and stock not in job["failed_symbols"]:
                    job["failed_symbols"].append(stock)
            job["processed"] = len(job["symbol_results"])
            self._condition.notify_all()

    def _mark_slow(self, job_id: str, stocks: list[str], error: str) -> None:
        with self._condition:
            job = self._jobs[job_id]
            for stock in stocks:
                if stock not in job["slow_symbols"]:
                    job["slow_symbols"].append(stock)
            job["last_slow_error"] = error
            self._condition.notify_all()

    def _mark_canceled(self, job_id: str) -> None:
        with self._condition:
            job = self._jobs[job_id]
            self._finish_locked(job, "canceled")
            self._condition.notify_all()

    def _is_stop_requested(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job.get("stop_requested"))

    def _finish_locked(self, job: dict, status: str) -> None:
        now = time.time()
        job["status"] = status
        job["finished_at"] = now
        started = job.get("started_at") or job.get("created_at") or now
        job["elapsed_seconds"] = max(0.0, now - float(started))

    def _snapshot_locked(self, job: dict) -> dict:
        now = time.time()
        snapshot = {
            key: _numpy_to_python(value)
            for key, value in job.items()
            if key not in {"created_at", "started_at", "finished_at"}
        }
        snapshot["created_at"] = job.get("created_at")
        snapshot["started_at"] = job.get("started_at")
        snapshot["finished_at"] = job.get("finished_at")
        if job.get("started_at") and not job.get("finished_at"):
            snapshot["elapsed_seconds"] = max(0.0, now - float(job["started_at"]))
        return snapshot

    def _gc_locked(self) -> None:
        now = time.time()
        expired = [
            job_id
            for job_id, job in self._jobs.items()
            if now - float(job.get("created_at") or now) > DOWNLOAD_JOB_TTL_SECONDS
        ]
        for job_id in expired:
            self._jobs.pop(job_id, None)
            try:
                self._queue.remove(job_id)
            except ValueError:
                pass

    @staticmethod
    def _stop_xtdata_download() -> None:
        try:
            client = xtdata.get_client()
            stop = getattr(client, "stop_supply_history_data2", None)
            if callable(stop):
                stop()
                return
        except Exception:
            logger.debug("xtdata.get_client().stop_supply_history_data2 failed", exc_info=True)
        stop = getattr(xtdata, "stop_supply_history_data2", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                logger.debug("xtdata.stop_supply_history_data2 failed", exc_info=True)


_download_jobs = _DownloadJobManager()
_sector_download_lock = threading.RLock()
_sector_download_active: dict[str, object] = {
    "thread": None,
    "running": False,
    "started_at": None,
    "timeout_seconds": None,
    "last_progress": None,
    "method": None,
}


def reset_download_job_manager_for_tests() -> None:
    _download_jobs.reset_for_tests()
    with _sector_download_lock:
        _sector_download_active.update(
            {
                "thread": None,
                "running": False,
                "started_at": None,
                "timeout_seconds": None,
                "last_progress": None,
                "method": None,
            }
        )


def _safe_sector_count() -> int | None:
    try:
        return len(xtdata.get_sector_list() or [])
    except Exception:
        logger.debug("Unable to read QMT sector list during sector download", exc_info=True)
        return None


def _tail_text(value: str | bytes | None, limit: int = 4000) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value[-limit:]


def _sector_download_child_code() -> str:
    return r"""
import json
import time
import traceback

from xtquant import xtdata


def _json_default(value):
    try:
        return str(value)
    except Exception:
        return repr(value)


started = time.time()
payload = {
    "status": "ok",
    "method": "xtdata.download_sector_data",
    "elapsed_seconds": 0.0,
    "result": None,
}
try:
    payload["result"] = xtdata.download_sector_data()
except Exception as exc:
    payload.update(
        {
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(limit=5),
        }
    )
finally:
    payload["elapsed_seconds"] = round(time.time() - started, 3)
    print(json.dumps(payload, ensure_ascii=False, default=_json_default), flush=True)
"""


def _sector_download_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    current_pythonpath = env.get("PYTHONPATH", "")
    path_entries = [entry for entry in sys.path if entry]
    if current_pythonpath:
        path_entries.append(current_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(path_entries))
    return env


def _parse_sector_download_payload(stdout: str) -> dict | None:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _execute_sector_download_process(timeout_seconds: float) -> tuple[str, dict]:
    started = time.time()
    command = [sys.executable, "-u", "-c", _sector_download_child_code()]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_sector_download_env(),
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return (
            "timeout",
            {
                "method": SECTOR_DOWNLOAD_METHOD,
                "elapsed_seconds": round(time.time() - started, 3),
                "timeout_seconds": timeout_seconds,
                "stdout_tail": _tail_text(exc.stdout),
                "stderr_tail": _tail_text(exc.stderr),
                "child_process_isolated": True,
                "child_process_killed": True,
            },
        )

    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    payload = _parse_sector_download_payload(stdout) or {
        "method": SECTOR_DOWNLOAD_METHOD,
        "result": None,
    }
    payload.setdefault("method", SECTOR_DOWNLOAD_METHOD)
    payload["returncode"] = completed.returncode
    payload["stdout_tail"] = _tail_text(stdout)
    payload["stderr_tail"] = _tail_text(stderr)
    payload["child_process_isolated"] = True
    payload["child_process_killed"] = False
    payload.setdefault("elapsed_seconds", round(time.time() - started, 3))
    if completed.returncode != 0:
        payload.setdefault("error", f"sector download child exited with {completed.returncode}")
        return "error", payload
    status = str(payload.get("status") or "ok")
    if status == "ok":
        return "ok", payload
    if status == "timeout":
        return "timeout", payload
    return "error", payload


def _execute_sector_download_in_process(on_progress) -> dict:
    downloader = getattr(xtdata, "download_history_data2", None)
    if callable(downloader):
        result = downloader([], SECTOR_DOWNLOAD_PERIOD, callback=on_progress)
        return {
            "method": "xtdata.download_history_data2",
            "result": result,
            "child_process_isolated": False,
        }
    return {
        "method": "xtdata.download_sector_data",
        "result": xtdata.download_sector_data(),
        "child_process_isolated": False,
    }


def _run_sector_download(timeout_seconds: float = SECTOR_DOWNLOAD_TIMEOUT_SECONDS) -> dict:
    started = time.time()
    timeout = min(600.0, max(0.1, float(timeout_seconds or SECTOR_DOWNLOAD_TIMEOUT_SECONDS)))
    with _sector_download_lock:
        active_thread = _sector_download_active.get("thread")
        if bool(_sector_download_active.get("running")):
            active_started = float(_sector_download_active.get("started_at") or started)
            return {
                "status": "busy",
                "reason": "qmt_sector_download_already_running",
                "elapsed_seconds": round(time.time() - active_started, 3),
                "timeout_seconds": _sector_download_active.get("timeout_seconds"),
                "last_progress": _sector_download_active.get("last_progress"),
                "method": _sector_download_active.get("method"),
                "thread_alive": isinstance(active_thread, threading.Thread)
                and active_thread.is_alive(),
            }
        if isinstance(active_thread, threading.Thread):
            if active_thread.is_alive():
                active_started = float(_sector_download_active.get("started_at") or started)
                return {
                    "status": "busy",
                    "reason": "qmt_sector_download_already_running",
                    "elapsed_seconds": round(time.time() - active_started, 3),
                    "timeout_seconds": _sector_download_active.get("timeout_seconds"),
                    "last_progress": _sector_download_active.get("last_progress"),
                    "method": _sector_download_active.get("method"),
                    "thread_alive": True,
                }
            _sector_download_active.update(
                {
                    "thread": None,
                    "running": False,
                    "started_at": None,
                    "timeout_seconds": None,
                    "last_progress": None,
                    "method": None,
                }
            )

    before_count = _safe_sector_count()
    result_queue: Queue = Queue(maxsize=1)
    progress: dict[str, object] = {"last": None}

    def on_progress(data):
        clean = _numpy_to_python(data)
        progress["last"] = clean
        with _sector_download_lock:
            if _sector_download_active.get("thread") is thread:
                _sector_download_active["last_progress"] = clean

    def target() -> None:
        try:
            if SECTOR_DOWNLOAD_USE_SUBPROCESS:
                status, payload = _execute_sector_download_process(timeout)
                result_queue.put((status, payload))
            else:
                result_queue.put(("ok", _execute_sector_download_in_process(on_progress)))
        except Exception as exc:
            result_queue.put(("error", exc))
        finally:
            with _sector_download_lock:
                if _sector_download_active.get("thread") is thread:
                    _sector_download_active.update(
                        {
                            "running": False,
                        }
                    )

    thread = threading.Thread(
        target=target,
        name="qmt-sector-data-download",
        daemon=True,
    )

    with _sector_download_lock:
        _sector_download_active.update(
            {
                "thread": thread,
                "running": True,
                "started_at": started,
                "timeout_seconds": timeout,
                "last_progress": None,
                "method": SECTOR_DOWNLOAD_METHOD
                if SECTOR_DOWNLOAD_USE_SUBPROCESS
                else "xtdata.download_history_data2",
            }
        )
    thread.start()

    while thread.is_alive():
        elapsed = time.time() - started
        guard_timeout = timeout
        if SECTOR_DOWNLOAD_USE_SUBPROCESS:
            guard_timeout += SECTOR_DOWNLOAD_STOP_GRACE_SECONDS
        if elapsed > guard_timeout:
            _DownloadJobManager._stop_xtdata_download()
            thread.join(SECTOR_DOWNLOAD_STOP_GRACE_SECONDS)
            after_count = _safe_sector_count()
            thread_alive = thread.is_alive()
            active_method = _sector_download_active.get("method")
            if not thread_alive:
                with _sector_download_lock:
                    if _sector_download_active.get("thread") is thread:
                        _sector_download_active.update(
                            {
                                "thread": None,
                                "running": False,
                                "started_at": None,
                                "timeout_seconds": None,
                                "last_progress": None,
                                "method": None,
                            }
                        )
            return {
                "status": "timeout",
                "reason": "qmt_sector_download_timeout",
                "elapsed_seconds": round(time.time() - started, 3),
                "timeout_seconds": timeout,
                "before_sector_count": before_count,
                "after_sector_count": after_count,
                "last_progress": progress["last"],
                "method": active_method,
                "child_process_isolated": SECTOR_DOWNLOAD_USE_SUBPROCESS,
                "child_process_killed": False,
                "thread_alive": thread_alive,
            }
        thread.join(0.1)

    status, payload = result_queue.get() if not result_queue.empty() else ("ok", None)
    after_count = _safe_sector_count()
    elapsed = round(time.time() - started, 3)
    with _sector_download_lock:
        if _sector_download_active.get("thread") is thread:
            _sector_download_active.update(
                {
                    "thread": None,
                    "running": False,
                    "started_at": None,
                    "timeout_seconds": None,
                    "last_progress": None,
                    "method": None,
                }
            )
    if status == "ok":
        method = payload.get("method") if isinstance(payload, dict) else None
        result = payload.get("result") if isinstance(payload, dict) else payload
        return {
            "status": "ok",
            "elapsed_seconds": elapsed,
            "before_sector_count": before_count,
            "after_sector_count": after_count,
            "last_progress": progress["last"],
            "method": method,
            "child_process_isolated": bool(
                isinstance(payload, dict) and payload.get("child_process_isolated")
            ),
            "child_process_killed": bool(
                isinstance(payload, dict) and payload.get("child_process_killed")
            ),
            "result": _numpy_to_python(result),
        }
    if status == "timeout":
        payload_dict = payload if isinstance(payload, dict) else {"error": str(payload)}
        return {
            "status": "timeout",
            "reason": "qmt_sector_download_timeout",
            "elapsed_seconds": elapsed,
            "timeout_seconds": timeout,
            "before_sector_count": before_count,
            "after_sector_count": after_count,
            "last_progress": progress["last"],
            "method": payload_dict.get("method"),
            "child_process_isolated": bool(payload_dict.get("child_process_isolated")),
            "child_process_killed": bool(payload_dict.get("child_process_killed")),
            "stdout_tail": payload_dict.get("stdout_tail", ""),
            "stderr_tail": payload_dict.get("stderr_tail", ""),
            "thread_alive": thread.is_alive(),
        }
    payload_dict = payload if isinstance(payload, dict) else {"error": str(payload)}
    return {
        "status": "error",
        "reason": "qmt_sector_download_error",
        "elapsed_seconds": elapsed,
        "before_sector_count": before_count,
        "after_sector_count": after_count,
        "last_progress": progress["last"],
        "method": payload_dict.get("method"),
        "child_process_isolated": bool(payload_dict.get("child_process_isolated")),
        "child_process_killed": bool(payload_dict.get("child_process_killed")),
        "stdout_tail": payload_dict.get("stdout_tail", ""),
        "stderr_tail": payload_dict.get("stderr_tail", ""),
        "error": str(payload_dict.get("error", payload)),
    }


@router.post("/jobs")
def create_history_download_job(req: HistoryDownloadJobRequest):
    return _download_jobs.submit(req)


@router.get("/jobs/{job_id}")
def get_history_download_job(job_id: str):
    return _download_jobs.get(job_id)


@router.post("/jobs/{job_id}/cancel")
def cancel_history_download_job(job_id: str):
    return _download_jobs.cancel(job_id)


@router.post("/financial")
def download_financial(req: FinancialDownloadRequest):
    xtdata.download_financial_data(
        req.stocks,
        table_list=req.tables,
        start_time=req.start_time,
        end_time=req.end_time,
    )
    return {"status": "ok", "stocks": req.stocks, "tables": req.tables}


@router.post("/sector_data")
def download_sector_data(
    req: SectorDownloadRequest | None = None,
    timeout_seconds: float | None = None,
):
    effective_timeout = timeout_seconds
    if req is not None and req.timeout_seconds is not None:
        effective_timeout = req.timeout_seconds
    if effective_timeout is None:
        effective_timeout = SECTOR_DOWNLOAD_TIMEOUT_SECONDS
    return _run_sector_download(timeout_seconds=effective_timeout)


@router.post("/index_weight")
def download_index_weight():
    xtdata.download_index_weight()
    return {"status": "ok"}


@router.post("/etf_info")
def download_etf_info():
    xtdata.download_etf_info()
    return {"status": "ok"}


@router.post("/cb_data")
def download_cb_data():
    xtdata.download_cb_data()
    return {"status": "ok"}


@router.post("/history_contracts")
def download_history_contracts():
    xtdata.download_history_contracts()
    return {"status": "ok"}


@router.post("/ipo_data")
def download_ipo_data():
    """Trigger IPO data download."""
    xtdata.download_ipo_data()
    return {"status": "ok"}


@router.post("/option_data")
def download_option_data():
    """Trigger option data download."""
    xtdata.download_option_data()
    return {"status": "ok"}


@router.post("/financial2")
def download_financial_data2(req: FinancialDownload2Request):
    """Synchronous financial data download (blocks until complete)."""
    xtdata.download_financial_data2(
        req.stocks,
        table_list=req.tables,
    )
    return {"status": "ok", "stocks": req.stocks, "tables": req.tables}


@router.post("/holiday")
def download_holiday_data():
    """Download holiday calendar data."""
    xtdata.download_holiday_data()
    return {"status": "ok"}
