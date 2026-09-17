"""Router — Data download endpoints /api/download/*."""

from __future__ import annotations

import logging
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from collections import deque
from queue import Queue
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException
from bigqmt_signal_trader.telemetry import bind_context, current_context, emit
from ..bigqmt import market_data
from ..download_state import DownloadStateStore

from ..helpers import (
    XtdataCallCancelledError,
    XtdataTransportStuckError,
    _call_xtdata_serialized,
    _call_xtdata_serialized_cancellable,
    _mark_xtdata_transport_stuck,
    _numpy_to_python,
    _reset_xtdata_transport_for_tests,
)
from ..models import (
    FinancialDownload2Request,
    FinancialDownloadRequest,
    HistoryDownloadJobRequest,
    SectorDownloadRequest,
)


def _emit_download(name: str, critical: bool = False, **fields) -> None:
    emit(name, critical=critical, **fields)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/download", tags=["download"])

DOWNLOAD_JOB_TTL_SECONDS = 24 * 60 * 60
DOWNLOAD_MAX_BATCH_SIZE = 100
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_BATCH_TIMEOUT_SECONDS = 120.0
DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS = 60.0
DOWNLOAD_STOP_GRACE_SECONDS = 5.0
DOWNLOAD_HISTORY_VISIBILITY_ATTEMPTS = 10
DOWNLOAD_HISTORY_VISIBILITY_INTERVAL_SECONDS = 0.1
SECTOR_DOWNLOAD_TIMEOUT_SECONDS = 60.0
SECTOR_DOWNLOAD_STOP_GRACE_SECONDS = 5.0
SECTOR_DOWNLOAD_PERIOD = (2009, 86400000)
SECTOR_DOWNLOAD_USE_SUBPROCESS = True
SECTOR_DOWNLOAD_METHOD = "xtdata.download_sector_data"


def _default_download_state_dir() -> str:
    configured = os.getenv("QMT_BRIDGE_DOWNLOAD_STATE_DIR")
    if configured:
        return configured
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return os.path.join(local_app_data, "qmt-bridge", "download-jobs")
    return os.path.join(os.path.expanduser("~"), ".qmt-bridge", "download-jobs")


def _in_shanghai_a_share_session(now: float | None = None) -> bool:
    """Return whether a new heavy native download should yield to the market."""
    current = datetime.fromtimestamp(
        time.time() if now is None else now, tz=ZoneInfo("Asia/Shanghai")
    )
    if current.weekday() >= 5:
        return False
    minute = current.hour * 60 + current.minute
    return 9 * 60 + 30 <= minute < 11 * 60 + 30 or 13 * 60 <= minute < 15 * 60


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
    def __init__(
        self,
        *,
        state_dir: str | None = None,
        market_session=_in_shanghai_a_share_session,
        defer_market_hours: bool | None = None,
    ) -> None:
        self._jobs: dict[str, dict] = {}
        self._queue: deque[str] = deque()
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._worker: threading.Thread | None = None
        self._closed = False
        self._native_active_thread: threading.Thread | None = None
        self._state = DownloadStateStore(_default_download_state_dir() if state_dir is None else state_dir)
        self._market_session = market_session
        self._defer_market_hours = (
            os.getenv("QMT_BRIDGE_ALLOW_MARKET_HOURS_DOWNLOAD", "").lower()
            not in {"1", "true", "yes"}
            if defer_market_hours is None
            else defer_market_hours
        )
        with self._condition:
            self._restore_locked()
            if self._queue:
                self._ensure_worker_locked()

    def submit(self, req: HistoryDownloadJobRequest) -> dict:
        stocks = list(dict.fromkeys(stock.strip() for stock in req.stocks if str(stock).strip()))
        if not stocks:
            raise HTTPException(status_code=400, detail="stocks must not be empty")
        batch_size = min(DOWNLOAD_MAX_BATCH_SIZE, max(1, int(req.batch_size or 10)))
        max_attempts = min(DOWNLOAD_MAX_ATTEMPTS, max(1, int(req.max_attempts or 2)))
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
            "download_invoked_symbols": [],
            "download_invocation_completed": False,
            "history_visibility_verified": False,
            "coverage_verified": False,
            "native_inflight_symbols": [],
            "trace_context": current_context(),
        }
        with self._condition:
            self._gc_locked()
            self._jobs[job_id] = job
            self._queue.append(job_id)
            self._persist_locked(job)
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
        _emit_download("download_job_submitted", critical=True, job_id=job_id,
                       total=len(stocks), period=req.period)
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
            _emit_download("download_job_cancel_requested", critical=True, job_id=job_id,
                           status=job.get("status"))
            if job["status"] == "queued":
                try:
                    self._queue.remove(job_id)
                except ValueError:
                    pass
                self._finish_locked(job, "canceled")
            self._persist_locked(job)
            self._condition.notify_all()
            return self._snapshot_locked(job)

    def reset_for_tests(self) -> None:
        with self._condition:
            self._jobs = {}
            self._queue = deque()
            # The in-process singleton is reset by unit tests; do not make their
            # outcome depend on the host clock or write persistent test state.
            self._defer_market_hours = False
            self._state.close()
            self._state = DownloadStateStore(None)
            if self._worker is not None and not self._worker.is_alive():
                self._worker = None
            self._condition.notify_all()

    def close(self) -> None:
        """Release the local state-dir ownership after the native lane is idle."""
        with self._condition:
            active = self._native_active_thread
            if active is not None and active.is_alive():
                raise RuntimeError("cannot close while a native download is active")
            self._closed = True
            self._condition.notify_all()
            worker = self._worker
        if worker is not None:
            worker.join(timeout=2.0)
        if worker is not None and worker.is_alive():
            raise RuntimeError("cannot close while a download worker is active")
        self._state.close()

    def _ensure_worker_locked(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        self._worker = threading.Thread(
            target=self._worker_loop,
            name="qmt-history-download-jobs",
            daemon=True,
        )
        self._worker.start()

    def _persist_locked(self, job: dict) -> None:
        self._state.save(_numpy_to_python(job))
        _emit_download("download_job_persisted", job_id=job.get("job_id"),
                       status=job.get("status"), processed=job.get("processed"),
                       total=job.get("total"))

    def _restore_locked(self) -> None:
        """Restore only work whose last native invocation is known not to be in flight."""
        now = time.time()
        for job in self._state.load():
            if now - float(job.get("created_at") or now) > DOWNLOAD_JOB_TTL_SECONDS:
                self._state.remove(str(job["job_id"]))
                continue
            status = str(job.get("status") or "queued")
            if job.get("native_inflight_symbols"):
                # A native call could have succeeded after the bridge died.  It
                # must be reconciled against QMT data by an operator, never retried.
                job["status"] = "requires_reconciliation"
                job["error"] = "native download outcome unknown after bridge restart"
                job["finished_at"] = now
                self._jobs[str(job["job_id"])] = job
                self._persist_locked(job)
                _emit_download("download_job_restore_unknown", critical=True,
                               job_id=job.get("job_id"))
                continue
            if status not in {"queued", "running", "paused_market_hours"}:
                self._jobs[str(job["job_id"])] = job
                continue
            job["status"] = "queued"
            job["started_at"] = None
            self._jobs[str(job["job_id"])] = job
            self._queue.append(str(job["job_id"]))
            _emit_download("download_job_restored", critical=True, job_id=job.get("job_id"))

    def _worker_loop(self) -> None:
        while True:
            active = self._native_active_thread
            if active is not None:
                # Never free the sole native lane while a timed-out/canceled QMT
                # function is still running.
                active.join()
                self._native_active_thread = None
            with self._condition:
                while not self._queue and not self._closed:
                    self._condition.wait()
                if self._closed:
                    return
                job_id = self._queue.popleft()
                job = self._jobs.get(job_id)
            if job is None:
                continue
            try:
                with bind_context(**dict(job.get("trace_context") or {})):
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
            self._persist_locked(job)
            _emit_download("download_job_started", critical=True, job_id=job_id)
            self._condition.notify_all()

        previously_ok = [
            stock
            for stock, result in job.get("symbol_results", {}).items()
            if result.get("status") == "ok"
        ]
        # Persisted success is only a hint.  Recheck QMT data before skipping it
        # after a bridge restart.
        missing_previously_ok = set(self._missing_downloaded_symbols(job_id, previously_ok))
        stocks = [
            stock
            for stock in job["stocks"]
            if stock not in previously_ok or stock in missing_previously_ok
        ]
        batch_size = int(job["batch_size"])
        logger.info("Started history download job: job_id=%s total=%s", job_id, len(stocks))

        for offset in range(0, len(stocks), batch_size):
            batch = stocks[offset : offset + batch_size]
            if self._is_stop_requested(job_id):
                self._mark_canceled(job_id)
                return
            with self._condition:
                job = self._jobs[job_id]
                if self._defer_market_hours and self._market_session():
                    job["status"] = "paused_market_hours"
                    job["current_batch"] = []
                    self._persist_locked(job)
                    _emit_download("download_job_market_paused", critical=True,
                                   job_id=job_id)
                    self._condition.notify_all()
                    # No new native invocation is started during market hours.
                    # The worker remains the sole owner of the lane.
                    while self._market_session() and not job.get("stop_requested"):
                        self._condition.wait(timeout=1.0)
                    if job.get("stop_requested"):
                        self._mark_canceled(job_id)
                        return
                    job["status"] = "running"
                job["current_batch"] = batch
                self._persist_locked(job)
                self._condition.notify_all()

            batch_status, batch_error = self._run_batch(job_id, batch, attempt=1)
            _emit_download("download_job_batch_finished", job_id=job_id,
                           attempt=1, outcome=batch_status, symbols=len(batch))
            if batch_status == "ok":
                self._mark_symbols(job_id, batch, status="ok", error="")
                continue
            if batch_status == "target_missing":
                self._mark_symbols(job_id, batch, status=batch_status, error=batch_error)
                continue
            if batch_status == "transport_stuck":
                self._mark_slow(job_id, batch, batch_error)
                self._mark_symbols(
                    job_id,
                    stocks[offset:],
                    status=batch_status,
                    error=batch_error,
                )
                break
            if self._is_stop_requested(job_id):
                self._mark_canceled(job_id)
                return

            self._mark_slow(job_id, batch, batch_error)
            if len(batch) == 1:
                retry_status, retry_error = self._retry_symbol(
                    job_id,
                    batch[0],
                    first_status=batch_status,
                    first_error=batch_error,
                    next_attempt=2,
                )
                if retry_status == "transport_stuck":
                    self._mark_symbols(
                        job_id,
                        stocks[offset + 1 :],
                        status=retry_status,
                        error=retry_error,
                    )
                    break
                continue

            transport_stuck = False
            for stock_offset, stock in enumerate(batch):
                if self._is_stop_requested(job_id):
                    self._mark_canceled(job_id)
                    return
                retry_status, retry_error = self._retry_symbol(
                    job_id,
                    stock,
                    first_status=batch_status,
                    first_error=batch_error,
                    next_attempt=2,
                )
                if retry_status == "transport_stuck":
                    self._mark_symbols(
                        job_id,
                        stocks[offset + stock_offset + 1 :],
                        status=retry_status,
                        error=retry_error,
                    )
                    transport_stuck = True
                    break
            if transport_stuck:
                break

        with self._condition:
            job = self._jobs[job_id]
            job["current_batch"] = []
            failed_count = len(job.get("failed_symbols") or [])
            total = int(job.get("total") or 0)
            transport_failures = [
                result
                for result in job["symbol_results"].values()
                if result.get("status") == "transport_stuck"
            ]
            if transport_failures:
                reason = transport_failures[0].get("error") or "xtdata transport stuck"
                job["error"] = f"xtdata transport unavailable: {reason}"
                final_status = "failed"
            elif total > 0 and failed_count >= total:
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
        _emit_download("download_job_finished", critical=True, job_id=job_id,
                       outcome={"completed": "success", "canceled": "canceled"}.get(
                           self.get(job_id).get("status"), "error"
                       ), status=self.get(job_id).get("status"))

    def _retry_symbol(
        self,
        job_id: str,
        stock: str,
        *,
        first_status: str = "error",
        first_error: str = "",
        next_attempt: int = 1,
    ) -> tuple[str, str]:
        max_attempts = int(self.get(job_id)["max_attempts"])
        last_status = first_status
        last_error = first_error
        for attempt in range(max(1, next_attempt), max_attempts + 1):
            _emit_download("download_job_retry", job_id=job_id, attempt=attempt,
                           symbol_count=1, previous_outcome=last_status)
            status, error = self._run_batch(job_id, [stock], attempt=attempt)
            last_status, last_error = status, error
            if status == "ok":
                self._mark_symbols(job_id, [stock], status="ok", error="")
                return status, error
            if status == "transport_stuck":
                self._mark_symbols(job_id, [stock], status=status, error=error)
                return status, error
            if self._is_stop_requested(job_id):
                return status, error
        self._mark_symbols(job_id, [stock], status=last_status, error=last_error)
        return last_status, last_error

    def _run_batch(self, job_id: str, batch: list[str], *, attempt: int) -> tuple[str, str]:
        started = time.time()
        deadline = started + DOWNLOAD_BATCH_TIMEOUT_SECONDS
        last_progress_at = started
        result_queue: Queue = Queue(maxsize=1)
        cancel_call = threading.Event()
        native_single = True
        trace_context = current_context()

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
            with bind_context(**trace_context):
                try:
                    job = self.get(job_id)
                    if native_single:
                        results = []
                        for stock in batch:
                            if cancel_call.is_set() or self._is_stop_requested(job_id):
                                raise XtdataCallCancelledError(
                                    "download canceled before next symbol"
                                )

                            def invoke_single():
                                if cancel_call.is_set() or self._is_stop_requested(job_id):
                                    raise XtdataCallCancelledError(
                                        "download canceled before native invocation"
                                    )
                                remaining = deadline - time.time()
                                if remaining <= 0:
                                    raise _DownloadCallTimeout(
                                        "batch budget expired before next symbol"
                                    )
                                return market_data.download_history_data(
                                    stock,
                                    period=job["period"],
                                    start_time=job["start_time"],
                                    end_time=job["end_time"],
                                    timeout_seconds=remaining,
                                )

                            with self._condition:
                                current = self._jobs[job_id]
                                current["native_inflight_symbols"] = [stock]
                                self._persist_locked(current)
                            _emit_download("download_job_native_started", critical=True,
                                           job_id=job_id, attempt=attempt, symbol_count=1)
                            results.append(
                                _call_xtdata_serialized_cancellable(
                                    cancel_call, invoke_single
                                )
                            )
                            with self._condition:
                                current = self._jobs[job_id]
                                current["native_inflight_symbols"] = []
                                self._persist_locked(current)
                            self._record_download_invocation(job_id, [stock])
                            on_progress({
                                "stock": stock,
                                "completed": len(results),
                                "total": len(batch),
                            })
                        result = results
                    result_queue.put(("ok", result))
                except XtdataTransportStuckError as exc:
                    self._clear_native_inflight(job_id)
                    result_queue.put(("transport_stuck", exc))
                except XtdataCallCancelledError as exc:
                    self._clear_native_inflight(job_id)
                    result_queue.put(("call_canceled", exc))
                except _DownloadCallTimeout as exc:
                    self._clear_native_inflight(job_id)
                    result_queue.put(("download_timeout", exc))
                except Exception as exc:
                    self._clear_native_inflight(job_id)
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
                    # Big QMT has no supported cancellation API for this native
                    # call.  Let it finish; the target checks cancellation before
                    # it starts the following symbol.
                    thread.join(0.1)
                now = time.time()
                if now - started > DOWNLOAD_BATCH_TIMEOUT_SECONDS:
                    thread.join(DOWNLOAD_STOP_GRACE_SECONDS)
                    raise _DownloadCallTimeout(
                        f"batch timeout after {DOWNLOAD_BATCH_TIMEOUT_SECONDS:.0f}s"
                    )
                if now - last_progress_at > DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS:
                    thread.join(DOWNLOAD_STOP_GRACE_SECONDS)
                    raise _DownloadCallTimeout(
                        f"no progress for {DOWNLOAD_NO_PROGRESS_TIMEOUT_SECONDS:.0f}s"
                    )
                thread.join(0.1)
        except _DownloadCallTimeout as exc:
            thread_alive = thread.is_alive()
            logger.warning(
                "History download batch timed out: job_id=%s stocks=%s attempt=%s error=%s",
                job_id,
                batch,
                attempt,
                exc,
            )
            if thread_alive:
                reason = f"{exc}; xtdata transport thread is still active"
                _mark_xtdata_transport_stuck(reason)
                _emit_download("download_job_native_stuck", critical=True,
                               job_id=job_id, attempt=attempt)
                self._native_active_thread = thread
                with self._condition:
                    job = self._jobs.get(job_id)
                    if job is not None:
                        # Persist an explicit unknown outcome.  A restarted bridge
                        # will not automatically replay this native invocation.
                        self._persist_locked(job)
                return "transport_stuck", reason
            return "download_timeout", str(exc)

        status, payload = result_queue.get() if not result_queue.empty() else ("ok", None)
        elapsed = time.time() - started
        if status == "transport_stuck":
            return status, str(payload)
        if status == "call_canceled":
            return "download_timeout", str(payload)
        if status == "download_timeout":
            return status, str(payload)
        if status == "ok":
            for validation_attempt in range(DOWNLOAD_HISTORY_VISIBILITY_ATTEMPTS):
                missing_symbols = self._missing_downloaded_symbols(job_id, batch)
                if not missing_symbols:
                    break
                if validation_attempt + 1 < DOWNLOAD_HISTORY_VISIBILITY_ATTEMPTS:
                    time.sleep(DOWNLOAD_HISTORY_VISIBILITY_INTERVAL_SECONDS)
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
        getter = getattr(market_data, "get_market_data_ex", None)
        if not callable(getter):
            return []
        job = self.get(job_id)
        try:
            raw = _call_xtdata_serialized(
                getter,
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
            self._persist_locked(job)
            self._condition.notify_all()

    def _clear_native_inflight(self, job_id: str) -> None:
        with self._condition:
            job = self._jobs.get(job_id)
            if job is not None:
                job["native_inflight_symbols"] = []
                self._persist_locked(job)

    def _record_download_invocation(self, job_id: str, stocks: list[str]) -> None:
        with self._condition:
            job = self._jobs[job_id]
            invoked = job["download_invoked_symbols"]
            for stock in stocks:
                if stock not in invoked:
                    invoked.append(stock)
            requested = set(job.get("stocks") or [])
            job["download_invocation_completed"] = bool(requested) and requested.issubset(
                set(invoked)
            )
            self._persist_locked(job)
            self._condition.notify_all()

    def _mark_slow(self, job_id: str, stocks: list[str], error: str) -> None:
        with self._condition:
            job = self._jobs[job_id]
            for stock in stocks:
                if stock not in job["slow_symbols"]:
                    job["slow_symbols"].append(stock)
            job["last_slow_error"] = error
            self._persist_locked(job)
            self._condition.notify_all()

    def _mark_canceled(self, job_id: str) -> None:
        with self._condition:
            job = self._jobs[job_id]
            self._finish_locked(job, "canceled")
            self._persist_locked(job)
            _emit_download("download_job_canceled", critical=True, job_id=job_id)
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
        requested = set(job.get("stocks") or [])
        invoked = set(job.get("download_invoked_symbols") or [])
        job["download_invocation_completed"] = bool(requested) and requested.issubset(
            invoked
        )
        verified = {
            stock
            for stock, result in (job.get("symbol_results") or {}).items()
            if result.get("status") == "ok"
        }
        # This verifies only that each symbol has at least one usable row after
        # the download call. It does not prove complete requested-window or
        # trading-calendar coverage, so the stronger coverage claim stays false.
        job["history_visibility_verified"] = bool(requested) and requested.issubset(
            verified
        )
        job["coverage_verified"] = False
        self._persist_locked(job)

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
            self._state.remove(job_id)
            try:
                self._queue.remove(job_id)
            except ValueError:
                pass

_download_jobs: _DownloadJobManager | None = None


def start_download_jobs(*, state_dir: str | None = None) -> _DownloadJobManager:
    """Start resumable work only after the BigQMT runtime is ready."""
    global _download_jobs
    if _download_jobs is None:
        _download_jobs = _DownloadJobManager(state_dir=state_dir)
    return _download_jobs


def close_download_jobs() -> None:
    """Stop the worker and release its local state ownership when safe."""
    global _download_jobs
    manager = _download_jobs
    if manager is None:
        return
    manager.close()
    _download_jobs = None


def _require_download_jobs() -> _DownloadJobManager:
    if _download_jobs is None:
        raise HTTPException(
            status_code=503,
            detail="BigQMT download runtime is not ready",
        )
    return _download_jobs
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
    global _download_jobs
    _reset_xtdata_transport_for_tests()
    if _download_jobs is None:
        _download_jobs = _DownloadJobManager(state_dir="", defer_market_hours=False)
    else:
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
        return len(_call_xtdata_serialized(market_data.get_sector_list) or [])
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

from ..bigqmt import xtdata


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
    for key, value in current_context().items():
        if key in {"trace_id", "span_id", "parent_span_id", "http_request_id"}:
            env["QMT_BRIDGE_TRACE_" + key.upper()] = str(value)
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
    _emit_download("sector_download_subprocess_started", critical=True,
                   timeout_seconds=timeout_seconds)
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
        _emit_download("sector_download_subprocess_timeout", critical=True,
                       timeout_seconds=timeout_seconds)
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
        _emit_download("sector_download_subprocess_failed", critical=True,
                       returncode=completed.returncode)
        payload.setdefault("error", f"sector download child exited with {completed.returncode}")
        return "error", payload
    status = str(payload.get("status") or "ok")
    if status == "ok":
        _emit_download("sector_download_subprocess_completed", critical=True,
                       elapsed_ms=int((time.time() - started) * 1000))
        return "ok", payload
    if status == "timeout":
        return "timeout", payload
    return "error", payload


def _execute_sector_download_in_process(on_progress) -> dict:
    return {
        "method": "xtdata.download_sector_data",
        "result": _call_xtdata_serialized(market_data.download_sector_data),
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
                status, payload = _call_xtdata_serialized(
                    _execute_sector_download_process,
                    timeout,
                )
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
                else "xtdata.download_sector_data",
            }
        )
    thread.start()

    while thread.is_alive():
        elapsed = time.time() - started
        guard_timeout = timeout
        if SECTOR_DOWNLOAD_USE_SUBPROCESS:
            guard_timeout += SECTOR_DOWNLOAD_STOP_GRACE_SECONDS
        if elapsed > guard_timeout:
            # Big QMT provides no supported cancellation hook for this native
            # work.  Report timeout without calling MiniQMT-only control APIs.
            thread.join(SECTOR_DOWNLOAD_STOP_GRACE_SECONDS)
            thread_alive = thread.is_alive()
            if thread_alive:
                _mark_xtdata_transport_stuck(
                    "sector download timeout; xtdata transport thread is still active"
                )
            after_count = None if thread_alive else _safe_sector_count()
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
    return _require_download_jobs().submit(req)


@router.get("/jobs/{job_id}")
def get_history_download_job(job_id: str):
    return _require_download_jobs().get(job_id)


@router.post("/jobs/{job_id}/cancel")
def cancel_history_download_job(job_id: str):
    return _require_download_jobs().cancel(job_id)


@router.post("/financial")
def download_financial(req: FinancialDownloadRequest):
    _call_xtdata_serialized(
        market_data.download_financial_data,
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
    _call_xtdata_serialized(market_data.download_index_weight)
    return {"status": "ok"}


@router.post("/etf_info")
def download_etf_info():
    _call_xtdata_serialized(market_data.download_etf_info)
    return {"status": "ok"}


@router.post("/cb_data")
def download_cb_data():
    _call_xtdata_serialized(market_data.download_cb_data)
    return {"status": "ok"}


@router.post("/history_contracts")
def download_history_contracts():
    _call_xtdata_serialized(market_data.download_history_contracts)
    return {"status": "ok"}


@router.post("/financial2")
def download_financial_data2(req: FinancialDownload2Request):
    """Synchronous financial data download (blocks until complete)."""
    _call_xtdata_serialized(
        market_data.download_financial_data2,
        req.stocks,
        table_list=req.tables,
    )
    return {"status": "ok", "stocks": req.stocks, "tables": req.tables}


@router.post("/holiday")
def download_holiday_data():
    """Download holiday calendar data."""
    _call_xtdata_serialized(market_data.download_holiday_data)
    return {"status": "ok"}
