"""Small local binary cache for expensive read-only QMT calls."""

from __future__ import annotations

from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import pickle
import tempfile
import threading
import time
from typing import Any, Callable

from bigqmt_signal_trader.telemetry import emit, span


BINARY_CACHE_SCHEMA_VERSION = "qmt_bridge_binary_cache_v1"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError:
        return default
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = float(str(raw).strip())
    except ValueError:
        return default
    return max(minimum, value)


def _default_cache_dir() -> Path:
    base = os.getenv("QMT_BRIDGE_BINARY_CACHE_DIR")
    if base:
        return Path(base)
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "qmt-bridge" / "binary-cache"
    return Path(tempfile.gettempdir()) / "qmt-bridge" / "binary-cache"


def _normalize_for_key(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _normalize_for_key(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple, set)):
        return [_normalize_for_key(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _cache_key(namespace: str, params: dict[str, Any]) -> str:
    payload = {
        "namespace": namespace,
        "params": _normalize_for_key(params),
        "schema": BINARY_CACHE_SCHEMA_VERSION,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


@dataclass(frozen=True)
class CacheRead:
    hit: bool
    data: Any = None
    metadata: dict[str, Any] | None = None


class BinaryCache:
    def __init__(
        self,
        *,
        enabled: bool | None = None,
        cache_dir: Path | None = None,
        ttl_seconds: int | None = None,
        max_bytes: int | None = None,
        singleflight_wait_seconds: float | None = None,
    ) -> None:
        self.enabled = _env_bool("QMT_BRIDGE_BINARY_CACHE_ENABLED", True) if enabled is None else enabled
        self.cache_dir = cache_dir or _default_cache_dir()
        self.ttl_seconds = (
            ttl_seconds
            if ttl_seconds is not None
            else _env_int("QMT_BRIDGE_BINARY_CACHE_TTL_SECONDS", 86400, minimum=1)
        )
        self.max_bytes = (
            max_bytes
            if max_bytes is not None
            else _env_int("QMT_BRIDGE_BINARY_CACHE_MAX_BYTES", 2 * 1024 * 1024 * 1024, minimum=0)
        )
        self.singleflight_wait_seconds = (
            singleflight_wait_seconds
            if singleflight_wait_seconds is not None
            else _env_float("QMT_BRIDGE_BINARY_CACHE_SINGLEFLIGHT_WAIT_SECONDS", 30.0)
        )
        if self.singleflight_wait_seconds <= 0:
            self.singleflight_wait_seconds = 30.0
        self._inflight_lock = threading.Lock()
        self._inflight: dict[str, Future[tuple[Any, dict[str, Any]]]] = {}

    def path_for(self, namespace: str, params: dict[str, Any]) -> Path:
        digest = _cache_key(namespace, params)
        return self.cache_dir / namespace / f"{digest}.pkl"

    def get(self, namespace: str, params: dict[str, Any]) -> CacheRead:
        if not self.enabled:
            return CacheRead(hit=False, metadata={"enabled": False})
        path = self.path_for(namespace, params)
        try:
            with path.open("rb") as handle:
                envelope = pickle.load(handle)
            metadata = dict(envelope.get("metadata") or {})
            data = envelope.get("data")
            created_at = float(metadata.get("created_at") or 0)
            ttl_seconds = int(metadata.get("ttl_seconds") or self.ttl_seconds)
        except FileNotFoundError:
            return CacheRead(hit=False, metadata={"reason": "miss", "path": str(path)})
        except Exception as exc:
            self._delete_best_effort(path)
            return CacheRead(hit=False, metadata={"reason": "read_error", "error": str(exc), "path": str(path)})

        if created_at <= 0 or time.time() - created_at > ttl_seconds:
            self._delete_best_effort(path)
            return CacheRead(hit=False, metadata={"reason": "expired", "path": str(path)})
        if metadata.get("schema") != BINARY_CACHE_SCHEMA_VERSION:
            self._delete_best_effort(path)
            return CacheRead(hit=False, metadata={"reason": "schema_mismatch", "path": str(path)})
        metadata["path"] = str(path)
        return CacheRead(hit=True, data=data, metadata=metadata)

    def set(
        self,
        namespace: str,
        params: dict[str, Any],
        data: Any,
        *,
        extra_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            emit("market.binary_cache.store_skipped", namespace=namespace, reason="disabled")
            return {"enabled": False, "stored": False}
        path = self.path_for(namespace, params)
        path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "schema": BINARY_CACHE_SCHEMA_VERSION,
            "namespace": namespace,
            "created_at": time.time(),
            "ttl_seconds": self.ttl_seconds,
            **(extra_metadata or {}),
        }
        envelope = {"metadata": metadata, "data": data}
        fd, tmp_name = tempfile.mkstemp(
            prefix=f"{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                pickle.dump(envelope, handle, protocol=pickle.HIGHEST_PROTOCOL)
            tmp_path.replace(path)
            size = path.stat().st_size
            metadata["size_bytes"] = size
            self._enforce_size_budget()
            emit("market.binary_cache.stored", namespace=namespace, size_bytes=size)
            return {"enabled": True, "stored": True, "path": str(path), **metadata}
        except Exception:
            self._delete_best_effort(tmp_path)
            raise

    def cached_call(
        self,
        namespace: str,
        params: dict[str, Any],
        loader: Callable[[], Any],
        *,
        should_store: Callable[[Any], bool] | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        read = self.get(namespace, params)
        if read.hit:
            emit("market.binary_cache.hit", namespace=namespace)
            return read.data, {"enabled": self.enabled, "hit": True, **(read.metadata or {})}

        key = _cache_key(namespace, params)
        with self._inflight_lock:
            flight = self._inflight.get(key)
            owns_flight = flight is None
            if flight is None:
                flight = Future()
                self._inflight[key] = flight
        if not owns_flight:
            try:
                with span("market.binary_cache.singleflight_wait", namespace=namespace) as result:
                    value = flight.result(timeout=self.singleflight_wait_seconds)
                    result["outcome"] = "success"
                    return value
            except FutureTimeout as exc:
                # A caller waiting behind a slow QMT request must retain a
                # bounded request budget; it did not execute the loader.
                emit("market.binary_cache.singleflight_wait", namespace=namespace,
                     outcome="timeout", critical=True)
                raise TimeoutError("binary_cache_singleflight_wait_timeout") from exc

        try:
            with span("market.binary_cache.load", namespace=namespace) as result:
                data = loader()
                result["outcome"] = "success"
            cacheable = should_store(data) if should_store is not None else True
            if cacheable:
                metadata = self.set(namespace, params, data, extra_metadata=extra_metadata)
                result = (data, {"hit": False, **metadata})
            else:
                emit("market.binary_cache.store_skipped", namespace=namespace,
                     reason="not_cacheable")
                result = (
                    data,
                    {"enabled": self.enabled, "hit": False, "stored": False, "reason": "not_cacheable"},
                )
            flight.set_result(result)
            return result
        except BaseException as exc:
            flight.set_exception(exc)
            raise
        finally:
            with self._inflight_lock:
                if self._inflight.get(key) is flight:
                    self._inflight.pop(key, None)

    def stats(self) -> dict[str, Any]:
        total_files = 0
        total_bytes = 0
        namespaces: dict[str, dict[str, int]] = {}
        if self.cache_dir.exists():
            for path in self.cache_dir.rglob("*.pkl"):
                if not path.is_file():
                    continue
                size = path.stat().st_size
                total_files += 1
                total_bytes += size
                namespace = path.parent.name
                bucket = namespaces.setdefault(namespace, {"files": 0, "bytes": 0})
                bucket["files"] += 1
                bucket["bytes"] += size
        return {
            "enabled": self.enabled,
            "schema": BINARY_CACHE_SCHEMA_VERSION,
            "cache_dir": str(self.cache_dir),
            "ttl_seconds": self.ttl_seconds,
            "max_bytes": self.max_bytes,
            "singleflight_wait_seconds": self.singleflight_wait_seconds,
            "files": total_files,
            "bytes": total_bytes,
            "namespaces": namespaces,
        }

    def _enforce_size_budget(self) -> None:
        if self.max_bytes <= 0 or not self.cache_dir.exists():
            return
        files = [
            (path.stat().st_mtime, path.stat().st_size, path)
            for path in self.cache_dir.rglob("*.pkl")
            if path.is_file()
        ]
        total = sum(size for _mtime, size, _path in files)
        if total <= self.max_bytes:
            return
        for _mtime, size, path in sorted(files):
            self._delete_best_effort(path)
            total -= size
            if total <= self.max_bytes:
                break

    @staticmethod
    def _delete_best_effort(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            return
        except Exception:
            return


_binary_cache: BinaryCache | None = None


def get_binary_cache() -> BinaryCache:
    global _binary_cache
    if _binary_cache is None:
        _binary_cache = BinaryCache()
    return _binary_cache


def reset_binary_cache(cache: BinaryCache | None = None) -> None:
    global _binary_cache
    _binary_cache = cache
