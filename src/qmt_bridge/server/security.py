"""API Key authentication for protected endpoints."""

import hmac

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from bigqmt_signal_trader import telemetry

from .config import Settings, get_settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(
    api_key: str | None = Security(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> str:
    """Dependency that enforces API Key authentication.

    Used for trading endpoints that always require authentication.
    """
    if not settings.api_key:
        telemetry.emit("http.auth", outcome="rejected", reason_code="api_key_not_configured")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API key not configured on server",
        )
    if api_key is None or not hmac.compare_digest(api_key, settings.api_key):
        telemetry.emit("http.auth", outcome="rejected", reason_code="invalid_or_missing_api_key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
    telemetry.emit("http.auth", outcome="success", required=True)
    return api_key


def optional_api_key(
    api_key: str | None = Security(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> str | None:
    """Dependency that enforces API Key only when require_auth_for_data is True.

    Used for data endpoints where auth is configurable.
    """
    if not settings.require_auth_for_data:
        telemetry.emit("http.auth", outcome="not_required", required=False)
        return api_key
    return require_api_key(api_key, settings)


def require_api_key_for_writes(
    request: Request,
    api_key: str | None = Security(_api_key_header),
    settings: Settings = Depends(get_settings),
) -> str | None:
    """Apply configured data auth to reads and mandatory auth to mutations.

    Router-level use prevents newly added POST/PUT/PATCH/DELETE endpoints from
    accidentally bypassing authentication.  Safe methods retain the existing
    ``QMT_BRIDGE_REQUIRE_AUTH_FOR_DATA`` behaviour.
    """
    if request.method.upper() not in {"GET", "HEAD", "OPTIONS"}:
        return require_api_key(api_key, settings)
    return optional_api_key(api_key, settings)
