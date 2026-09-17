"""BaseClient — HTTP transport layer with authentication support."""

import json
import math
import urllib.request
from typing import Optional
from urllib.parse import urlencode

from qmt_bridge.no_proxy import urlopen_direct
from qmt_bridge.trace_headers import outgoing_trace_headers
from bigqmt_signal_trader import telemetry


class BaseClient:
    """Lightweight HTTP/WebSocket client for QMT Bridge server.

    All mixin classes inherit from this to share ``_get``, ``_post``, ``_delete``,
    and ``_headers`` helpers.
    """

    def __init__(self, host: str, port: int = 13543, *, api_key: str = "", timeout: float = 30.0):
        """初始化客户端连接。

        Args:
            host: QMT Bridge 服务端 IP 地址或主机名，如 ``"192.168.1.100"``
            port: 服务端口，默认 13543
            api_key: API Key，交易端点需要认证时必填
            timeout: HTTP socket timeout in seconds; must be finite and positive.
        """
        self.base_url = f"http://{host}:{port}"
        self.ws_url = f"ws://{host}:{port}"
        self.api_key = api_key
        self.timeout = float(timeout)
        if not math.isfinite(self.timeout) or self.timeout <= 0:
            raise ValueError("timeout must be finite and positive")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        """Build request headers, including API Key if configured."""
        headers: dict[str, str] = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        headers.update(outgoing_trace_headers())
        return headers

    @telemetry.traced("sdk.http.get", service_role="sdk")
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Send a GET request and return parsed JSON."""
        if params:
            query = urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{self.base_url}{path}?{query}"
        else:
            url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, headers=self._headers())
        return self._request(req)

    @telemetry.traced("sdk.http.post", service_role="sdk")
    def _post(self, path: str, body: dict) -> dict:
        """Send a POST request with JSON body and return parsed JSON."""
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode()
        headers = {"Content-Type": "application/json", **self._headers()}
        req = urllib.request.Request(url, data=data, headers=headers)
        return self._request(req)

    @telemetry.traced("sdk.http.delete", service_role="sdk")
    def _delete(self, path: str, params: Optional[dict] = None) -> dict:
        """Send a DELETE request and return parsed JSON."""
        if params:
            query = urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{self.base_url}{path}?{query}"
        else:
            url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, method="DELETE", headers=self._headers())
        return self._request(req)

    def _request(self, req) -> dict:
        with telemetry.span("sdk.http.exchange", method=req.get_method(), attempt=1,
                            timeout_seconds=self.timeout) as observation:
            try:
                with urlopen_direct(req, timeout=self.timeout) as resp:
                    observation["http_status"] = getattr(resp, "status", None)
                    body = resp.read()
                    observation["response_bytes"] = len(body)
            except Exception as exc:
                observation.update(http_status=getattr(exc, "code", None), phase="network",
                                   response_observed=False)
                raise
        with telemetry.span("sdk.http.decode") as observation:
            payload = json.loads(body.decode())
            if isinstance(payload, dict) and isinstance(payload.get("status"), str):
                observation["business_status"] = payload["status"]
            return payload

    def _to_dataframes(self, data: dict) -> dict:
        """Convert {stock: [records]} to {stock: DataFrame}.

        Gracefully degrades to returning raw dicts when pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError:
            telemetry.emit("sdk.dataframes", outcome="not_available", reason_code="pandas_not_installed")
            return data

        result: dict[str, pd.DataFrame] = {}
        for stock, records in data.items():
            if not records:
                result[stock] = pd.DataFrame()
                continue
            result[stock] = pd.DataFrame(records)
        return result

    def _response_value(self, payload: dict, key: str = "data", default=None):
        """Return a response field without hiding a provider failure envelope."""
        if payload.get("status") in {"unsupported", "unavailable", "error", "overloaded", "timeout", "partial", "stale"}:
            telemetry.emit("sdk.business_result", outcome=payload["status"])
            return payload
        return payload.get(key, default)
