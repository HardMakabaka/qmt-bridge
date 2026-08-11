"""BaseClient — HTTP transport layer with authentication support."""

import json
import urllib.request
from typing import Optional
from urllib.parse import urlencode

from qmt_bridge.no_proxy import urlopen_direct


class BaseClient:
    """Lightweight HTTP/WebSocket client for QMT Bridge server.

    All mixin classes inherit from this to share ``_get``, ``_post``, ``_delete``,
    and ``_headers`` helpers.
    """

    def __init__(self, host: str, port: int = 13543, *, api_key: str = ""):
        """初始化客户端连接。

        Args:
            host: QMT Bridge 服务端 IP 地址或主机名，如 ``"192.168.1.100"``
            port: 服务端口，默认 13543
            api_key: API Key，交易端点需要认证时必填
        """
        self.base_url = f"http://{host}:{port}"
        self.ws_url = f"ws://{host}:{port}"
        self.api_key = api_key

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        """Build request headers, including API Key if configured."""
        headers: dict[str, str] = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Send a GET request and return parsed JSON."""
        if params:
            query = urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{self.base_url}{path}?{query}"
        else:
            url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, headers=self._headers())
        with urlopen_direct(req) as resp:
            return json.loads(resp.read().decode())

    def _post(self, path: str, body: dict) -> dict:
        """Send a POST request with JSON body and return parsed JSON."""
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode()
        headers = {"Content-Type": "application/json", **self._headers()}
        req = urllib.request.Request(url, data=data, headers=headers)
        with urlopen_direct(req) as resp:
            return json.loads(resp.read().decode())

    def _delete(self, path: str, params: Optional[dict] = None) -> dict:
        """Send a DELETE request and return parsed JSON."""
        if params:
            query = urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{self.base_url}{path}?{query}"
        else:
            url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, method="DELETE", headers=self._headers())
        with urlopen_direct(req) as resp:
            return json.loads(resp.read().decode())

    def _to_dataframes(self, data: dict) -> dict:
        """Convert {stock: [records]} to {stock: DataFrame}.

        Gracefully degrades to returning raw dicts when pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError:
            return data

        result: dict[str, pd.DataFrame] = {}
        for stock, records in data.items():
            if not records:
                result[stock] = pd.DataFrame()
                continue
            result[stock] = pd.DataFrame(records)
        return result

    def _response_value(self, payload: dict, key: str = "data", default=None):
        if payload.get("status") in {"unsupported", "unavailable", "error"}:
            return payload
        return payload.get(key, default)

    def _response_value(self, payload: dict, key: str = "data", default=None):
        """Return a response field without hiding a provider failure envelope."""
        if payload.get("status") in {"unsupported", "unavailable", "error"}:
            return payload
        return payload.get(key, default)
