"""MetaMixin — system metadata client methods."""


class MetaMixin:
    """Client methods for /api/meta/* endpoints."""

    def get_markets(self) -> dict:
        """Fetch available markets."""
        resp = self._get("/api/meta/markets")
        return self._response_value(resp, key="markets", default={})

    def get_periods(self) -> list:
        """Fetch available K-line periods."""
        resp = self._get("/api/meta/periods")
        return self._response_value(resp, key="periods", default=[])

    def get_stock_list_by_category(self, category: str) -> list[str]:
        """Fetch stock codes by category."""
        resp = self._get("/api/meta/stock_list", {"category": category})
        return self._response_value(resp, key="stocks", default=[])

    def get_last_trade_date(self, market: str) -> str:
        """Fetch the last trade date for a market."""
        resp = self._get("/api/meta/last_trade_date", {"market": market})
        return resp.get("last_trade_date", "")

    def get_server_version(self) -> str:
        """Fetch the QMT Bridge server version."""
        resp = self._get("/api/meta/version")
        return resp.get("version", "")

    def get_runtime_version(self) -> str:
        """Fetch the embedded Big QMT adapter's upstream version."""
        resp = self._get("/api/meta/runtime_version")
        return resp.get("runtime_version", "")

    def get_connection_status(self) -> dict:
        """Check the Big QMT bridge connection status."""
        return self._get("/api/meta/connection_status")

    def health_check(self) -> dict:
        """Simple health check."""
        return self._get("/api/meta/health")

    def get_quote_server_status(self) -> dict:
        """Get detailed quote server connection status."""
        return self._get("/api/meta/quote_server_status")

    def get_capabilities(self) -> dict:
        return self._get("/api/meta/capabilities")

    def get_readiness(self) -> dict:
        return self._get("/api/meta/readiness")

    def get_history_readiness(
        self, stock: str, start_time: str, end_time: str, *, timeout_seconds: float = 8,
    ) -> dict:
        """Return the complete RPC-only minute-probe status, not just its data."""
        return self._get("/api/meta/history-readiness", {
            "stock": stock, "start_time": start_time, "end_time": end_time,
            "timeout_seconds": timeout_seconds,
        })

    def get_recovery_status(self) -> dict:
        """Read the current HTTP write-inflight guard without making an RPC call."""
        return self._get("/api/meta/recovery-status")

    def get_binary_cache_stats(self) -> dict:
        return self._get("/api/meta/binary_cache")
