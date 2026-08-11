"""ETFMixin — ETF & convertible bond client methods."""


class ETFMixin:
    """Client methods for /api/etf/* and /api/cb/* endpoints."""

    def get_etf_list(self) -> list[str]:
        """Fetch lightweight ETF code list."""
        resp = self._get("/api/etf/list")
        return self._response_value(resp, key="stocks", default=[])

    def get_etf_info(self) -> dict:
        """Fetch ETF subscription/redemption list."""
        resp = self._get("/api/etf/info")
        return self._response_value(resp, default={})

    def get_fund_etf_info(self, stocks: list[str] | None = None) -> dict:
        return self._get(
            "/api/fund/etf-info",
            {"stocks": ",".join(stocks or [])},
        )

    def get_fund_iopv(self, stocks: list[str]) -> dict:
        return self._get("/api/fund/iopv", {"stocks": ",".join(stocks)})

    def get_cb_info(self, stock: str) -> dict:
        """Fetch convertible bond information."""
        resp = self._get("/api/cb/info", {"stock": stock})
        return self._response_value(resp, default={})

    def get_cb_list(self) -> list[str]:
        """Fetch all convertible bond codes."""
        resp = self._get("/api/cb/list")
        return self._response_value(resp, key="stocks", default=[])

    def get_cb_detail(self, stock: str) -> dict:
        """Fetch detailed convertible bond information."""
        resp = self._get("/api/cb/detail", {"stock": stock})
        return self._response_value(resp, default={})

    def get_cb_conversion_price(self, stock: str) -> dict:
        """Fetch convertible bond conversion price info."""
        resp = self._get("/api/cb/conversion_price", {"stock": stock})
        return self._response_value(resp, default={})

    def get_bond_info(self, stock: str) -> dict:
        """Fetch bond-specific information."""
        resp = self._get("/api/cb/bond_info", {"stock": stock})
        return self._response_value(resp, default={})
