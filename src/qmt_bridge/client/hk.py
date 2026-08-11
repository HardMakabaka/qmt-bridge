"""HKMixin — Hong Kong market client methods."""


class HKMixin:
    """Client methods for /api/hk/* endpoints."""

    def get_hk_stock_list(self) -> list[str]:
        """Get list of HK-connected stocks."""
        resp = self._get("/api/hk/stock_list")
        return self._response_value(resp, key="stocks", default=[])

    def get_hk_connect_stocks(self, connect_type: str = "north") -> list[str]:
        """Get HK-connect stock list by direction."""
        resp = self._get("/api/hk/connect_stocks", {"connect_type": connect_type})
        return self._response_value(resp, key="stocks", default=[])

    def get_north_finance_change(self, period: str = "1d") -> dict:
        return self._get("/api/hk/north_finance_change", {"period": period})

    def get_hkt_statistics(self, stock: str) -> dict:
        return self._get("/api/hk/statistics", {"stock": stock})

    def get_hkt_details(self, stock: str) -> dict:
        return self._get("/api/hk/details", {"stock": stock})

    def get_hkt_exchange_rate(
        self,
        account_id: str = "",
        account_type: str = "",
    ) -> dict:
        return self._get("/api/hk/exchange_rate", {
            "account_id": account_id,
            "account_type": account_type,
        })
