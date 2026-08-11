"""UtilityMixin — utility client methods."""


class UtilityMixin:
    """Client methods for /api/utility/* endpoints."""

    def get_stock_name(self, stock: str) -> str:
        """Get the Chinese name for a stock code."""
        resp = self._get("/api/utility/stock_name", {"stock": stock})
        return self._response_value(resp, key="name", default="")

    def get_batch_stock_name(self, stocks: list[str]) -> dict[str, str]:
        """Get Chinese names for multiple stock codes."""
        resp = self._get("/api/utility/batch_stock_name", {"stocks": ",".join(stocks)})
        return self._response_value(resp, default={})

    def code_to_market(self, stock: str) -> dict:
        """Determine which market a stock code belongs to."""
        return self._get("/api/utility/code_to_market", {"stock": stock})

    def search_stocks(
        self, keyword: str, category: str = "沪深A股", limit: int = 20
    ) -> list[str]:
        """Search stocks by keyword (code prefix or name)."""
        resp = self._get("/api/utility/search", {
            "keyword": keyword,
            "category": category,
            "limit": limit,
        })
        return self._response_value(resp, key="stocks", default=[])

    def get_industry_name(self, stock: str, industry_type: str = "SW2") -> dict:
        return self._get("/api/utility/industry_name", {
            "stock": stock,
            "industry_type": industry_type,
        })

    def get_market_time(self, market: str) -> dict:
        return self._get("/api/utility/market_time", {"market": market})

    def get_basket(self, basket_name: str) -> dict:
        return self._get("/api/utility/basket", {"basket_name": basket_name})
