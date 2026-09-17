"""FinancialMixin — financial data client methods."""


class FinancialMixin:
    """Client methods for /api/financial/* endpoints."""

    def get_financial_data(
        self,
        stocks: list[str],
        tables: list[str] | None = None,
        start_time: str = "",
        end_time: str = "",
        report_type: str = "report_time",
    ) -> dict:
        """Fetch financial statement data (Balance/Income/CashFlow etc.)."""
        resp = self._get("/api/financial/data", {
            "stocks": ",".join(stocks),
            "tables": ",".join(tables) if tables else "",
            "start_time": start_time,
            "end_time": end_time,
            "report_type": report_type,
        })
        return self._response_value(resp, default={})


    def get_raw_financial_data(
        self,
        stocks: list[str],
        fields: list[str] | None = None,
        start_time: str = "",
        end_time: str = "",
        report_type: str = "report_time",
    ) -> dict:
        return self._get("/api/financial/raw_data", {
            "stocks": ",".join(stocks),
            "fields": ",".join(fields or []),
            "start_time": start_time,
            "end_time": end_time,
            "report_type": report_type,
        })

    def get_findata(self, table: str, field: str = "", session: str = "") -> dict:
        return self._get("/api/financial/findata", {
            "table": table,
            "field": field,
            "session": session,
        })

    def download_financial(
        self,
        stocks: list[str],
        tables: list[str] | None = None,
        start_time: str = "",
        end_time: str = "",
    ) -> dict:
        """Trigger financial data download on the server."""
        return self._post("/api/download/financial", {
            "stocks": stocks,
            "tables": tables or [],
            "start_time": start_time,
            "end_time": end_time,
        })
