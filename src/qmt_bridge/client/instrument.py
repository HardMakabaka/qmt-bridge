"""InstrumentMixin — instrument info client methods."""


class InstrumentMixin:
    """Client methods for /api/instrument/* endpoints."""

    def get_batch_instrument_detail(
        self, stocks: list[str], iscomplete: bool = False
    ) -> dict:
        """Fetch instrument details for multiple stocks."""
        resp = self._get("/api/instrument/batch_detail", {
            "stocks": ",".join(stocks),
            "iscomplete": iscomplete,
        })
        return self._response_value(resp, default={})

    def get_instrument_type(self, stock: str) -> str:
        """Determine instrument type (stock/futures/option/...)."""
        resp = self._get("/api/instrument/type", {"stock": stock})
        return resp.get("type", "")

    def get_ipo_info(self, start_time: str = "", end_time: str = "") -> dict:
        """Fetch IPO information."""
        resp = self._get("/api/instrument/ipo_info", {
            "start_time": start_time,
            "end_time": end_time,
        })
        return self._response_value(resp, default={})

    def get_index_weight(self, index_code: str) -> dict:
        """Fetch index constituent weights."""
        resp = self._get("/api/instrument/index_weight", {"index_code": index_code})
        return self._response_value(resp, default={})

    def get_st_history(self, stock: str) -> dict:
        """Fetch ST history for a stock."""
        resp = self._get("/api/instrument/st_history", {"stock": stock})
        return self._response_value(resp, default={})

    def get_open_date(self, stocks: list[str]) -> dict:
        return self._get("/api/instrument/open_date", {"stocks": ",".join(stocks)})

    def get_total_share(self, stocks: list[str]) -> dict:
        return self._get("/api/instrument/total_share", {"stocks": ",".join(stocks)})

    def get_last_volume(self, stock: str) -> dict:
        return self._get("/api/instrument/last_volume", {"stock": stock})

    def get_turnover_rate(
        self,
        stocks: list[str],
        start_time: str = "",
        end_time: str = "",
    ) -> dict:
        return self._get("/api/instrument/turnover_rate", {
            "stocks": ",".join(stocks),
            "start_time": start_time,
            "end_time": end_time,
        })

    def get_svol(self, stock: str) -> dict:
        return self._get("/api/instrument/svol", {"stock": stock})

    def get_bvol(self, stock: str) -> dict:
        return self._get("/api/instrument/bvol", {"stock": stock})

    def get_longhubang(
        self,
        stocks: list[str],
        start_time: str = "",
        end_time: str = "",
    ) -> dict:
        return self._get("/api/instrument/longhubang", {
            "stocks": ",".join(stocks),
            "start_time": start_time,
            "end_time": end_time,
        })

    def get_top10_share_holder(
        self,
        stocks: list[str],
        data_name: str = "",
        start_time: str = "",
        end_time: str = "",
    ) -> dict:
        return self._get("/api/instrument/top10_share_holder", {
            "stocks": ",".join(stocks),
            "data_name": data_name,
            "start_time": start_time,
            "end_time": end_time,
        })

    def get_risk_free_rate(self, index: int = 0) -> dict:
        return self._get("/api/instrument/risk_free_rate", {"index": index})

    def get_his_index_data(self, stock: str) -> dict:
        return self._get("/api/instrument/his_index_data", {"stock": stock})

    def is_suspended_stock(self, stock: str) -> dict:
        return self._get("/api/instrument/suspended", {"stock": stock})
