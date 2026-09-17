"""FuturesMixin — futures data client methods."""


class FuturesMixin:
    """Client methods for /api/futures/* endpoints."""

    def get_main_contract(
        self, code_market: str, start_time: str = "", end_time: str = ""
    ) -> dict:
        """Fetch futures main contract."""
        resp = self._get("/api/futures/main_contract", {
            "code_market": code_market,
            "start_time": start_time,
            "end_time": end_time,
        })
        return self._response_value(resp, default={})


    def get_contract_multiplier(self, contract_code: str) -> dict:
        return self._get(
            "/api/futures/contract_multiplier",
            {"contract_code": contract_code},
        )

    def get_contract_expire_date(self, code_market: str) -> dict:
        return self._get(
            "/api/futures/contract_expire_date",
            {"code_market": code_market},
        )

    def get_his_contract_list(self, market: str) -> dict:
        return self._get("/api/futures/his_contract_list", {"market": market})
