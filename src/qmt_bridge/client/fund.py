"""FundMixin — fund transfer client methods."""


class FundMixin:
    """Client methods for /api/fund/* endpoints."""



    def query_available_fund(self, account_id: str = "") -> dict:
        """Query available fund balance."""
        return self._get("/api/fund/available", {"account_id": account_id})
