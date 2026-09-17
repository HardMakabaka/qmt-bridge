"""CreditMixin — credit (margin) trading client methods."""


class CreditMixin:
    """Client methods for /api/credit/* endpoints."""


    def query_credit_positions(self, account_id: str = "") -> dict:
        """Query credit trading positions."""
        return self._get("/api/credit/positions", {"account_id": account_id})

    def query_credit_asset(self, account_id: str = "") -> dict:
        """Query credit trading account asset."""
        return self._get("/api/credit/asset", {"account_id": account_id})

    def query_credit_debt(self, account_id: str = "") -> dict:
        """Query credit debt information."""
        return self._get("/api/credit/debt", {"account_id": account_id})


    def query_slo_stocks(self, account_id: str = "") -> dict:
        """Query stocks available for short selling."""
        return self._get("/api/credit/slo_stocks", {"account_id": account_id})

    def query_fin_stocks(self, account_id: str = "") -> dict:
        """Query stocks available for margin buying."""
        return self._get("/api/credit/fin_stocks", {"account_id": account_id})

    def query_credit_subjects(self, account_id: str = "") -> dict:
        """Query credit subject list (标的证券)."""
        return self._get("/api/credit/subjects", {"account_id": account_id})

    def query_credit_assure(self, account_id: str = "") -> dict:
        """Query credit assurance / collateral info (担保品)."""
        return self._get("/api/credit/assure", {"account_id": account_id})
