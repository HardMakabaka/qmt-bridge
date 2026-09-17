"""SMTMixin — SMT (约定式交易) client methods."""


class SMTMixin:
    """Client methods for /api/smt/* endpoints."""






    def query_appointment_info(self, account_id: str = "") -> dict:
        """Query SMT appointment info (约定式预约信息)."""
        return self._get("/api/smt/appointment", {"account_id": account_id})

    def query_smt_secu_info(self, account_id: str = "") -> dict:
        """Query SMT security info (约定式证券信息)."""
        return self._get("/api/smt/secu_info", {"account_id": account_id})

    def query_smt_secu_rate(
        self,
        stock_code: str = "",
        max_term: int = 0,
        fare_way: int = 0,
        credit_type: int = 0,
        trade_type: int = 0,
        account_id: str = "",
    ) -> dict:
        """Query SMT security rates (约定式证券费率)."""
        return self._get("/api/smt/secu_rate", {
            "stock_code": stock_code,
            "max_term": max_term,
            "fare_way": fare_way,
            "credit_type": credit_type,
            "trade_type": trade_type,
            "account_id": account_id,
        })
