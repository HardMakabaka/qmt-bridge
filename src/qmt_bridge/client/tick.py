"""TickMixin — L2/tick data client methods."""


class TickMixin:
    """Client methods for /api/tick/* endpoints."""

    def get_l2_quote(
        self, stock: str, start_time: str = "", end_time: str = "", count: int = -1
    ) -> dict:
        """Fetch L2 quote snapshot."""
        resp = self._get("/api/tick/l2_quote", {
            "stock": stock,
            "start_time": start_time,
            "end_time": end_time,
            "count": count,
        })
        return self._response_value(resp, default={})

    def get_l2_order(
        self, stock: str, start_time: str = "", end_time: str = "", count: int = -1
    ) -> dict:
        """Fetch L2 order-by-order data."""
        resp = self._get("/api/tick/l2_order", {
            "stock": stock,
            "start_time": start_time,
            "end_time": end_time,
            "count": count,
        })
        return self._response_value(resp, default={})

    def get_l2_transaction(
        self, stock: str, start_time: str = "", end_time: str = "", count: int = -1
    ) -> dict:
        """Fetch L2 transaction-by-transaction data."""
        resp = self._get("/api/tick/l2_transaction", {
            "stock": stock,
            "start_time": start_time,
            "end_time": end_time,
            "count": count,
        })
        return self._response_value(resp, default={})
