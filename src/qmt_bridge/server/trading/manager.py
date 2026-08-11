"""XtTraderManager — lifecycle management for XtQuantTrader."""

import logging

from ..helpers import _status_payload

logger = logging.getLogger("qmt_bridge.trading")


class TradingWriteDisabled(PermissionError):
    def __init__(self, blocker: str):
        self.blocker = blocker
        super().__init__(blocker)


class XtTraderManager:
    """Manages an XtQuantTrader instance.

    Created during FastAPI lifespan startup when trading is enabled.
    """

    def __init__(
        self,
        runtime=None,
        account_id: str = "",
        order_writes_enabled: bool = True,
    ):
        self.runtime = runtime
        self.account_id = account_id
        self.order_writes_enabled = bool(order_writes_enabled)
        self._trader = None
        self._account = None

    def connect(self, event_loop=None):
        """Initialize and connect the XtQuantTrader instance."""
        from .callbacks import BridgeTraderCallback

        if self.runtime is None:
            raise RuntimeError("Big QMT runtime is required")
        self._trader = self.runtime.new_trader()
        self._account = self.runtime.new_account(self.account_id)
        self._callback = BridgeTraderCallback()
        if event_loop is not None:
            self._callback.set_event_loop(event_loop)

        self._trader.register_callback(self._callback)
        start_result = self._trader.start()
        if start_result != 0:
            raise RuntimeError(f"Big QMT RPC event listener failed to start: {start_result}")
        result = self._trader.connect()
        if result != 0:
            raise RuntimeError(f"Big QMT RPC connect failed: {result}")
        subscribe_result = self._trader.subscribe(self._account)
        if subscribe_result != 0:
            raise RuntimeError(
                f"Big QMT RPC account subscription failed: {subscribe_result}"
            )

        logger.info("Big QMT RPC trader connected, account=%s", self.account_id)

    def disconnect(self):
        """Disconnect and clean up."""
        if self._trader is not None:
            try:
                self._trader.stop()
            except Exception:
                logger.exception("Error stopping XtQuantTrader")
            self._trader = None

    def _resolve_account(self, account_id: str = ""):
        """Get the StockAccount — use provided or default."""
        if account_id and account_id != self.account_id:
            if self.runtime is None:
                raise RuntimeError("Big QMT runtime is required")
            return self.runtime.new_account(account_id)
        return self._account

    @property
    def write_blockers(self) -> list[str]:
        blockers = []
        if self.runtime is not None:
            try:
                self.runtime.probe()
            except (OSError, RuntimeError, TimeoutError, ValueError):
                blockers.append("bigqmt_rpc_unavailable")
        if not self.order_writes_enabled:
            blockers.append("bridge_order_writes_disabled")
        if self.runtime is None or not bool(
            self.runtime.ping_payload.get("allow_order_methods", False)
        ):
            blockers.append("bigqmt_order_methods_disabled")
        if self.runtime is None or self.runtime.ping_payload.get("terminal_real_mode") is not True:
            blockers.append("bigqmt_terminal_real_mode_required")
        return blockers

    @property
    def writes_enabled(self) -> bool:
        return not self.write_blockers

    @property
    def event_status(self) -> dict:
        from ..events import build_event_transport_status

        trader = self._trader
        thread = getattr(trader, "_event_thread", None)
        cursor = getattr(trader, "_event_cursor", None)
        return build_event_transport_status(
            cursor=cursor if isinstance(cursor, dict) else None,
            replay_gap=bool(getattr(trader, "_event_replay_gap", False)),
            listener_alive=bool(thread is not None and thread.is_alive()),
        )

    def _require_writes_enabled(self) -> None:
        blockers = self.write_blockers
        if blockers:
            raise TradingWriteDisabled(blockers[0])

    def _call_trader_optional(self, function_name: str, *args, **kwargs):
        """Call an optional XtQuantTrader method without leaking 500 errors."""
        if self._trader is None:
            return _status_payload(
                "unavailable",
                reason="xttrader_not_connected",
                function=function_name,
            )
        func = getattr(self._trader, function_name, None)
        if not callable(func):
            return _status_payload(
                "unsupported",
                reason=f"xttrader_{function_name}_missing",
                function=function_name,
            )
        try:
            return func(*args, **kwargs)
        except NotImplementedError as exc:
            return _status_payload(
                "unsupported",
                reason=str(exc) or f"xttrader_{function_name}_unsupported",
                function=function_name,
                error_type=exc.__class__.__name__,
            )
        except (ConnectionError, OSError, TimeoutError) as exc:
            return _status_payload(
                "unavailable",
                reason=str(exc) or f"xttrader_{function_name}_unavailable",
                function=function_name,
                error_type=exc.__class__.__name__,
            )
        except Exception as exc:
            return _status_payload(
                "error",
                reason=str(exc) or f"xttrader_{function_name}_failed",
                function=function_name,
                error_type=exc.__class__.__name__,
            )

    # ------------------------------------------------------------------
    # Order operations
    # ------------------------------------------------------------------

    def order(self, stock_code: str, order_type: int, order_volume: int,
              price_type: int = 5, price: float = 0.0,
              strategy_name: str = "", order_remark: str = "",
              account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._trader.order_stock(
            account, stock_code, order_type, order_volume,
            price_type, price, strategy_name, order_remark,
        )

    def cancel_order(self, order_id: int, account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._trader.cancel_order_stock(account, order_id)

    def cancel_order_sysid(self, order_sysid: str, market, account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._trader.cancel_order_stock_sysid(account, market, str(order_sysid))

    # ------------------------------------------------------------------
    # Query operations
    # ------------------------------------------------------------------

    def query_orders(
        self,
        account_id: str = "",
        cancelable_only: bool = False,
        client_submit_id: str = "",
    ):
        account = self._resolve_account(account_id)
        if self.runtime is None:
            orders = self._trader.query_stock_orders(account, cancelable_only)
        else:
            orders = self._trader.query_stock_orders(account, cancelable_only, "")
        if not client_submit_id:
            return orders
        return [
            order
            for order in (orders or [])
            if str(getattr(order, "order_remark", "") or "").strip() == client_submit_id
        ]

    def query_positions(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._trader.query_stock_positions(account)

    def query_asset(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._trader.query_stock_asset(account)

    def query_trades(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        if self.runtime is None:
            return self._trader.query_stock_trades(account)
        return self._trader.query_stock_trades(account, "")

    def query_order_detail(self, order_id: int = 0, account_id: str = ""):
        account = self._resolve_account(account_id)
        if self.runtime is None:
            orders = self._trader.query_stock_orders(account, False)
        else:
            orders = self._trader.query_stock_orders(account, False, "")
        if orders:
            for o in orders:
                if getattr(o, "order_id", None) == order_id:
                    return o
        return None

    # ------------------------------------------------------------------
    # Credit operations
    # ------------------------------------------------------------------

    def credit_order(self, stock_code: str, order_type: int, order_volume: int,
                     price_type: int = 5, price: float = 0.0,
                     credit_type: str = "fin_buy",
                     strategy_name: str = "", order_remark: str = "",
                     account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "credit_order",
            account, stock_code, order_type, order_volume,
            price_type, price, credit_type, strategy_name, order_remark,
        )

    def query_credit_positions(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._trader.query_stock_positions(account)

    def query_credit_asset(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._trader.query_stock_asset(account)

    def query_credit_debt(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_stk_compacts", account)

    def query_credit_available(self, stock_code: str = "", account_id: str = ""):
        return _status_payload(
            "unsupported",
            reason="xttrader_query_credit_available_missing",
            function="query_credit_available",
            stock_code=stock_code,
        )

    def query_slo_stocks(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_credit_slo_code", account)

    def query_fin_stocks(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_credit_subjects", account)

    # ------------------------------------------------------------------
    # Fund operations
    # ------------------------------------------------------------------

    def fund_transfer(self, transfer_direction: int, amount: float, account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "fund_transfer", account, transfer_direction, amount
        )

    def query_fund_transfer_records(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_fund_transfer", account)

    def query_available_fund(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        asset = self._trader.query_stock_asset(account)
        return asset

    def ctp_fund_transfer(self, direction: int, amount: float, account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "ctp_fund_transfer", account, direction, amount
        )

    def query_ctp_balance(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_ctp_balance", account)

    # ------------------------------------------------------------------
    # Bank operations
    # ------------------------------------------------------------------

    def bank_transfer(self, direction: int, amount: float, bank_code: str = "", account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "bank_transfer", account, direction, amount, bank_code
        )

    def query_bank_balance(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_bank_balance", account)

    def query_bank_transfer_records(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_bank_transfer_records", account)

    def query_bound_banks(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_bound_banks", account)

    def query_transfer_limit(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_transfer_limit", account)

    def query_bank_available(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_bank_available", account)

    def query_bank_transfer_status(self, transfer_id: str = "", account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "query_bank_transfer_status", account, transfer_id
        )

    # ------------------------------------------------------------------
    # SMT operations (约定式交易 — real API)
    # ------------------------------------------------------------------

    def smt_order(self, stock_code: str, order_type: int, order_volume: int,
                  price_type: int = 5, price: float = 0.0,
                  smt_type: str = "", strategy_name: str = "", order_remark: str = "",
                  account_id: str = ""):
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "smt_order",
            account, stock_code, order_type, order_volume,
            price_type, price, smt_type, strategy_name, order_remark,
        )

    def smt_negotiate_order_async(self, stock_code: str, order_type: int,
                                  order_volume: int, price: float = 0.0,
                                  compact_id: str = "",
                                  strategy_name: str = "", order_remark: str = "",
                                  account_id: str = ""):
        """Async SMT negotiate order — result via on_smt_appointment_async_response callback."""
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "smt_negotiate_order_async",
            account, stock_code, order_type, order_volume,
            price, compact_id, strategy_name, order_remark,
        )

    def cancel_smt_order(self, order_id: int, account_id: str = ""):
        """Cancel an SMT order."""
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional("cancel_smt_order", account, order_id)

    def smt_query_quoter(self, account_id: str = ""):
        """Query SMT quoter information (报价方信息)."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("smt_query_quoter", account)

    def smt_query_compact(self, account_id: str = ""):
        """Query SMT compacts (约定合约)."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("smt_query_compact", account)

    def query_appointment_info(self, account_id: str = ""):
        """Query SMT appointment info (约定式预约信息)."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_appointment_info", account)

    def query_smt_secu_info(self, account_id: str = ""):
        """Query SMT security info (约定式证券信息)."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_smt_secu_info", account)

    def query_smt_secu_rate(
        self,
        stock_code: str = "",
        max_term: int = 0,
        fare_way: int = 0,
        credit_type: int = 0,
        trade_type: int = 0,
        account_id: str = "",
    ):
        """Query SMT security rates (约定式证券费率)."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "query_smt_secu_rate",
            account,
            stock_code,
            max_term,
            fare_way,
            credit_type,
            trade_type,
        )

    # ------------------------------------------------------------------
    # Async order operations
    # ------------------------------------------------------------------

    def order_async(self, stock_code: str, order_type: int, order_volume: int,
                    price_type: int = 5, price: float = 0.0,
                    strategy_name: str = "", order_remark: str = "",
                    account_id: str = ""):
        """Async order — result delivered via on_order_stock_async_response callback."""
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "order_stock_async",
            account, stock_code, order_type, order_volume,
            price_type, price, strategy_name, order_remark,
        )

    def cancel_order_async(self, order_id: int, account_id: str = ""):
        """Async cancel — result delivered via on_cancel_order_stock_async_response callback."""
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "cancel_order_stock_async", account, order_id
        )

    def cancel_order_sysid_async(self, order_sysid: str, market, account_id: str = ""):
        """Async cancel by QMT order_sysid — result delivered via trading callback."""
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "cancel_order_stock_sysid_async", account, market, str(order_sysid)
        )

    # ------------------------------------------------------------------
    # Single-item queries
    # ------------------------------------------------------------------

    def query_single_order(self, order_id: int, account_id: str = ""):
        """Query a single order by order_id."""
        account = self._resolve_account(account_id)
        return self._trader.query_stock_order(account, order_id)

    def query_single_trade(self, trade_id: int, account_id: str = ""):
        """Query a single trade by trade_id."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_stock_trade", account, trade_id)

    def query_single_position(self, stock_code: str, account_id: str = ""):
        """Query position for a single stock."""
        account = self._resolve_account(account_id)
        return self._trader.query_stock_position(account, stock_code)

    # ------------------------------------------------------------------
    # Position statistics
    # ------------------------------------------------------------------

    def query_position_statistics(self, account_id: str = ""):
        """Query position statistics summary."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_position_statistics", account)

    # ------------------------------------------------------------------
    # Credit extended queries
    # ------------------------------------------------------------------

    def query_credit_subjects(self, account_id: str = ""):
        """Query credit subject list."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_credit_subjects", account)

    def query_credit_assure(self, account_id: str = ""):
        """Query credit assurance / collateral info."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_credit_assure", account)

    # ------------------------------------------------------------------
    # IPO queries
    # ------------------------------------------------------------------

    def query_new_purchase_limit(self, account_id: str = ""):
        """Query IPO new purchase limit."""
        account = self._resolve_account(account_id)
        return self._trader.query_new_purchase_limit(account)

    def query_ipo_data(self):
        """Query IPO calendar data."""
        return self._trader.query_ipo_data()

    # ------------------------------------------------------------------
    # Account info
    # ------------------------------------------------------------------

    def get_account_status(self, account_id: str = ""):
        try:
            return {"connected": self._trader is not None}
        except Exception:
            return {"connected": False}

    def get_account_info(self, account_id: str = ""):
        account = self._resolve_account(account_id)
        return self._trader.query_stock_asset(account)

    def query_account_infos(self):
        """Query info for all registered accounts."""
        return self._trader.query_account_infos()

    # ------------------------------------------------------------------
    # COM queries
    # ------------------------------------------------------------------

    def query_com_fund(self, account_id: str = ""):
        """Query COM fund (期权/期货账户资金)."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_com_fund", account)

    def query_com_position(self, account_id: str = ""):
        """Query COM positions (期权/期货持仓)."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional("query_com_position", account)

    # ------------------------------------------------------------------
    # CTP cross-market transfers
    # ------------------------------------------------------------------

    def ctp_transfer_option_to_future(self, amount: float, account_id: str = ""):
        """Transfer from option account to future account."""
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "ctp_transfer_option_to_future", account, amount
        )

    def ctp_transfer_future_to_option(self, amount: float, account_id: str = ""):
        """Transfer from future account to option account."""
        self._require_writes_enabled()
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "ctp_transfer_future_to_option", account, amount
        )

    # ------------------------------------------------------------------
    # Data export / external sync
    # ------------------------------------------------------------------

    def export_data(self, data_type: str = "orders", file_path: str = "", account_id: str = ""):
        """Export trading data to file."""
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "export_data", account, data_type, file_path
        )

    def query_data(
        self,
        data_type: str = "orders",
        result_path: str = "",
        start_time: str | None = None,
        end_time: str | None = None,
        account_id: str = "",
    ):
        """Query exported trading data."""
        if not result_path:
            return _status_payload(
                "unsupported",
                reason="xttrader_query_data_requires_result_path",
                function="query_data",
            )
        account = self._resolve_account(account_id)
        return self._call_trader_optional(
            "query_data",
            account,
            result_path,
            data_type,
            start_time,
            end_time,
            {},
        )

    def sync_transaction_from_external(
        self,
        operation: str,
        data_type: str,
        data: list,
        account_type: str = "STOCK",
        account_id: str = "",
    ):
        """Sync external transaction records into the system."""
        self._require_writes_enabled()
        target_account_id = str(account_id or self.account_id)
        return self._call_trader_optional(
            "sync_transaction_from_external",
            operation,
            data_type,
            target_account_id,
            account_type,
            data,
        )
