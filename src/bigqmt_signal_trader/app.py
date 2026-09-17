"""信号交易应用编排层。"""

import datetime as _dt

from .code_utils import normalize_stock_code
from .models import OrderSubmitResult
from .models import AccountSnapshot, OrderRequest, SignalAction
from .price_engine import build_order_price
from .risk_guard import validate_signal
from .telemetry import emit, span


class _SubmissionStateUnconfirmed(Exception):
    """A broker write may have happened; do not ACK or automatically retry."""


class SignalTradingApp:
    def __init__(
        self,
        account_id,
        signal_source,
        market_data,
        position_provider,
        order_gateway,
        position_sync_sink,
        state_store,
        consumer_id="bigqmt-signal-trader",
        fetch_limit=20,
    ):
        self.account_id = account_id
        self.signal_source = signal_source
        self.market_data = market_data
        self.position_provider = position_provider
        self.order_gateway = order_gateway
        self.position_sync_sink = position_sync_sink
        self.state_store = state_store
        self.consumer_id = consumer_id
        self.fetch_limit = int(fetch_limit)

    def tick(self, now=None):
        now = now or _dt.datetime.now()
        signals = self.signal_source.fetch(self.account_id, self.fetch_limit)
        emit("signal.fetch", account_id=self.account_id, count=len(signals or []), outcome="success")
        positions = self.position_provider.get_positions(self.account_id)

        for signal in signals:
            # A Redis Stream pending entry can be reclaimed after a process
            # dies between submit and ACK.  Reconcile its durable state first:
            # a submitted/terminal signal is acknowledged, never replayed.
            get_status = getattr(self.state_store, "get_status", None)
            previous = get_status(signal.signal_id, signal.account_id) if callable(get_status) else {}
            previous_status = str((previous or {}).get("status") or "").upper()
            if previous_status in {
                "SUBMITTED", "SUBMITTED_UNCONFIRMED", "CONFIRMED",
                "IDEMPOTENT", "FAILED", "SKIPPED",
            }:
                emit("signal.reconcile.terminal_ack", critical=True, signal_id=signal.signal_id,
                     account_id=signal.account_id, outcome="success", state=previous_status)
                self.signal_source.ack(signal)
                continue
            if previous_status in {"CLAIMED", "SUBMITTING", "RECONCILIATION_REQUIRED"}:
                # A claim or durable submit intent can outlive the process
                # which created it.  It is never safe to call passorder again
                # until QMT has proved the exact prior broker submission.
                if self._reconcile_pending_submission(signal, previous):
                    emit("signal.reconcile.ack", critical=True, signal_id=signal.signal_id,
                         account_id=signal.account_id, outcome="success", state=previous_status)
                    self.signal_source.ack(signal)
                else:
                    emit("signal.reconcile.pending", critical=True, signal_id=signal.signal_id,
                         account_id=signal.account_id, outcome="unknown", state=previous_status)
                continue
            if not self.state_store.claim(signal, self.consumer_id):
                emit("signal.claim", signal_id=signal.signal_id, account_id=signal.account_id,
                     outcome="rejected")
                continue
            emit("signal.claim", critical=True, signal_id=signal.signal_id,
                 account_id=signal.account_id, outcome="success")
            try:
                self._handle_signal(signal, now, positions)
            except _SubmissionStateUnconfirmed:
                # The durable pre-submit intent remains in Redis.  A later
                # XAUTOCLAIM tick must reconcile it, not turn it into another
                # passorder call or a fake failed acknowledgement.
                emit("signal.submission.unknown", critical=True, signal_id=signal.signal_id,
                     account_id=signal.account_id, outcome="unknown")
                continue
            except Exception as exc:
                emit("signal.failed", critical=True, signal_id=signal.signal_id,
                     account_id=signal.account_id, outcome="rejected", error_type=type(exc).__name__)
                self.state_store.mark_finished(signal.signal_id, "FAILED", str(exc))
                self.signal_source.ack(signal)

        self.sync_positions("tick", now=now)

    def _handle_signal(self, signal, now, positions):
        decision = validate_signal(signal, now, positions)
        if not decision.allowed:
            emit("signal.risk_gate", critical=True, signal_id=signal.signal_id,
                 account_id=signal.account_id, outcome="rejected", reason=decision.reason)
            self.state_store.mark_finished(signal.signal_id, "SKIPPED", decision.reason)
            self.signal_source.ack(signal)
            return

        price = build_order_price(
            self.market_data,
            decision.stock_code,
            signal.action.value,
            price_type=signal.price_type,
            fixed_price=signal.price,
        )
        request = OrderRequest(
            signal_id=signal.signal_id,
            account_id=signal.account_id,
            action=signal.action.value,
            stock_code=decision.stock_code,
            volume=decision.volume,
            price=price,
            price_type="LIMIT",
            strategy_name=signal.strategy_name,
            # An empty remark is not an identity.  The signal id is stable
            # across Redis pending recovery and is therefore the fallback.
            remark=signal.remark or signal.signal_id,
        )
        mark_submitting = getattr(self.state_store, "mark_submitting", None)
        if not callable(mark_submitting):
            raise RuntimeError("state_store must persist submit intent before broker submission")
        mark_submitting(signal.signal_id, request)
        emit("signal.intent.persisted", critical=True, signal_id=signal.signal_id,
             account_id=signal.account_id, client_submit_id=request.remark,
             stock_code=request.stock_code, action=request.action, volume=request.volume,
             price=request.price, strategy_name=request.strategy_name, outcome="success")
        try:
            with span("signal.gateway_submit", signal_id=signal.signal_id,
                      client_submit_id=request.remark, account_id=signal.account_id) as trace:
                result = self.order_gateway.submit(request)
                trace["outcome"] = "success"
            self.state_store.mark_submitted(signal.signal_id, result)
        except Exception as exc:
            emit("signal.submission.receipt", critical=True, signal_id=signal.signal_id,
                 account_id=signal.account_id, client_submit_id=request.remark,
                 outcome="unknown", error_type=type(exc).__name__)
            raise _SubmissionStateUnconfirmed(str(exc))
        emit("signal.submission.receipt", critical=True, signal_id=signal.signal_id,
             account_id=signal.account_id, client_submit_id=request.remark,
             order_sys_id=getattr(result, "order_sys_id", None), outcome="success",
             status=getattr(result, "status", ""))
        self.signal_source.ack(signal)
        emit("signal.ack", critical=True, signal_id=signal.signal_id,
             account_id=signal.account_id, outcome="success")

    def _reconcile_pending_submission(self, signal, previous):
        """Return true only when QMT proves the stored intent was submitted.

        Unknown, unavailable, and conflicting lookup results deliberately stay
        pending.  Retrying a write after this crash window would turn an
        ambiguous broker outcome into a duplicate order.
        """
        identity = str((previous or {}).get("user_order_id") or signal.remark or signal.signal_id)
        stock_code = str((previous or {}).get("stock_code") or "")
        action = str((previous or {}).get("action") or "").upper()
        strategy_name = str((previous or {}).get("strategy_name") or signal.strategy_name or "")
        try:
            volume = int((previous or {}).get("volume"))
            price = float((previous or {}).get("price"))
        except (TypeError, ValueError):
            # A legacy CLAIMED record has no durable exact intent.  It may be
            # queried by identity but is never eligible for automatic replay.
            return False
        query = getattr(self.order_gateway, "query_submission_identities_strict", None)
        if not callable(query):
            return False
        try:
            orders, trades = query(signal.account_id, strategy_name)
        except Exception:
            emit("signal.reconcile.query", critical=True, signal_id=signal.signal_id,
                 account_id=signal.account_id, outcome="unknown")
            return False
        for candidate in list(orders or []) + list(trades or []):
            candidate_identity = str(
                getattr(candidate, "user_order_id", "")
                or getattr(candidate, "remark", "")
                or ""
            )
            if candidate_identity != identity:
                continue
            if not self._matches_submit_intent(
                candidate, stock_code, action, volume, price, strategy_name
            ):
                return False
            self.state_store.mark_submitted(
                signal.signal_id,
                OrderSubmitResult(
                    "CONFIRMED", identity,
                    str(getattr(candidate, "order_sys_id", "") or "") or None,
                    "broker order reconciled from durable submit intent",
                ),
            )
            emit("signal.reconcile.strict", critical=True, signal_id=signal.signal_id,
                 account_id=signal.account_id, client_submit_id=identity,
                 order_sys_id=getattr(candidate, "order_sys_id", None), outcome="success")
            return True
        emit("signal.reconcile.strict", critical=True, signal_id=signal.signal_id,
             account_id=signal.account_id, client_submit_id=identity, outcome="unknown")
        return False

    @staticmethod
    def _matches_submit_intent(candidate, stock_code, action, volume, price, strategy_name):
        if normalize_stock_code(getattr(candidate, "stock_code", "")) != normalize_stock_code(stock_code):
            return False
        if str(getattr(candidate, "action", "") or "").upper() != action:
            return False
        if int(getattr(candidate, "volume", 0) or 0) != volume:
            return False
        candidate_price = float(getattr(candidate, "price", 0) or 0)
        if abs(candidate_price - price) > 0.00000001:
            return False
        candidate_strategy = str(getattr(candidate, "strategy_name", "") or "")
        return candidate_strategy == strategy_name

    def on_init(self, runtime):
        return None

    def on_order_event(self, event):
        return None

    def on_trade_event(self, event):
        self.sync_positions("trade_event")

    def sync_positions(self, reason, now=None):
        now = now or _dt.datetime.now()
        emit("signal.position_sync", account_id=self.account_id, outcome="started", reason=reason)
        asset = self.position_provider.get_asset(self.account_id)
        positions = self.position_provider.get_positions(self.account_id)
        snapshot = AccountSnapshot(
            account_id=self.account_id,
            asset=asset,
            positions=positions,
            reason=reason,
            updated_at=now,
        )
        self.position_sync_sink.publish(snapshot)
        emit("signal.position_sync", account_id=self.account_id, outcome="success", reason=reason)
