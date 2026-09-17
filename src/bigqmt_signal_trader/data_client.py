"""Direct Big QMT market-data client."""

from ._client_support import *
from .rpc_client import BigQmtRpcClient
from .telemetry import emit, span

class BigQmtDataClient:
    # Full Big QMT exposes only the runtime-injected, single-symbol history
    # downloader.
    native_history_download_mode = "global_single"

    def __init__(self, client):
        self.client = client
        self._cache_obj = None

    def _local_cache(self):
        cfg = dict(getattr(self.client, "local_cache_config", {}) or {})
        if not _bool_value(cfg.get("enabled"), True):
            return None
        if self._cache_obj is None:
            self._cache_obj = LocalMarketCache(cache_dir=cfg.get("dir"), fmt=cfg.get("format", "auto"))
        return self._cache_obj

    def _call(self, method, **params):
        return self.client.call(method, params)

    def get_full_tick(self, code_list):
        codes = list(code_list or [])
        if not codes:
            emit("market.full_tick", outcome="empty", code_count=0)
            return {}
        cache_config = dict(getattr(self.client, "full_tick_cache_config", {}) or {})
        if _bool_value(cache_config.get("enabled"), False):
            redis_client = self.client._redis()
            request_full_tick_cache(
                redis_client,
                self.client.account_id,
                codes,
                demand_ttl_seconds=cache_config.get("demand_ttl_seconds", 10),
                cache_ttl_seconds=cache_config.get("cache_ttl_seconds", 10),
            )
            data = wait_full_tick_cache(
                redis_client,
                self.client.account_id,
                codes,
                max_age_seconds=cache_config.get("cache_ttl_seconds", 10),
                wait_seconds=cache_config.get("wait_seconds", 3.5),
                poll_interval_seconds=cache_config.get("poll_interval_seconds", 0.2),
            )
            if data is not None:
                emit("market.full_tick.cache", outcome="success", code_count=len(codes))
                return data
            upper_codes = {str(code).strip().upper() for code in codes}
            if upper_codes & {"SH", "SZ", "BJ", "HK"}:
                # Whole-market snapshots must stay on the demand cache. A live RPC
                # here would ship ~50k rows on every miss, so surface the timeout.
                raise TimeoutError("full tick redis cache timeout: %s" % ",".join(str(code) for code in codes))
            # Symbol-list miss (cold start / expired snapshot): fall back to a live
            # RPC so the first call is ~ms instead of a hard wait_seconds stall.
            with span("market.full_tick.rpc_fallback", code_count=len(codes)) as result:
                value = self.client.call("get_full_tick", {"codes": codes}) or {}
                result["outcome"] = "success" if value else "empty"
                return value
        upper_codes = {str(code).strip().upper() for code in codes}
        timeout_seconds = 30 if upper_codes & {"SH", "SZ", "BJ", "HK"} else None
        with span("market.full_tick.rpc", code_count=len(codes)) as result:
            value = self.client.call("get_full_tick", {"codes": codes}, timeout_seconds=timeout_seconds) or {}
            result["outcome"] = "success" if value else "empty"
            return value

    def get_instrument_detail(self, stock_code):
        return self.client.call("get_instrument_detail", {"code": stock_code}) or {}

    def get_instrument_detail_list(self, stock_list, iscomplete=False):
        return {
            stock_code: self.get_instrument_detail(stock_code)
            for stock_code in list(stock_list or [])
        }

    def get_instrumentdetail(self, stock_code):
        return self.get_instrument_detail(stock_code)

    def get_instrument_type(self, stock_code, variety_list=None):
        return self._call("get_instrument_type", code=stock_code, variety_list=variety_list)

    def get_stock_list_in_sector(self, sector_name, real_timetag=-1):
        name = str(sector_name or "")
        try:
            return self._call("get_stock_list_in_sector", sector_name=sector_name, real_timetag=real_timetag) or []
        except Exception:
            pass
        if name in ("沪深A股", "沪深A股".encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")):
            ticks = self.get_full_tick(["SH", "SZ"])
            return sorted(code for code in ticks.keys() if _is_hs_a_share(code))
        raise NotImplementedError("sector is not supported by Big QMT: %s" % sector_name)

    def get_market_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        return self._call(
            "get_market_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )

    def get_market_data_ex(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        # Live pull over RPC. Cache-through: whatever we fetch is written to the
        # local cache (keyed by dividend_type), so it stays the latest — important
        # for 前复权 (front-adjusted) data, whose history re-scales on each dividend.
        codes = list(stock_list or [])
        with span("market.data_client.rpc", period=period, code_count=len(codes),
                  requested_count=count, explicit_window=bool(end_time)) as result:
            data = self._call(
                "get_market_data_ex",
                field_list=list(field_list or []), stock_list=codes, period=period,
                start_time=start_time, end_time=end_time, count=count,
                dividend_type=dividend_type, fill_data=fill_data,
            )
            result["outcome"] = "success" if data else "empty"
        raw_rows = sum(
            int(getattr(frame, "shape", (0,))[0])
            for frame in data.values()
        ) if isinstance(data, dict) else 0
        if not fill_data and isinstance(data, dict):
            # Older embedded adapters dropped fill_data=False. Never cache their
            # suspension padding as real bars. Ordinary idle minutes remain valid.
            import pandas as pd

            data = dict(data)
            for code, frame in data.items():
                if not isinstance(frame, pd.DataFrame) or not {
                    "suspendFlag", "volume", "amount"
                }.issubset(frame.columns):
                    continue
                padding = (
                    pd.to_numeric(frame["suspendFlag"], errors="coerce").eq(1)
                    & pd.to_numeric(frame["volume"], errors="coerce").eq(0)
                    & pd.to_numeric(frame["amount"], errors="coerce").eq(0)
                )
                data[code] = frame.loc[~padding].copy()
        returned_rows = sum(
            int(getattr(frame, "shape", (0,))[0])
            for frame in data.values()
        ) if isinstance(data, dict) else 0
        emit("market.data_client.result",
             outcome="success" if returned_rows else "empty",
             period=period, returned_codes=len(data) if isinstance(data, dict) else 0,
             returned_rows=returned_rows, filtered_rows=max(0, raw_rows - returned_rows),
             fill_data=bool(fill_data))
        cache = self._local_cache()
        if cache is not None and isinstance(data, dict):
            cached = 0
            for code, df in data.items():
                try:
                    cache.write(code, period, df, dividend_type=dividend_type)
                    cached += 1
                except Exception:
                    emit("market.data_client.local_cache_write", outcome="unknown", period=period)
            emit("market.data_client.local_cache_write", outcome="success", period=period,
                 code_count=cached)
        return data

    def get_market_data_ex_scoped(self, stock_list, start_time, end_time, count=3,
                                  timeout_seconds=None):
        # A distinct RPC method makes old embedded runtimes fail closed instead
        # of ignoring new kwargs and continuing to leak implicit subscriptions.
        params = {
            "stock_list": list(stock_list),
            "start_time": start_time,
            "end_time": end_time,
            "count": count,
        }
        if timeout_seconds is None:
            return self._call("get_market_data_ex_scoped", **params)
        return self.client.call(
            "get_market_data_ex_scoped", params, timeout_seconds=timeout_seconds
        )

    def get_market_data3(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        return self.get_market_data_ex(
            field_list=field_list,
            stock_list=stock_list,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )
    def get_full_kline(self, stock_code, period="1d", start_time="", end_time=""):
        data = self.get_market_data_ex(
            field_list=[],
            stock_list=[stock_code],
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type="none",
            fill_data=True,
        )
        if isinstance(data, dict):
            return data.get(stock_code)
        return data

    def get_local_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        data_dir=None,
    ):
        """Read bars from the CLIENT-side local cache — no RPC to Big QMT.

        Populate the cache first with get_market_data_ex(...). Returns a dict
        {code: DataFrame}. A cache-missed code is omitted, unless
        local_cache_fallback_rpc is enabled (then it is fetched + cached).
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        cache = self._local_cache()
        if cache is None:
            # Cache disabled -> behave like a plain RPC local-data read.
            emit("market.data_client.local_data_source", outcome="success", source="rpc",
                 code_count=len(codes), period=period)
            return self._call(
                "get_local_data",
                field_list=list(field_list or []),
                stock_list=codes,
                period=period,
                start_time=start_time,
                end_time=end_time,
                count=count,
                dividend_type=dividend_type,
                fill_data=fill_data,
                data_dir=data_dir,
            )
        fields = list(field_list or [])
        result = {}
        missing = []
        for code in codes:
            df = cache.read(code, period, start_time, end_time, count, dividend_type=dividend_type)
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                result[code] = self._select_fields(df, fields)
            else:
                missing.append(code)
        if missing and _bool_value(self.client.local_cache_config.get("fallback_rpc"), False):
            fetched = self._pull_and_cache(missing, period, start_time, end_time, count, dividend_type)
            for code in missing:
                df = fetched.get(code)
                if df is not None and getattr(df, "shape", (0,))[0] > 0:
                    result[code] = self._select_fields(df, fields)
        emit("market.data_client.local_data_source",
             outcome="partial" if missing else ("success" if result else "empty"),
             source="local_cache" if not missing else "local_cache_plus_rpc",
             period=period, hit_count=len(result), missing_count=len(missing))
        return result

    @staticmethod
    def _select_fields(df, fields):
        if not fields:
            return df
        try:
            keep = [c for c in df.columns if c in fields or c in _TIME_COL_NAMES]
            return df[keep] if keep else df
        except Exception:
            return df

    def _pull_and_cache(self, codes, period, start_time, end_time, count, dividend_type="none"):
        """Fetch codes over RPC (get_market_data_ex already caches them)."""
        data = self.get_market_data_ex(
            field_list=DEFAULT_DOWNLOAD_FIELDS,
            stock_list=list(codes),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
        )
        out = {}
        for code in codes:
            df = data.get(code) if isinstance(data, dict) else None
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                out[code] = df
        return out

    def subscribe_quote(self, stock_code, period="1d", start_time="", end_time="", count=0, callback=None):
        seq = int(
            self.client.call(
                "subscribe_quote",
                {"stock_code": stock_code, "period": period},
            )
            or 0
        )
        if seq <= 0:
            emit("market.subscription.create", outcome="unsupported", period=period)
            raise RuntimeError("native quote subscription unavailable")
        if callback is not None:
            self.client.register_quote_callback(seq, stock_code, callback)
        emit("market.subscription.create", outcome="success", period=period,
             has_callback=bool(callback))
        return seq

    def unsubscribe_quote(self, seq):
        try:
            value = self.client.call("unsubscribe_quote", {"seq": int(seq)})
            emit("market.subscription.release", outcome="success")
            return value
        finally:
            self.client.unregister_quote_callback(seq)

    def get_divid_factors(self, stock_code, start_time="", end_time=""):
        return self._call("get_divid_factors", stock_code=stock_code, start_time=start_time, end_time=end_time)

    def download_history_data(self, stock_code, period, start_time="", end_time="", incrementally=None, dividend_type="none", timeout_seconds=None):
        """Invoke the full Big QMT runtime-global single-symbol downloader.

        ``timeout_seconds`` is transport metadata and is never serialized into
        the official four-argument QMT payload. Other runtime-only options are
        accepted as bridge-only metadata and intentionally ignored.
        """
        caller = getattr(self.client, "call", None)
        if not callable(caller):
            raise NotImplementedError(
                "native_history_download_unavailable: "
                "bigqmt_global_download_history_data_unavailable"
            )
        try:
            return caller(
                "download_history_data",
                {
                    "stock_code": stock_code,
                    "period": period,
                    "start_time": start_time,
                    "end_time": end_time,
                },
                timeout_seconds=timeout_seconds,
            )
        except RuntimeError as exc:
            message = str(exc)
            if (
                "rpc method is not allowed: download_history_data" in message
                or "bigqmt_global_download_history_data_unavailable" in message
            ):
                raise NotImplementedError(
                    "native_history_download_unavailable: "
                    "bigqmt_global_download_history_data_unavailable"
                )
            raise

    def download_cb_data(self):
        return self._call("download_cb_data")

    def download_history_contracts(self):
        return self._call("download_history_contracts")

    def download_index_weight(self):
        return self._call("download_index_weight")

    def download_sector_data(self):
        return self._call("download_sector_data")

    def local_cache_stats(self):
        """Return (cached files, periods) for the client-side local cache."""
        cache = self._local_cache()
        return cache.stats() if cache is not None else (0, [])

    def get_trading_dates(self, market, start_time="", end_time="", count=-1):
        return self._call("get_trading_dates", market=market, start_time=start_time, end_time=end_time, count=count)

    def get_period_list(self):
        return ["tick", "1m", "5m", "15m", "30m", "60m", "1d"]

    def get_trading_period(self, stock_code):
        return self.get_trade_times(stock_code)

    def get_holidays(self):
        return self._call("get_holidays")

    def download_holiday_data(self, incrementally=True):
        return self._call("download_holiday_data", incrementally=incrementally)

    def get_ipo_info(self, start_time="", end_time=""):
        return self._call("get_ipo_info", start_time=start_time, end_time=end_time)

    def get_etf_info(self):
        return self._call("get_etf_info")

    def download_etf_info(self):
        return self._call("download_etf_info")

    def get_option_list(self, undl_code, dedate, opttype="", isavailavle=False):
        return self._call("get_option_list", undl_code=undl_code, dedate=dedate, opttype=opttype, isavailavle=isavailavle)

    def get_his_option_list(self, undl_code, dedate):
        return self._call("get_his_option_list", undl_code=undl_code, dedate=dedate)

    def get_his_option_list_batch(self, undl_code, start_time="", end_time=""):
        return self._call("get_his_option_list_batch", undl_code=undl_code, start_time=start_time, end_time=end_time)

    def get_financial_data(self, stock_list, table_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_financial_table_list(self):
        return [
            "Balance",
            "Income",
            "CashFlow",
            "Capital",
            "Holdernum",
            "Top10holder",
            "Top10flowholder",
            "Pershareindex",
        ]

    def getfindata(self, table, field="", session=""):
        return self._call("getfindata", table=table, field=field, session=session)

    def download_financial_data(self, stock_list, table_list=None, start_time="", end_time="", incrementally=None):
        return self._call(
            "download_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            incrementally=incrementally,
        )

    def download_financial_data2(self, stock_list, table_list=None, start_time="", end_time="", callback=None):
        result = self._call(
            "download_financial_data2",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
        )
        if callback is not None:
            callback(result)
        return result

    def get_sector_list(self):
        return self._call("get_sector_list")

    def get_sector_info(self, sector_name=""):
        return self._call("get_sector_info", sector_name=sector_name)

    def get_markets(self):
        return self._call("get_markets")

    def get_market_last_trade_date(self, market):
        return self._call("get_market_last_trade_date", market=market)

    def call_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None):
        return self._call(
            "call_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )

    def call_formula_batch(self, formula_name, stock_codes, period, start_time="", end_time="", count=-1, dividend_type=None, **params):
        result = {}
        for stock_code in list(stock_codes or []):
            try:
                data = self.call_formula(
                    formula_name,
                    stock_code,
                    period,
                    start_time,
                    end_time,
                    count,
                    dividend_type,
                    params,
                )
                result[stock_code] = {"status": "ok", "data": data}
            except Exception as exc:
                result[stock_code] = {
                    "status": "error",
                    "data": None,
                    "reason_code": "bigqmt_formula_call_failed",
                    "message": str(exc),
                }
        return result

    def subscribe_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None, callback=None):
        result = self._call(
            "subscribe_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )
        if callback is not None:
            callback(result)
        return result

    def unsubscribe_formula(self, request_id):
        return self._call("unsubscribe_formula", request_id=request_id)

    def get_formula_result(self, request_id, start_time="", end_time="", count=-1, timeout_second=-1):
        return self._call(
            "get_formula_result",
            request_id=request_id,
            start_time=start_time,
            end_time=end_time,
            count=count,
            timeout_second=timeout_second,
        )

    def gen_factor_index(self, data_name, formula_name, vars, sector_list, start_time="", end_time="", period="1d", dividend_type="none"):
        return self._call(
            "gen_factor_index",
            data_name=data_name,
            formula_name=formula_name,
            vars=vars,
            sector_list=list(sector_list or []),
            start_time=start_time,
            end_time=end_time,
            period=period,
            dividend_type=dividend_type,
        )

    # ------------------------------------------------------------------
    # 扩展行情/基本面方法（对应 ContextInfo 方法，走 RPC 白名单）。
    # 仅对最常用的显式声明签名；其余通过 __getattr__ 自动转发。
    # ------------------------------------------------------------------

    def get_longhubang(self, stock_list=None, start_time="", end_time="", count=-1):
        return self._call(
            "get_longhubang",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            count=count,
        )

    def get_top10_share_holder(self, stock_list, data_name, start_time, end_time, report_type="report_time"):
        return self._call(
            "get_top10_share_holder",
            stock_list=list(stock_list or []),
            data_name=data_name,
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_holder_num(self, stock_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_holder_num",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_turnover_rate(self, stock_code=None, start_time="19720101", end_time="22010101"):
        return self._call(
            "get_turnover_rate",
            stock_code=list(stock_code or []),
            start_time=start_time,
            end_time=end_time,
        )

    def get_industry(self, industry_name):
        return self._call("get_industry", industry_name=industry_name)

    def bsm_price(self, opt_type, target_price, strike_price, risk_free, sigma, days, dividend=0):
        return self._call(
            "bsm_price",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            risk_free=risk_free,
            sigma=sigma,
            days=days,
            dividend=dividend,
        )

    def bsm_iv(self, opt_type, target_price, strike_price, option_price, risk_free, days, dividend=0):
        return self._call(
            "bsm_iv",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            option_price=option_price,
            risk_free=risk_free,
            days=days,
            dividend=dividend,
        )

    def get_option_iv(self, opt_code):
        return self._call("get_option_iv", opt_code=opt_code)

    def get_option_detail_data(self, stockcode):
        return self._call("get_option_detail_data", stockcode=stockcode)

    def get_option_undl_data(self, undl_code_ref=""):
        return self._call("get_option_undl_data", undl_code_ref=undl_code_ref)

    def get_option_undl(self, opt_code):
        return self._call("get_option_undl", opt_code=opt_code)

    def get_raw_financial_data(self, field_list, stock_list, start_time, end_time, report_type="report_time", data_type="dict"):
        return self._call(
            "get_raw_financial_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
            data_type=data_type,
        )

    def get_factor_data(self, field_list, stock_list, start_date, end_date):
        return self._call(
            "get_factor_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_date=start_date,
            end_date=end_date,
        )

    def get_north_finance_change(self, period):
        return self._call("get_north_finance_change", period=period)

    def get_hkt_statistics(self, stock_code):
        return self._call("get_hkt_statistics", stock_code=stock_code)

    def get_hkt_details(self, stock_code):
        return self._call("get_hkt_details", stock_code=stock_code)

    def get_hkt_exchange_rate(self, account_id="", account_type=""):
        return self._call(
            "get_hkt_exchange_rate",
            account_id=account_id,
            account_type=account_type,
        )

    def create_sector(self, sector_name, stock_list):
        return self._call("create_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def get_stock_name(self, stock):
        return self._call("get_stock_name", stock=stock)

    def get_last_volume(self, stock):
        return self._call("get_last_volume", stock=stock)

    def get_open_date(self, stock):
        return self._call("get_open_date", stock=stock)

    def get_contract_expire_date(self, stock):
        return self._call("get_contract_expire_date", stock=stock)

    def get_contract_multiplier(self, stockcode):
        return self._call("get_contract_multiplier", stockcode=stockcode)

    def get_total_share(self, stockcode):
        return self._call("get_total_share", stockcode=stockcode)

    def get_svol(self, stock):
        return self._call("get_svol", stock=stock)

    def get_bvol(self, stock):
        return self._call("get_bvol", stock=stock)

    def get_risk_free_rate(self, index=-1):
        return self._call("get_risk_free_rate", index=index)

    def get_basket(self, basket_name):
        return self._call("get_basket", basket_name=basket_name)

    def get_etf_iopv(self, stockcode):
        return self._call("get_etf_iopv", stockcode=stockcode)

    def get_industry_name_of_stock(self, industry_type, stock):
        return self._call(
            "get_industry_name_of_stock",
            industry_type=industry_type,
            stock=stock,
        )

    def get_market_time(self, market):
        return self._call("get_market_time", market=market)

    def is_suspended_stock(self, stock):
        return self._call("is_suspended_stock", stock=stock)

    def get_close_price(self, market, stock_code, real_timetag, period=86400000, divid_type=0):
        return self._call(
            "get_close_price",
            market=market,
            stock_code=stock_code,
            real_timetag=real_timetag,
            period=period,
            divid_type=divid_type,
        )

    def get_main_contract(self, code_market):
        return self._call("get_main_contract", code_market=code_market)

    def get_his_contract_list(self, market):
        return self._call("get_his_contract_list", market=market)

    def get_date_location(self, date):
        return self._call("get_date_location", date=date)

    def get_his_st_data(self, stock_code):
        return self._call("get_his_st_data", stock_code=stock_code)

    def get_his_index_data(self, stock_code):
        return self._call("get_his_index_data", stock_code=stock_code)

    def call_method(self, method, **params):
        """Generic escape hatch: call any RPC market-data method by name.

        Use this for ContextInfo methods that don't have an explicit wrapper
        above (e.g. ``xtdata.call_method("get_last_close", stock="000001.SZ")``,
        ``xtdata.call_method("get_float_caps", stockcode="000001.SZ")``). The
        full list of callable methods is in ``MARKET_DATA_METHODS``.
        """
        return self._call(method, **params)

    # ------------------------------------------------------------------
    # L2 行情（需 L2 权限 + 原生 xtdata SDK 行情服务）
    # ------------------------------------------------------------------

    def get_l2_quote(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_quote", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_order(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_order", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_transaction(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_transaction", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    # ------------------------------------------------------------------
    # 指数权重 / 交易日历 / 交易时段 / 可转债 / 品种判断
    # ------------------------------------------------------------------

    def get_index_weight(self, index_code):
        return self._call("get_index_weight", index_code=index_code)

    def get_trading_calendar(self, market, start_time="", end_time="", tradetimes=False):
        return self._call("get_trading_calendar", market=market, start_time=start_time,
                          end_time=end_time, tradetimes=tradetimes)

    def get_trade_times(self, stockcode):
        return self._call("get_trade_times", stockcode=stockcode)

    def get_cb_info(self, stockcode):
        return self._call("get_cb_info", stockcode=stockcode)

    def is_stock_type(self, stock, tag):
        return self._call("is_stock_type", stock=stock, tag=tag)

    # ------------------------------------------------------------------
    # 板块增删
    # ------------------------------------------------------------------

    def add_sector(self, sector_name, stock_list):
        return self._call("add_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def remove_sector(self, sector_name):
        return self._call("remove_sector", sector_name=sector_name)

    # ------------------------------------------------------------------
    # 时间戳转换（纯计算）
    # ------------------------------------------------------------------

    @staticmethod
    def datetime_to_timetag(datetime_str, format="%Y%m%d%H%M%S"):
        import datetime as _dt
        try:
            return int(_dt.datetime.strptime(str(datetime_str), format).timestamp() * 1000)
        except Exception:
            return 0

    @staticmethod
    def timetag_to_datetime(timetag, format):
        import datetime as _dt
        try:
            return _dt.datetime.fromtimestamp(int(timetag) / 1000.0).strftime(format)
        except Exception:
            return ""

    @staticmethod
    def timetagToDateTime(timetag, format):
        return BigQmtDataClient.timetag_to_datetime(timetag, format)
