"""Independent FormulaServer connections, without real QMT or trading calls."""

import importlib.util
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch


SOURCE = Path(__file__).parents[1] / "src/bigqmt_signal_trader/formula_server.py"
spec = importlib.util.spec_from_file_location("formula_history_test_subject", SOURCE)
formula = importlib.util.module_from_spec(spec)
spec.loader.exec_module(formula)


class FakeClient:
    host = "127.0.0.1"
    port = 58600
    timeout_seconds = 1

    def __init__(self, *, barrier=None, failing_code=None, **kwargs):
        self.barrier = barrier
        self.failing_code = failing_code
        self.calls = []
        self.closed = False

    def request(self, method, params):
        self.calls.append((method, params))
        if self.barrier is not None:
            self.barrier.wait(timeout=2)
        if self.failing_code in params.get("stockCodes", []):
            raise formula.FormulaServerUnavailable("test connection failure")
        if method != "getMarketData":
            return {"result": ["600000.SH"]}
        result = []
        for code in params["stockCodes"]:
            result.extend([code, ["20260904 15:00:00", ["close", float(code[:6])]]])
        return {"result": result}

    def close(self):
        self.closed = True


class FormulaHistoryConcurrencyTests(unittest.TestCase):
    def params(self, **updates):
        params = {
            "field_list": ["close"],
            "stock_list": ["600000.SH", "000001.SZ", "600036.SH", "000333.SZ"],
            "period": "1m", "start_time": "20260904000000",
            "end_time": "20260904235959", "count": 241, "dividend_type": "none",
        }
        params.update(updates)
        return params

    def router(self, client, workers=2):
        router = formula.FormulaServerRouter(
            client=client, config={"history_read_workers": workers},
        )
        self.addCleanup(router.close)
        return router

    def test_two_connections_overlap_and_preserve_exact_payload_and_request(self):
        barrier = threading.Barrier(2)
        first, second = FakeClient(barrier=barrier), FakeClient(barrier=barrier)
        params = self.params()
        expected = self.router(FakeClient(), workers=1).call("get_market_data_ex", params)
        with patch.object(formula, "FormulaServerClient", return_value=second) as factory:
            router = self.router(first)
            actual = router.call("get_market_data_ex", params)
            self.assertEqual(expected, actual)
            self.assertEqual(list(actual), params["stock_list"])
            self.assertEqual(len(first.calls), 1)
            self.assertEqual(len(second.calls), 1)
            self.assertEqual(first.calls[0][1]["stockCodes"], params["stock_list"][:2])
            self.assertEqual(second.calls[0][1]["stockCodes"], params["stock_list"][2:])
            for client in (first, second):
                wire = client.calls[0][1]
                self.assertEqual(wire["count"], 241)
                self.assertEqual(wire["fields"], ["close"])
                self.assertEqual(wire["startTime"], params["start_time"])
                self.assertEqual(wire["endTime"], params["end_time"])
            self.assertEqual(factory.call_count, 1)
            self.assertEqual(router.stats()["parallel_history_calls"], 1)

    def test_single_stock_and_explicit_serial_setting_do_not_create_connection(self):
        for workers, codes in ((1, self.params()["stock_list"]), (2, ["600000.SH"])):
            with self.subTest(workers=workers, codes=codes):
                client = FakeClient()
                with patch.object(formula, "FormulaServerClient") as factory:
                    self.router(client, workers).call("get_market_data_ex", self.params(stock_list=codes))
                factory.assert_not_called()
                self.assertEqual(len(client.calls), 1)

    def test_reference_reads_and_tick_do_not_fan_out(self):
        client = FakeClient()
        router = self.router(client)
        with patch.object(formula, "FormulaServerClient") as factory:
            router.call("get_stock_list_in_sector", {"sector_name": "test"})
            router.call("get_market_data_ex", self.params(period="tick"))
        factory.assert_not_called()
        self.assertEqual(len(client.calls), 2)

    def test_adjusted_and_download_calls_are_not_routed(self):
        client = FakeClient()
        router = self.router(client)
        with self.assertRaises(formula.Unroutable):
            router.call("get_market_data_ex", self.params(dividend_type="front"))
        with self.assertRaises(formula.Unroutable):
            router.call("download_history_data2", self.params())
        self.assertEqual(client.calls, [])

    def test_failure_waits_for_other_read_and_returns_no_partial_payload(self):
        entered, release = threading.Event(), threading.Event()
        first = FakeClient(failing_code="600000.SH")

        class SlowClient(FakeClient):
            def request(self, method, params):
                entered.set()
                if not release.wait(timeout=2):
                    raise RuntimeError("test release missing")
                return super().request(method, params)

        second = SlowClient()
        with patch.object(formula, "FormulaServerClient", return_value=second):
            router = self.router(first)
            with ThreadPoolExecutor(max_workers=1) as caller:
                pending = caller.submit(router.call, "get_market_data_ex", self.params())
                try:
                    self.assertTrue(entered.wait(timeout=1))
                    self.assertFalse(pending.done(), "must drain sibling before RPC fallback")
                finally:
                    release.set()
                with self.assertRaises(formula.Unroutable):
                    pending.result(timeout=2)
        self.assertEqual(router.hits, 0)

    def test_expired_queued_history_worker_does_not_invoke_formula(self):
        entered, release = threading.Event(), threading.Event()

        class BlockingClient(FakeClient):
            def request(self, method, params):
                entered.set()
                release.wait(timeout=2)
                return super().request(method, params)

        first, second = BlockingClient(), FakeClient()
        router = self.router(first)
        router._history_executor = ThreadPoolExecutor(max_workers=1)
        router._history_second_client = second
        try:
            with self.assertRaises(formula.Unroutable):
                router.call(
                    "get_market_data_ex", self.params(),
                    deadline_monotonic=time.monotonic() + 0.02,
                )
            self.assertTrue(entered.wait(timeout=1))
            self.assertEqual(second.calls, [])
        finally:
            release.set()

    def test_pool_and_connections_are_reused_and_closed(self):
        first, second = FakeClient(), FakeClient()
        with patch.object(formula, "FormulaServerClient", return_value=second) as factory:
            router = self.router(first)
            router.call("get_market_data_ex", self.params())
            executor = router._history_executor
            router.call("get_market_data_ex", self.params())
            self.assertIs(router._history_executor, executor)
            self.assertEqual(factory.call_count, 1)
            router.close()
            router.close()
        self.assertTrue(first.closed)
        self.assertTrue(second.closed)
        self.assertTrue(all(not thread.is_alive() for thread in executor._threads))

    def test_overlapping_batches_still_use_only_two_connections(self):
        first = FakeClient(barrier=threading.Barrier(2))
        second = FakeClient(barrier=first.barrier)
        with patch.object(formula, "FormulaServerClient", return_value=second) as factory:
            router = self.router(first)
            with ThreadPoolExecutor(max_workers=2) as callers:
                futures = [callers.submit(router.call, "get_market_data_ex", self.params()) for _ in range(2)]
                results = [future.result(timeout=3) for future in futures]
            self.assertEqual(results[0], results[1])
            self.assertEqual(factory.call_count, 1)
            self.assertEqual(len(first.calls), 2)
            self.assertEqual(len(second.calls), 2)

    def test_bridge_setting_supports_serial_rollback_and_validates_limit(self):
        from qmt_bridge.server.bigqmt import BigQmtRuntime
        from qmt_bridge.server.config import Settings

        with patch.dict(os.environ, {"QMT_BRIDGE_FORMULA_HISTORY_READ_WORKERS": "1"}):
            settings = Settings.from_env()
        self.assertEqual(settings.formula_history_read_workers, 1)
        runtime = BigQmtRuntime(settings)
        self.assertEqual(runtime._client_config()["formula_server"]["history_read_workers"], 1)
        with self.assertRaisesRegex(ValueError, "HISTORY_READ_WORKERS"):
            BigQmtRuntime(Settings(formula_history_read_workers=3)).connect()

    def test_runtime_close_closes_history_pool(self):
        from types import SimpleNamespace
        from qmt_bridge.server.bigqmt import BigQmtRuntime
        from qmt_bridge.server.config import Settings

        first, second = FakeClient(), FakeClient()
        with patch.object(formula, "FormulaServerClient", return_value=second):
            router = self.router(first)
            router.call("get_market_data_ex", self.params())
        runtime = BigQmtRuntime(Settings())
        runtime.client = SimpleNamespace(_formula_router_instance=router)
        runtime.close()
        self.assertTrue(first.closed)
        self.assertTrue(second.closed)


if __name__ == "__main__":
    unittest.main()
