"""Offline benchmark: synthetic DAT tail reads and shared quote fanout.

Run with the project's server environment. No QMT, account, or network access.
The reference is full parsing followed by tail(), not a prior deployed binary.
"""

import argparse
import asyncio
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import statistics
import struct
import tempfile
import time
import tracemalloc
from zoneinfo import ZoneInfo

from qmt_bridge.server.qmt_local_dat import _EXPECTED_TIMES, _read_stock
from qmt_bridge.server.ws.quote_hub import WholeQuoteHub


def measure(fn, samples):
    fn()
    elapsed = []
    for _ in range(samples):
        started = time.perf_counter()
        fn()
        elapsed.append((time.perf_counter() - started) * 1000)
    tracemalloc.start()
    fn()
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return {"p50_ms": round(statistics.median(elapsed), 3),
            "p95_ms": round(sorted(elapsed)[math.ceil(0.95 * samples) - 1], 3),
            "peak_python_bytes": peak}


async def fanout():
    calls = []
    def snapshot(**kwargs):
        calls.append(kwargs)
        return {"000001.SZ": {"lastPrice": 10}}
    hub = WholeQuoteHub(snapshot)
    subscriptions = [await hub.subscribe(["000001.SZ"], 60.0) for _ in range(10)]
    try:
        await asyncio.wait_for(asyncio.gather(*(sub.queue.get() for _, sub in subscriptions)), 2)
        return {"clients": 10, "native_calls_per_sample": len(calls), "unshared_reference_calls": 10}
    finally:
        await hub.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=252)
    parser.add_argument("--samples", type=int, default=12)
    args = parser.parse_args()
    if not 1 <= args.days <= 1000 or not 2 <= args.samples <= 100:
        parser.error("days must be 1..1000 and samples 2..100")
    with tempfile.TemporaryDirectory(prefix="qmt-bridge-bench-") as directory:
        path = Path(directory) / "fixture.DAT"
        day = datetime(2024, 1, 2, tzinfo=ZoneInfo("Asia/Shanghai"))
        with path.open("wb") as file:
            file.write(b"\xfe\xff\xff\xff\xff\xff\xff\x7f")
            for _ in range(args.days):
                while day.weekday() >= 5:
                    day += timedelta(days=1)
                for bar in _EXPECTED_TIMES:
                    timestamp = int(day.replace(hour=bar // 10000, minute=bar // 100 % 100).timestamp())
                    file.write(struct.pack("<16I", timestamp, 10000, 10100, 9900, 10000, 0, 10, 0, 10000, 0, 0, 0, 0, 0, 0, 0))
                day += timedelta(days=1)
        def reference():
            return _read_stock(path, start=None, end_exclusive=None, count=-1).tail(3)
        def optimized():
            return _read_stock(path, start=None, end_exclusive=None, count=3)
        assert reference().equals(optimized()), "tail optimization changed the data"
        result = {"kind": "offline_synthetic", "days": args.days, "bars": args.days * 241,
                  "fixture_bytes": path.stat().st_size, "samples": args.samples,
                  "reference_full_parse": measure(reference, args.samples),
                  "bounded_tail": measure(optimized, args.samples),
                  "shared_quote": asyncio.run(fanout())}
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
