"""Shared, bounded WebSocket quote fan-out primitives."""

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from bigqmt_signal_trader.telemetry import bind_context, current_context, emit, span


class LatestValueQueue:
    """A one-slot queue whose producer always retains the newest value."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)
        self.replaced = 0

    def offer(self, value: Any) -> None:
        if self.queue.full():
            try:
                self.queue.get_nowait()
                self.replaced += 1
            except asyncio.QueueEmpty:
                pass
        try:
            self.queue.put_nowait(value)
        except asyncio.QueueFull:  # another loop callback won the race
            pass

    def replace(self, value: Any) -> None:
        """Discard stale data when the native runtime has been replaced."""
        while True:
            try:
                self.queue.get_nowait()
                self.replaced += 1
            except asyncio.QueueEmpty:
                break
        self.queue.put_nowait(value)


@dataclass
class WholeQuoteGroup:
    codes: tuple[str, ...]
    interval: float
    subscribers: set[LatestValueQueue] = field(default_factory=set)
    task: asyncio.Task | None = None
    group_id: str = ""
    trace_context: dict[str, str] = field(default_factory=dict)


class WholeQuoteHub:
    """Shares a polling call for all clients with an equal code set."""

    def __init__(self, get_full_tick: Callable[..., Any]) -> None:
        self._get_full_tick = get_full_tick
        self._groups: dict[tuple[str, ...], WholeQuoteGroup] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, codes: list[str], interval: float) -> tuple[tuple[str, ...], LatestValueQueue]:
        key = tuple(sorted(dict.fromkeys(codes)))
        subscriber = LatestValueQueue()
        async with self._lock:
            group = self._groups.get(key)
            if group is None:
                parent = current_context()
                group_id = uuid.uuid4().hex
                group = WholeQuoteGroup(
                    key,
                    interval,
                    group_id=group_id,
                    trace_context={
                        # The sampler outlives any first subscriber.  It gets
                        # a fresh trace and an explicit causal link instead of
                        # inheriting a closed WebSocket scope.
                        "trace_id": "",
                        "span_id": "",
                        "parent_span_id": "",
                        "ws_session_id": "",
                        "quote_group_id": group_id,
                        "caused_by_trace_link": parent.get("trace_id", ""),
                    },
                )
                self._groups[key] = group
                group.task = asyncio.create_task(self._run_group(key, group), name="qmt-whole-quote")
                emit("ws.whole_quote.sampler_started", subscription_key="|".join(key), code_count=len(key), quote_group_id=group_id)
            else:
                # Faster consumers may reduce the shared sampling interval; slow
                # consumers can simply coalesce local snapshots.
                group.interval = min(group.interval, interval)
            group.subscribers.add(subscriber)
            emit("ws.whole_quote.subscribe", subscription_key="|".join(key), refcount=len(group.subscribers))
        return key, subscriber

    async def unsubscribe(self, key: tuple[str, ...], subscriber: LatestValueQueue) -> None:
        task = None
        async with self._lock:
            group = self._groups.get(key)
            if group is None:
                return
            group.subscribers.discard(subscriber)
            emit("ws.whole_quote.unsubscribe", subscription_key="|".join(key), refcount=len(group.subscribers), latest_replaced=subscriber.replaced)
            if not group.subscribers:
                self._groups.pop(key, None)
                task = group.task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            emit("ws.whole_quote.sampler_closed", subscription_key="|".join(key), critical=True)

    async def close(self) -> None:
        async with self._lock:
            tasks = [group.task for group in self._groups.values() if group.task]
            self._groups.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        emit("ws.whole_quote.hub_closed", critical=True, group_count=len(tasks))

    async def _run(self, key: tuple[str, ...], group: WholeQuoteGroup) -> None:
        try:
            while True:
                with span("ws.whole_quote.native_poll", subscription_key="|".join(key)) as result:
                    data = await asyncio.to_thread(self._get_full_tick, code_list=list(key))
                    result["outcome"] = "success"
                # Snapshot listeners cannot mutate the group while this loop runs,
                # but copy anyway so disconnect cleanup never changes iteration.
                for subscriber in tuple(group.subscribers):
                    subscriber.offer(data)
                await asyncio.sleep(group.interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A failed upstream sampling call must wake every connection.  Do
            # not leave sender tasks blocked on an empty queue or retain a
            # poisoned group for later subscribers.
            for subscriber in tuple(group.subscribers):
                subscriber.replace(
                    {
                        "type": "unavailable",
                        "reason": "native_whole_quote_unavailable",
                        "reconnect_required": True,
                    }
                )
            async with self._lock:
                if self._groups.get(key) is group:
                    self._groups.pop(key, None)
            emit("ws.whole_quote.native_poll", critical=True, outcome="error", subscription_key="|".join(key))

    async def _run_group(self, key: tuple[str, ...], group: WholeQuoteGroup) -> None:
        with bind_context(**group.trace_context):
            await self._run(key, group)


class RealtimeQuoteHub:
    """Reference-counted native quote subscriptions shared by WebSocket users."""

    def __init__(self, facade: Any, *, native_budget: int = 500, generation: Callable[[], object] | None = None) -> None:
        self._facade = facade
        self._native_budget = native_budget
        self._entries: dict[tuple[str, str], tuple[int, set[LatestValueQueue]]] = {}
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._generation = generation
        self._active_generation: object | None = None

    async def subscribe(self, codes: list[str], period: str) -> tuple[list[int], LatestValueQueue]:
        subscriber = LatestValueQueue()
        loop = asyncio.get_running_loop()
        acquired: list[tuple[str, str]] = []
        async with self._lock:
            current_generation = self._generation() if self._generation is not None else None
            if self._active_generation is not None and current_generation != self._active_generation:
                await self._invalidate_locked("native_runtime_replaced")
                emit("ws.realtime.generation_invalidated", critical=True, outcome="gap")
            self._active_generation = current_generation
            self._loop = loop
            try:
                for code in codes:
                    key = (code, period)
                    entry = self._entries.get(key)
                    if entry is None:
                        if len(self._entries) >= self._native_budget:
                            raise RuntimeError("native_quote_subscription_budget_exhausted")

                        def callback(event: Any, *, _key=key) -> None:
                            target_loop = self._loop
                            if target_loop is not None and not target_loop.is_closed():
                                target_loop.call_soon_threadsafe(self.publish, _key, event)

                        seq = int(self._facade.subscribe_quote(code, period=period, callback=callback) or 0)
                        if seq <= 0:
                            raise RuntimeError("native quote subscription unavailable")
                        entry = (seq, set())
                        self._entries[key] = entry
                        emit("ws.realtime.native_subscribe", critical=True, subscription_id=seq, stock_code=code, period=period)
                    entry[1].add(subscriber)
                    acquired.append(key)
                    emit("ws.realtime.subscribe", subscription_id=entry[0], stock_code=code, period=period, refcount=len(entry[1]))
            except Exception:
                await self._release_locked(acquired, subscriber)
                raise
        return [self._entries[key][0] for key in acquired], subscriber

    def publish(self, key: tuple[str, str], event: Any) -> None:
        if not isinstance(event, dict) or not event.get("code") or not isinstance(event.get("data"), dict):
            return
        entry = self._entries.get(key)
        if entry is None:
            return
        for subscriber in tuple(entry[1]):
            subscriber.offer(event)

    async def unsubscribe(self, codes: list[str], period: str, subscriber: LatestValueQueue) -> None:
        async with self._lock:
            await self._release_locked([(code, period) for code in codes], subscriber)

    async def _release_locked(self, keys: list[tuple[str, str]], subscriber: LatestValueQueue) -> None:
        unsubscribe: list[int] = []
        for key in keys:
            entry = self._entries.get(key)
            if entry is None:
                continue
            entry[1].discard(subscriber)
            emit("ws.realtime.unsubscribe", subscription_id=entry[0], stock_code=key[0], period=key[1], refcount=len(entry[1]), latest_replaced=subscriber.replaced)
            if not entry[1]:
                self._entries.pop(key, None)
                unsubscribe.append(entry[0])
        for seq in unsubscribe:
            try:
                self._facade.unsubscribe_quote(seq)
                emit("ws.realtime.native_unsubscribe", critical=True, subscription_id=seq)
            except (AttributeError, NotImplementedError, OSError, RuntimeError, TypeError, ValueError):
                pass

    async def close(self) -> None:
        async with self._lock:
            await self._invalidate_locked("service_shutdown")

    async def invalidate(self, reason: str = "native_runtime_replaced") -> None:
        """Tell existing clients to reconnect after a native runtime replacement."""
        async with self._lock:
            await self._invalidate_locked(reason)
            emit("ws.realtime.generation_invalidated", critical=True, outcome="gap", reason=reason)
            self._active_generation = None

    async def _invalidate_locked(self, reason: str) -> None:
        entries = list(self._entries.values())
        self._entries.clear()
        for seq, subscribers in entries:
            for subscriber in tuple(subscribers):
                subscriber.replace({"type": "unavailable", "reason": reason, "reconnect_required": True})
            try:
                self._facade.unsubscribe_quote(seq)
                emit("ws.realtime.native_unsubscribe", critical=True, subscription_id=seq, reason=reason)
            except (AttributeError, NotImplementedError, OSError, RuntimeError, TypeError, ValueError):
                pass
