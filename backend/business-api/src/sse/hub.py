"""One PostgreSQL listener per API process; bounded and scoped fan-out."""

import asyncio
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from uuid import uuid4

import psycopg
from api.auth import RegistryFailure
from backend.shared.notifications import DEFAULT_CHANNEL, Notifier, machine_key, parse_hint
from maintai_messaging.contracts import canonical_json
from psycopg import sql


@dataclass(frozen=True)
class SSESettings:
    channel: str = DEFAULT_CHANNEL
    max_connections: int = 128
    connection_bytes: int = 2 * 1024 * 1024
    total_bytes: int = 8 * 1024 * 1024
    coalesce_seconds: float = 1
    heartbeat_seconds: float = 15

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        return cls(
            channel=env.get("BUSINESS_NOTIFY_CHANNEL", DEFAULT_CHANNEL),
            max_connections=int(env.get("BUSINESS_SSE_MAX_CONNECTIONS", "128")),
            connection_bytes=int(env.get("BUSINESS_SSE_CONNECTION_BYTES", str(2 * 1024 * 1024))),
            total_bytes=int(env.get("BUSINESS_SSE_TOTAL_BYTES", str(8 * 1024 * 1024))),
        )

    def __post_init__(self):
        Notifier(self.channel)
        for value in (self.max_connections, self.connection_bytes, self.total_bytes):
            if type(value) is not int or value <= 0:
                raise ValueError("SSE budgets must be positive integers")
        if not 0 <= self.coalesce_seconds <= 60 or not 0 < self.heartbeat_seconds <= 60:
            raise ValueError("Invalid SSE timing")


@dataclass(eq=False)
class Subscription:
    machines: dict
    expires_at: float
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=32))
    hashes: dict = field(default_factory=dict)
    pending: int = 0
    closed: bool = False
    connected: bool = False


def snapshot_events(snapshot):
    for name, key, timestamp in (
        ("telemetry.received", "telemetry", "server_accepted_at"),
        ("prediction.completed", "latest_valid_prediction", "completed_at"),
        ("processing.status_changed", "latest_processing", "updated_at"),
    ):
        value = snapshot[key]
        if value is not None:
            yield {
                "schema_version": "1.0.0",
                "type": name,
                "machine_id": snapshot["machine_id"],
                "data": value,
                "occurred_at": value[timestamp],
            }


class NotificationHub:
    def __init__(self, repository, *, dsn, settings=None, metrics=None):
        self.repository, self.dsn = repository, dsn
        self.settings, self.metrics = settings or SSESettings(), metrics
        self.ready = False
        self.subscriptions, self.dirty, self.tasks = set(), {}, []
        self.pending_bytes = 0
        self.stopping = asyncio.Event()
        self.connection = None

    def _metric(self, name, *args):
        if self.metrics is not None:
            try:
                getattr(self.metrics, name)(*args)
            except Exception:
                logging.getLogger("maintai.business.sse").warning("SSE metric update failed")

    def subscribe(self, machines, *, expires_at):
        if not self.ready:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        if expires_at is None or expires_at <= time.time():
            raise RegistryFailure(401, "UNAUTHORIZED")
        if len(self.subscriptions) >= self.settings.max_connections:
            raise RegistryFailure(429, "RATE_LIMITED")
        value = Subscription({machine_key(machine): machine for machine in machines}, expires_at)
        self.subscriptions.add(value)
        return value

    def connected(self, subscription):
        if not subscription.closed and not subscription.connected:
            subscription.connected = True
            self._metric("sse_connection", 1)

    def unsubscribe(self, subscription):
        if subscription.closed:
            return
        subscription.closed = True
        self.subscriptions.discard(subscription)
        self.pending_bytes -= subscription.pending
        subscription.pending = 0
        while not subscription.queue.empty():
            subscription.queue.get_nowait()
        subscription.queue.put_nowait(None)
        if subscription.connected:
            self._metric("sse_connection", -1)

    def disconnect_all(self, reason):
        for subscription in tuple(self.subscriptions):
            self.unsubscribe(subscription)
        self.dirty.clear()
        self._metric("sse_resync", reason)

    def invalidate(self, key):
        if any(key in subscription.machines for subscription in self.subscriptions):
            if len(self.dirty) >= 4096 and key not in self.dirty:
                self.disconnect_all("GAP")
                return
            self.dirty.setdefault(key, time.monotonic() + self.settings.coalesce_seconds)

    async def flush(self):
        due = [key for key, deadline in self.dirty.items() if deadline <= time.monotonic()]
        for key in due:
            self.dirty.pop(key, None)
            targets = [s for s in self.subscriptions if key in s.machines and not s.closed]
            if not targets:
                continue
            machine = targets[0].machines[key]
            try:
                snapshot = await asyncio.wait_for(
                    asyncio.to_thread(self.repository.live, machine), 5
                )
                if snapshot is None or snapshot["machine_id"] != machine:
                    raise ValueError("Subscribed machine is unavailable or mismatched")
                events = []
                for value in snapshot_events(snapshot):
                    body, kind = canonical_json(value), value["type"]
                    frame = (
                        b"event: "
                        + kind.encode()
                        + b"\nid: "
                        + uuid4().hex.encode()
                        + b"\ndata: "
                        + body
                        + b"\n\n"
                    )
                    events.append((kind, hashlib.sha256(body).digest(), frame))
            except (psycopg.Error, ValueError, TimeoutError):
                self.disconnect_all("OTHER")
                continue
            for subscription in targets:
                if subscription.closed or subscription.machines.get(key) != machine:
                    continue
                if subscription.expires_at <= time.time():
                    self.unsubscribe(subscription)
                    continue
                for kind, digest, frame in events:
                    cache_key = (machine, kind)
                    if subscription.hashes.get(cache_key) == digest:
                        continue
                    if (
                        subscription.pending + len(frame) > self.settings.connection_bytes
                        or self.pending_bytes + len(frame) > self.settings.total_bytes
                        or subscription.queue.full()
                    ):
                        self.unsubscribe(subscription)
                        self._metric("sse_resync", "GAP")
                        break
                    subscription.hashes[cache_key] = digest
                    subscription.queue.put_nowait(frame)
                    subscription.pending += len(frame)
                    self.pending_bytes += len(frame)

    async def next_frame(self, subscription, *, timeout=None):
        remaining = subscription.expires_at - time.time()
        if subscription.closed or remaining <= 0:
            self.unsubscribe(subscription)
            return None
        try:
            frame = await asyncio.wait_for(
                subscription.queue.get(), min(timeout or self.settings.heartbeat_seconds, remaining)
            )
        except TimeoutError:
            if subscription.expires_at <= time.time():
                self.unsubscribe(subscription)
                return None
            return b": heartbeat\n\n"
        if frame is not None:
            subscription.pending -= len(frame)
            self.pending_bytes -= len(frame)
        if subscription.expires_at <= time.time():
            self.unsubscribe(subscription)
            return None
        return frame

    async def _listen(self):
        delay = 1
        while not self.stopping.is_set():
            try:
                async with await psycopg.AsyncConnection.connect(
                    self.dsn, autocommit=True, connect_timeout=3
                ) as connection:
                    self.connection = connection
                    await connection.execute(
                        sql.SQL("LISTEN {}").format(sql.Identifier(self.settings.channel))
                    )
                    self.ready, delay = True, 1
                    while not self.stopping.is_set():
                        async for notification in connection.notifies(timeout=1):
                            try:
                                self.invalidate(parse_hint(notification.payload))
                            except (ValueError, RecursionError):
                                continue
            except (psycopg.Error, OSError):
                self.ready = False
                self.disconnect_all("NOTIFY_LOST")
                try:
                    await asyncio.wait_for(self.stopping.wait(), delay)
                except TimeoutError:
                    pass
                delay = min(delay * 2, 30)
            finally:
                self.connection = None
                self.ready = False

    async def _pump(self):
        while not self.stopping.is_set():
            await self.flush()
            try:
                await asyncio.wait_for(self.stopping.wait(), 0.05)
            except TimeoutError:
                pass

    async def start(self):
        self.tasks = [asyncio.create_task(self._listen()), asyncio.create_task(self._pump())]

    async def stop(self):
        self.stopping.set()
        self.ready = False
        self.disconnect_all("OTHER")
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
