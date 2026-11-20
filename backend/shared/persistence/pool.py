"""Thread-safe bounded PostgreSQL connection pool for backend workers and APIs."""

import threading
import time
from contextlib import contextmanager

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row


def _close_quietly(connection) -> None:
    try:
        connection.close()
    except Exception:
        pass


class ConnectionPool:
    """Bounded persistent PostgreSQL connection factory."""

    def __init__(
        self,
        dsn: str,
        *,
        size: int = 8,
        timeout: float = 30.0,
        max_idle_check_seconds: float = 10.0,
        connect_timeout: int = 10,
        options: str = (
            "-c statement_timeout=15000 -c lock_timeout=5000 "
            "-c idle_in_transaction_session_timeout=60000"
        ),
    ):
        if (
            not 1 <= size <= 32
            or timeout <= 0
            or max_idle_check_seconds < 0
            or connect_timeout <= 0
        ):
            raise ValueError("Connection pool budgets must be positive and bounded")
        self._dsn = dsn
        self._size = size
        self._timeout = timeout
        self._max_idle_check_seconds = max_idle_check_seconds
        self._connect_timeout = connect_timeout
        self._options = options
        self._condition = threading.Condition()
        self._idle = []
        self._allocated = 0
        self._closed = False

    def _connect(self):
        return psycopg.connect(
            self._dsn,
            row_factory=dict_row,
            connect_timeout=self._connect_timeout,
            options=self._options,
        )

    def _is_usable(self, connection) -> bool:
        return (
            not connection.closed
            and not connection.broken
            and connection.info.transaction_status == TransactionStatus.IDLE
        )

    def _borrow(self):
        deadline = time.monotonic() + self._timeout
        while True:
            candidate = None
            idle_since = 0.0
            with self._condition:
                while True:
                    if self._closed:
                        raise RuntimeError("Connection pool is closed")
                    if self._idle:
                        candidate, idle_since = self._idle.pop()
                        if self._is_usable(candidate):
                            break
                        _close_quietly(candidate)
                        self._allocated -= 1
                        self._condition.notify()
                        candidate = None
                        continue
                    if self._allocated < self._size:
                        self._allocated += 1
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Timed out waiting for a PostgreSQL connection")
                    self._condition.wait(remaining)

            if candidate is None:
                try:
                    connection = self._connect()
                except BaseException:
                    with self._condition:
                        self._allocated -= 1
                        self._condition.notify()
                    raise
                with self._condition:
                    if self._closed:
                        _close_quietly(connection)
                        self._allocated -= 1
                        self._condition.notify_all()
                        raise RuntimeError("Connection pool is closed")
                return connection

            if (time.monotonic() - idle_since) >= self._max_idle_check_seconds:
                try:
                    candidate.execute("SELECT 1")
                    candidate.rollback()
                except Exception:
                    _close_quietly(candidate)
                    with self._condition:
                        self._allocated -= 1
                        self._condition.notify()
                    continue

            with self._condition:
                if self._closed:
                    _close_quietly(candidate)
                    self._allocated -= 1
                    self._condition.notify_all()
                    raise RuntimeError("Connection pool is closed")
            return candidate

    def _finish(self, connection, *, commit: bool) -> None:
        if connection.closed or connection.broken:
            return
        status = connection.info.transaction_status
        if commit:
            if status == TransactionStatus.INTRANS:
                connection.commit()
            elif status != TransactionStatus.IDLE:
                try:
                    connection.rollback()
                finally:
                    _close_quietly(connection)
            return
        try:
            if status != TransactionStatus.IDLE:
                connection.rollback()
        except Exception:
            _close_quietly(connection)

    def _release(self, connection, *, healthy: bool) -> None:
        with self._condition:
            if self._closed or not healthy or not self._is_usable(connection):
                _close_quietly(connection)
                self._allocated -= 1
            else:
                self._idle.append((connection, time.monotonic()))
            self._condition.notify()

    @contextmanager
    def __call__(self):
        connection = self._borrow()
        healthy = False
        try:
            yield connection
        except Exception:
            self._finish(connection, commit=False)
            healthy = not (connection.closed or connection.broken)
            raise
        except BaseException:
            self._finish(connection, commit=False)
            raise
        else:
            self._finish(connection, commit=True)
            healthy = not (connection.closed or connection.broken)
        finally:
            self._release(connection, healthy=healthy)

    def close(self) -> None:
        with self._condition:
            self._closed = True
            for connection, _ in self._idle:
                _close_quietly(connection)
                self._allocated -= 1
            self._idle.clear()
            self._condition.notify_all()
