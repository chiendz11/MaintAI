"""Small bounded credential-specific connection pools; no pool dependency added."""

import threading
import time
from contextlib import contextmanager

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .security import HistoryError


class Connections:
    def __init__(self, dsn, *, readonly, size=4, timeout=2, source_schema=None):
        if not 1 <= size <= 16 or not 0 < timeout <= 5:
            raise ValueError("Connection pool budgets must be bounded")
        self.dsn, self.readonly, self.size, self.timeout = dsn, readonly, size, timeout
        self.source_schema = source_schema
        self.condition = threading.Condition()
        self.idle, self.allocated, self.closed = [], 0, False

    def borrow(self):
        deadline = time.monotonic() + self.timeout
        with self.condition:
            while True:
                if self.closed:
                    raise HistoryError(503, "DEPENDENCY_UNAVAILABLE")
                if self.idle:
                    connection = self.idle.pop()
                    if not connection.closed:
                        return connection
                    self.allocated -= 1
                if self.allocated < self.size:
                    self.allocated += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise HistoryError(503, "HISTORY_BUSY")
                self.condition.wait(remaining)
        try:
            options = "-c statement_timeout=" + ("3000" if self.readonly else "10000")
            options += " -c default_transaction_read_only=" + ("on" if self.readonly else "off")
            connection = psycopg.connect(
                self.dsn,
                row_factory=dict_row,
                connect_timeout=2,
                options=options,
                application_name="maintai-history-source"
                if self.readonly
                else "maintai-history-metadata",
            )
            if self.source_schema:
                connection.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(self.source_schema))
                )
                connection.commit()
            with self.condition:
                if self.closed:
                    connection.close()
                    raise ValueError("Pool was closed during connect")
            return connection
        except Exception:
            with self.condition:
                self.allocated -= 1
                self.condition.notify()
            raise HistoryError(503, "DEPENDENCY_UNAVAILABLE") from None

    @contextmanager
    def __call__(self):
        connection = self.borrow()
        try:
            with connection.transaction():
                yield connection
        finally:
            with self.condition:
                if self.closed or connection.closed:
                    connection.close()
                    self.allocated -= 1
                else:
                    self.idle.append(connection)
                self.condition.notify()

    def close(self):
        with self.condition:
            self.closed = True
            for connection in self.idle:
                connection.close()
                self.allocated -= 1
            self.idle.clear()
            self.condition.notify_all()
