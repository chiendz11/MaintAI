"""Retry infrastructure failures without relabeling valid source data as corrupt."""

import psycopg
from maintai_storage.archive import classify


def retryable(exc):
    if isinstance(
        exc,
        (
            psycopg.OperationalError,
            psycopg.InterfaceError,
            psycopg.errors.SerializationFailure,
            psycopg.errors.DeadlockDetected,
            psycopg.errors.LockNotAvailable,
            psycopg.errors.QueryCanceled,
        ),
    ):
        return True
    return classify(exc)[0]
