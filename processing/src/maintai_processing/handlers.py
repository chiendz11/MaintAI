"""Bind reviewed owner callbacks to the SDK's fenced Processing transaction."""

import importlib
import os

from maintai_messaging.errors import ConfigurationError
from maintai_messaging.ordering import KeyedTransactionalHandler
from maintai_messaging.postgres import connect


def _callback(name):
    value = os.environ.get(name, "")
    module, separator, attribute = value.partition(":")
    if not separator or not module or not attribute:
        raise ConfigurationError("Reviewed Processing callbacks must use module:function")
    callback = getattr(importlib.import_module(module), attribute)
    if not callable(callback):
        raise ConfigurationError("Processing callback must be callable")
    return callback


def make_handler(settings, role, registry):
    if role != "processing":
        raise ConfigurationError("This factory is Processing-owned")
    dsn = os.environ.get("PROCESSING_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise ConfigurationError("Processing owner database is required")
    prepare = _callback("PROCESSING_PREPARE_CALLBACK")
    persist = _callback("PROCESSING_PERSIST_CALLBACK")
    return KeyedTransactionalHandler(
        lambda: connect(dsn),
        settings.group_prefix + registry.topology["profiles"][role]["group_id"],
        prepare,
        persist,
        contracts=registry,
    )
