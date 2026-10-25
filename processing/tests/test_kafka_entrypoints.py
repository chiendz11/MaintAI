from maintai_processing import outbox_relay, worker


def test_factory_rejects_missing_owner_callbacks_without_connecting(monkeypatch):
    import pytest
    from maintai_messaging.errors import ConfigurationError

    from maintai_processing.handlers import make_handler

    monkeypatch.setenv("PROCESSING_DATABASE_URL", "fixture-only")
    monkeypatch.delenv("PROCESSING_PREPARE_CALLBACK", raising=False)
    with pytest.raises(ConfigurationError, match="callbacks"):
        make_handler(None, "processing", None)


def test_factory_wires_real_group_and_two_phase_callbacks(monkeypatch):
    from types import SimpleNamespace

    from maintai_processing import handlers

    seen = []

    def callback(*_):
        return None

    monkeypatch.setenv("PROCESSING_DATABASE_URL", "fixture-only")
    monkeypatch.setattr(handlers, "_callback", lambda name: callback)
    monkeypatch.setattr(
        handlers, "KeyedTransactionalHandler", lambda *args, **kwargs: seen.append((args, kwargs))
    )
    registry = SimpleNamespace(topology={"profiles": {"processing": {"group_id": "processing-v1"}}})
    handlers.make_handler(SimpleNamespace(group_prefix="it-"), "processing", registry)
    assert seen[0][0][1] == "it-processing-v1"
    assert seen[0][0][2:] == (callback, callback)


def test_worker_wraps_processing_role_without_invented_application_handler(monkeypatch):
    seen = []
    monkeypatch.delenv("PROCESSING_HANDLER_FACTORY", raising=False)
    monkeypatch.setattr(worker.cli, "main", lambda args: seen.append(args) or 0)
    assert worker.main(["--processing-mode", "keyed"]) == 0
    assert seen == [["consume", "--role", "processing", "--processing-mode", "keyed"]]


def test_worker_uses_explicit_factory_and_keeps_cli_override(monkeypatch):
    seen = []
    monkeypatch.setenv("PROCESSING_HANDLER_FACTORY", "app_handlers:make_handler")
    monkeypatch.setattr(worker.cli, "main", lambda args: seen.append(args) or 0)
    worker.main([])
    assert seen[-1][-2:] == ["--handler-factory", "app_handlers:make_handler"]
    worker.main(["--handler-factory", "reviewed:factory"])
    assert seen[-1].count("--handler-factory") == 1
    assert seen[-1][-1] == "reviewed:factory"


def test_relay_mode_is_explicit_and_does_not_migrate_at_startup(monkeypatch):
    seen = []
    monkeypatch.setattr(outbox_relay.cli, "main", lambda args: seen.append(args) or 0)
    assert outbox_relay.main(["--mode", "ordered", "--once"]) == 0
    assert seen == [["relay", "--mode", "ordered", "--once"]]
