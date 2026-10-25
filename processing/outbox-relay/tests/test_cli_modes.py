"""CLI mode takes precedence before mode-dependent budget validation."""

import pytest
from relay import main as relay_main

from .test_config import env


@pytest.mark.parametrize("environment_mode", ["legacy", "not-selected"])
def test_ordered_cli_validates_per_head_budget_before_starting_lifecycle(
    monkeypatch, environment_mode
):
    settings = {
        **env(),
        "OUTBOX_EXECUTION_MODE": environment_mode,
        "OUTBOX_BATCH_SIZE": "10",
        "OUTBOX_CLAIM_LEASE_SECONDS": "120",
        "KAFKA_DELIVERY_TIMEOUT_MS": "30000",
    }
    for key in ("PROCESSING_OUTBOX_BATCH_SIZE", "PROCESSING_OUTBOX_LEASE_SECONDS"):
        monkeypatch.delenv(key, raising=False)
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    observed = []

    async def run(config, stop, *, once):
        observed.append(config)
        return 0

    monkeypatch.setattr(relay_main, "run_relay", run)
    assert relay_main.main(["--mode", "ordered", "--once"]) == 0
    assert (observed[0].execution_mode, observed[0].batch_size, observed[0].lease_seconds) == (
        "ordered",
        10,
        120,
    )
