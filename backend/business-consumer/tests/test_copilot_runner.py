"""A provider failure is persisted; polling never runs generation twice inline."""

from datetime import UTC, datetime, timedelta
from threading import Event


def test_runner_persists_non_retryable_error_and_preserves_original_input():
    from backend.shared.copilot.runner import BriefRunner
    from rag_client import ProviderFailure

    claims = [
        {
            "job_id": "one",
            "alert_id": "alert",
            "input": {"original": True},
            "attempts": 1,
            "deadline": datetime.now(UTC) + timedelta(seconds=60),
        }
    ]

    class Jobs:
        def claim(self, owner):
            return claims.pop() if claims else None

        def fail(self, claim, code, **options):
            self.failure = code, options

        def heartbeat(self, claim):
            return True

    class Client:
        def generate(self, payload, **options):
            assert payload == {"original": True}
            raise ProviderFailure("RAG_CONTRACT_INVALID")

    jobs = Jobs()
    runner = BriefRunner(jobs, Client(), Event())
    assert runner.once("worker")
    assert jobs.failure[0] == "RAG_CONTRACT_INVALID"
    assert jobs.failure[1]["retryable"] is False
    assert not runner.once("worker")
