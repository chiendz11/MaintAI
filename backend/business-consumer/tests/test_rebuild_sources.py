"""Original HTTP outcome reader rejects identity drift and unavailable evidence."""

import json

import httpx
import pytest

from .test_projection_reducer import event


def test_outcome_reader_uses_service_token_and_verifies_original_identity(tmp_path):
    from backend.shared.projections.sources import OutcomeSource

    source = event("invalid", 1)
    token = tmp_path / "service-token"
    token.write_text("fixture-service-token")

    def respond(request):
        assert request.headers["Authorization"] == "Bearer fixture-service-token"
        assert request.url.path.endswith("/" + source.message_id)
        return httpx.Response(200, json={"result_id": source.message_id, "outcome": source.payload})

    reader = OutcomeSource("http://127.0.0.1:8081", token, transport=httpx.MockTransport(respond))
    member = {
        "topic": source.topic,
        "message_id": source.message_id,
        "identity_sha256": source.identity_sha256,
        "schema_version": "1.4.0",
    }
    decoded, ref = reader.read(member)
    assert decoded.identity_sha256 == source.identity_sha256
    assert ref["result_id"] == source.message_id
    member["identity_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="identity"):
        reader.read(member)


@pytest.mark.parametrize("status", [403, 404, 500])
def test_outcome_errors_never_substitute_latest(tmp_path, status):
    from backend.shared.projections.sources import OutcomeSource, SourcePending

    token = tmp_path / "token"
    token.write_text("fixture")
    reader = OutcomeSource(
        "http://127.0.0.1", token, transport=httpx.MockTransport(lambda _: httpx.Response(status))
    )
    with pytest.raises(SourcePending if status == 500 else ValueError):
        reader.read({"message_id": "result"})


def test_outcome_reader_rejects_oversized_response(tmp_path):
    from backend.shared.projections.sources import OutcomeSource

    token = tmp_path / "token"
    token.write_text("fixture")
    reader = OutcomeSource(
        "http://127.0.0.1",
        token,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=json.dumps({"padding": "x" * 1048576}))
        ),
    )
    with pytest.raises(ValueError, match="budget"):
        reader.read({"message_id": "result"})
