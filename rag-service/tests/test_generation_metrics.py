
import json

from evaluation.generation_metrics import evaluate_json_validity


def valid_response():
    return {
        "brief_id": "brief-001",
        "alert_id": "alert-001",
        "grounded_status": "INSUFFICIENT_EVIDENCE",
        "summary": "Insufficient evidence.",
        "uncertainty": "No relevant documents found.",
        "checklist": [],
        "citations": [],
        "retrieval": {
            "top_k": 5,
            "retrieved_chunk_ids": [],
        },
        "generator": None,
        "generated_at": None,
    }


def test_valid_response():
    result = evaluate_json_validity(valid_response())

    assert result["json_syntax_valid"] is True
    assert result["schema_valid"] is True
    assert result["errors"] == []


def test_valid_json_string():
    response = json.dumps(valid_response())

    result = evaluate_json_validity(response)

    assert result["json_syntax_valid"] is True
    assert result["schema_valid"] is True


def test_invalid_json_syntax():
    result = evaluate_json_validity('{"brief_id":')

    assert result["json_syntax_valid"] is False
    assert result["schema_valid"] is False


def test_missing_required_fields():
    result = evaluate_json_validity({})

    assert result["json_syntax_valid"] is True
    assert result["schema_valid"] is False
    assert len(result["errors"]) > 0


def test_non_grounded_response_with_citations():
    response = valid_response()

    response["citations"] = [{
        "citation_id": "c1",
        "document_id": "abb",
        "document_version": "1.0",
        "section": "Maintenance",
        "page": 1,
        "chunk_id": "123",
    }]

    result = evaluate_json_validity(response)

    assert result["schema_valid"] is False


def test_grounded_response_without_evidence():
    response = valid_response()
    response["grounded_status"] = "GROUNDED"

    result = evaluate_json_validity(response)

    assert result["schema_valid"] is False


def test_extra_field_rejected():
    response = valid_response()
    response["unexpected_field"] = "test"

    result = evaluate_json_validity(response)

    assert result["schema_valid"] is False


from evaluation.generation_metrics import evaluate_citation_accuracy


def test_citation_perfect_match():
    response = {
        "citations": [
            {"document_id": "abb", "chunk_id": "127"}
        ]
    }
    expected = [{"document_id": "abb", "chunk_id": "127"}]

    result = evaluate_citation_accuracy(response, expected)

    assert result["precision"] == 1.0
    assert result["recall"] == 1.0
    assert result["f1"] == 1.0


def test_citation_wrong_source():
    response = {
        "citations": [
            {"document_id": "abb", "chunk_id": "170"}
        ]
    }
    expected = [{"document_id": "abb", "chunk_id": "127"}]

    result = evaluate_citation_accuracy(response, expected)

    assert result["precision"] == 0.0
    assert result["recall"] == 0.0
    assert result["f1"] == 0.0


def test_citation_partial_match():
    response = {
        "citations": [
            {"document_id": "abb", "chunk_id": "127"}
        ]
    }
    expected = [
        {"document_id": "abb", "chunk_id": "127"},
        {"document_id": "abb", "chunk_id": "170"},
    ]

    result = evaluate_citation_accuracy(response, expected)

    assert result["precision"] == 1.0
    assert result["recall"] == 0.5
    assert abs(result["f1"] - 2 / 3) < 1e-9


def test_citation_extra_wrong_source():
    response = {
        "citations": [
            {"document_id": "abb", "chunk_id": "127"},
            {"document_id": "abb", "chunk_id": "999"},
        ]
    }
    expected = [{"document_id": "abb", "chunk_id": "127"}]

    result = evaluate_citation_accuracy(response, expected)

    assert result["precision"] == 0.5
    assert result["recall"] == 1.0


def test_citation_no_expected_sources():
    result = evaluate_citation_accuracy(
        {"citations": []},
        [],
    )

    assert result["precision"] is None
    assert result["recall"] is None
    assert result["f1"] is None
