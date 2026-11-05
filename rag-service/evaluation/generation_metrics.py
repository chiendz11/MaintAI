
"""Generation evaluation metrics for MaintAI RAG."""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from api.schemas import MaintenanceBriefResponse


def evaluate_json_validity(response: Any) -> dict:
    """
    Validate generated response against the MaintAI API contract.

    Accepts:
        - dict: Parsed response
        - str: Raw JSON response

    Returns:
        json_syntax_valid:
            Whether the response is valid JSON or a parsed JSON object.
        schema_valid:
            Whether it satisfies MaintenanceBriefResponse.
    """

    if isinstance(response, str):
        try:
            payload = json.loads(response)
        except json.JSONDecodeError:
            return {
                "json_syntax_valid": False,
                "schema_valid": False,
                "errors": ["Invalid JSON syntax"],
            }
    elif isinstance(response, dict):
        payload = response
    else:
        return {
            "json_syntax_valid": False,
            "schema_valid": False,
            "errors": ["Response must be a JSON string or dictionary"],
        }

    if not isinstance(payload, dict):
        return {
            "json_syntax_valid": True,
            "schema_valid": False,
            "errors": ["Response JSON must be an object"],
        }

    try:
        MaintenanceBriefResponse.model_validate(payload)
    except ValidationError as exc:
        return {
            "json_syntax_valid": True,
            "schema_valid": False,
            "errors": [
                {
                    "field": ".".join(str(part) for part in error["loc"]),
                    "message": error["msg"],
                }
                for error in exc.errors()
            ],
        }

    return {
        "json_syntax_valid": True,
        "schema_valid": True,
        "errors": [],
    }

def evaluate_citation_accuracy(
    response: dict,
    expected_sources: list[dict],
) -> dict:
    """
    Compare cited sources against manually verified ground truth.

    Each source is identified by (document_id, chunk_id).
    """

    def source_key(item: dict) -> tuple[str, str]:
        return (
            str(item["document_id"]),
            str(item["chunk_id"]),
        )

    cited = {
        source_key(item)
        for item in response.get("citations", [])
    }

    expected = {
        source_key(item)
        for item in expected_sources
    }

    if not expected:
        return {
            "precision": None,
            "recall": None,
            "f1": None,
            "matched_sources": 0,
            "cited_sources": len(cited),
            "expected_sources": 0,
        }

    matched = len(cited & expected)

    precision = matched / len(cited) if cited else 0.0
    recall = matched / len(expected)

    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall > 0
        else 0.0
    )

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "matched_sources": matched,
        "cited_sources": len(cited),
        "expected_sources": len(expected),
    }


def evaluate_answer_correctness(
    response: dict,
    expected_answer: str,
    human_score: float | None = None,
) -> dict:
    """
    Evaluate answer correctness.

    human_score:
        0.0 = Incorrect
        0.5 = Partially correct
        1.0 = Correct
        None = Not reviewed

    Exact match is diagnostic only, not semantic correctness.
    """

    summary = response.get("summary", "")

    if not isinstance(summary, str):
        summary = ""

    exact_match = (
        summary.strip().casefold()
        == expected_answer.strip().casefold()
    )

    if human_score is not None:
        if human_score not in (0.0, 0.5, 1.0):
            raise ValueError(
                "human_score must be 0.0, 0.5, 1.0, or None"
            )

    return {
        "exact_match": exact_match,
        "human_score": human_score,
        "reviewed": human_score is not None,
    }

def evaluate_semantic_grounding(
    response: dict,
    reviewed_claims: list[dict] | None = None,
) -> dict:
    """
    Evaluate whether generated claims are supported by evidence.

    reviewed_claims example:
        [
            {"claim": "...", "supported": True},
            {"claim": "...", "supported": False}
        ]

    None means no human review has been performed.
    """

    if reviewed_claims is None:
        return {
            "reviewed": False,
            "grounding_score": None,
            "supported_claims": 0,
            "total_claims": 0,
        }

    if not reviewed_claims:
        return {
            "reviewed": False,
            "grounding_score": None,
            "supported_claims": 0,
            "total_claims": 0,
        }

    for item in reviewed_claims:
        if not isinstance(item.get("supported"), bool):
            raise ValueError(
                "Each reviewed claim must have a boolean supported field"
            )

    supported = sum(
        1 for item in reviewed_claims if item["supported"]
    )

    return {
        "reviewed": True,
        "grounding_score": supported / len(reviewed_claims),
        "supported_claims": supported,
        "total_claims": len(reviewed_claims),
    }
