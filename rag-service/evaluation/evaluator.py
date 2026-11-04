
from __future__ import annotations

from statistics import mean
from typing import Any


RECALL_LEVELS = (1, 3, 5)


def source_key(
    document_id: Any,
    chunk_id: Any,
) -> tuple[str, str]:
    return str(document_id), str(chunk_id)


def recall_at_k(
    retrieved: list[dict],
    expected: list[dict],
    k: int,
) -> float | None:

    if not expected:
        return None

    gold = {
        source_key(
            item["document_id"],
            item["chunk_id"],
        )
        for item in expected
    }

    found = {
        source_key(
            item["document_id"],
            item["chunk_id"],
        )
        for item in retrieved[:k]
    }

    return len(gold & found) / len(gold)


def evaluate_retrieval(
    retrieved: list[dict],
    expected: list[dict],
) -> dict:

    return {
        f"recall_at_{k}": recall_at_k(
            retrieved,
            expected,
            k,
        )
        for k in RECALL_LEVELS
    }


def evaluate_grounding(
    response: dict,
    retrieved: list[dict],
    expected_actions: list[dict] | None = None,
) -> dict:

    citations = response.get("citations", [])
    checklist = response.get("checklist", [])
    status = response.get("grounded_status")

    retrieved_keys = {
        source_key(
            item["document_id"],
            item["chunk_id"],
        )
        for item in retrieved
    }

    citation_map = {}
    invalid_citations = set()

    # 1. Kiểm tra citation có thuộc tài liệu truy xuất
    for citation in citations:
        citation_id = str(citation["citation_id"])

        key = source_key(
            citation["document_id"],
            citation["chunk_id"],
        )

        if citation_id in citation_map:
            invalid_citations.add(citation_id)

        if key not in retrieved_keys:
            invalid_citations.add(citation_id)

        citation_map[citation_id] = key

    # 2. Đọc nhãn action từ gold set
    expected_by_step = None

    if expected_actions is not None:
        expected_by_step = {
            item["step"]: {
                source_key(
                    evidence["document_id"],
                    evidence["chunk_id"],
                )
                for evidence in item["supporting_sources"]
            }
            for item in expected_actions
        }

    # 3. Kiểm tra từng action trong checklist
    action_results = []

    for item in checklist:
        citation_ids = [
            str(value)
            for value in item.get("citation_ids", [])
        ]

        actual_sources = {
            citation_map[citation_id]
            for citation_id in citation_ids
            if citation_id in citation_map
        }

        structure_valid = (
            bool(citation_ids)
            and all(
                citation_id in citation_map
                and citation_id not in invalid_citations
                for citation_id in citation_ids
            )
        )

        label_result = "NOT_REVIEWED"

        if expected_by_step is not None:
            expected_sources = expected_by_step.get(
                item["step"]
            )

            if expected_sources is None:
                label_result = "UNLABELED_ACTION"

            elif actual_sources & expected_sources:
                label_result = "SOURCE_MATCH"

            else:
                label_result = "SOURCE_MISMATCH"

        action_results.append({
            "step": item["step"],
            "action": item["action"],
            "citation_ids": citation_ids,
            "citation_structure_valid": structure_valid,
            "source_label_result": label_result,
        })

    # 4. Chỉ đánh giá grounding khi response GROUNDED
    if status == "GROUNDED":
        structural_pass = (
            bool(action_results)
            and not invalid_citations
            and all(
                result["citation_structure_valid"]
                for result in action_results
            )
        )
    else:
        structural_pass = None

    # 5. Đối chiếu với gold set đã gán nhãn
    reviewed_pass = None

    if (
        expected_by_step is not None
        and structural_pass is not None
    ):
        reviewed_pass = (
            structural_pass
            and len(action_results) == len(expected_by_step)
            and all(
                result["source_label_result"]
                == "SOURCE_MATCH"
                for result in action_results
            )
        )

    return {
        "structural_pass": structural_pass,
        "invalid_citation_ids": sorted(invalid_citations),
        "reviewed_source_pass": reviewed_pass,
        "actions": action_results,
        "semantic_support_verified": False,
    }


def aggregate(cases: list[dict]) -> dict:

    completed = [
        case
        for case in cases
        if "error" not in case
    ]

    def avg_metric(key: str) -> float | None:
        values = [
            case["retrieval"][key]
            for case in completed
            if case["retrieval"][key] is not None
        ]

        return mean(values) if values else None

    # Chỉ lấy response có thể đánh giá grounding
    grounding = [
        case["grounding"]
        for case in completed
        if (
            case.get("grounding") is not None
            and case["grounding"]["structural_pass"]
            is not None
        )
    ]

    reviewed = [
        result["reviewed_source_pass"]
        for result in grounding
        if result["reviewed_source_pass"] is not None
    ]

    return {
        "total_cases": len(cases),
        "completed_cases": len(completed),
        "failed_cases": len(cases) - len(completed),

        "recall_at_1": avg_metric("recall_at_1"),
        "recall_at_3": avg_metric("recall_at_3"),
        "recall_at_5": avg_metric("recall_at_5"),

        "citation_structure_pass_rate": (
            mean(
                int(result["structural_pass"])
                for result in grounding
            )
            if grounding else None
        ),

        "reviewed_source_pass_rate": (
            mean(
                int(value)
                for value in reviewed
            )
            if reviewed else None
        ),

        "semantic_grounding_pass_rate": None,
    }
