
"""Generation evaluation runner for MaintAI RAG."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from evaluation.generation_metrics import (
    evaluate_json_validity,
    evaluate_citation_accuracy,
    evaluate_answer_correctness,
    evaluate_semantic_grounding,
)


def load_cases(path: Path) -> list[dict]:
    cases = []

    with path.open("r", encoding="utf-8-sig") as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue

            case = json.loads(line)

            if not isinstance(case, dict):
                raise ValueError(
                    f"Line {line_number} must be a JSON object"
                )

            if "case_id" not in case:
                raise ValueError(
                    f"Missing case_id on line {line_number}"
                )

            if "response" not in case:
                raise ValueError(
                    f"Missing response on line {line_number}"
                )

            cases.append(case)

    ids = [case["case_id"] for case in cases]

    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate case_id in dataset")

    if not cases:
        raise ValueError("Evaluation dataset is empty")

    return cases


def evaluate_case(case: dict) -> dict:
    response = case["response"]

    validity = evaluate_json_validity(response)

    result = {
        "case_id": case["case_id"],
        "json_validity": validity,
        "citation_accuracy": None,
        "answer_correctness": None,
        "semantic_grounding": None,
    }

    if not validity["schema_valid"]:
        return result

    if isinstance(response, str):
        response = json.loads(response)

    if "expected_sources" in case:
        result["citation_accuracy"] = (
            evaluate_citation_accuracy(
                response,
                case["expected_sources"],
            )
        )

    if "expected_answer" in case:
        result["answer_correctness"] = (
            evaluate_answer_correctness(
                response,
                case["expected_answer"],
                human_score=case.get("human_score"),
            )
        )

    result["semantic_grounding"] = (
        evaluate_semantic_grounding(
            response,
            reviewed_claims=case.get("reviewed_claims"),
        )
    )

    return result


def aggregate(results: list[dict]) -> dict:
    def average(values: list[float]) -> float | None:
        return mean(values) if values else None

    valid = [
        result for result in results
        if result["json_validity"]["schema_valid"]
    ]

    citations = [
        result["citation_accuracy"]
        for result in valid
        if result["citation_accuracy"] is not None
    ]

    correctness = [
        result["answer_correctness"]["human_score"]
        for result in valid
        if result["answer_correctness"] is not None
        and result["answer_correctness"]["reviewed"]
    ]

    grounding = [
        result["semantic_grounding"]["grounding_score"]
        for result in valid
        if result["semantic_grounding"] is not None
        and result["semantic_grounding"]["reviewed"]
    ]

    return {
        "total_cases": len(results),
        "schema_valid_rate": (
            len(valid) / len(results) if results else None
        ),
        "citation_precision": average([
            item["precision"]
            for item in citations
            if item["precision"] is not None
        ]),
        "citation_recall": average([
            item["recall"]
            for item in citations
            if item["recall"] is not None
        ]),
        "citation_f1": average([
            item["f1"]
            for item in citations
            if item["f1"] is not None
        ]),
        "answer_correctness": average(correctness),
        "semantic_grounding": average(grounding),
        "correctness_reviewed_cases": len(correctness),
        "grounding_reviewed_cases": len(grounding),
    }


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("evaluation/gold_set_generation.jsonl"),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("evaluation/generation_report.json"),
    )

    args = parser.parse_args()

    cases = load_cases(args.dataset)
    results = [evaluate_case(case) for case in cases]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": str(args.dataset),
        "summary": aggregate(results),
        "cases": results,
    }

    args.output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    args.output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(
        report["summary"],
        indent=2,
        ensure_ascii=False,
    ))

    print(f"REPORT SAVED: {args.output}")


if __name__ == "__main__":
    main()
