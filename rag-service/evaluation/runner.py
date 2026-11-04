
import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from api.schemas import MaintenanceBriefRequest
from pipeline import RAGPipeline
from evaluation.evaluator import (
    evaluate_retrieval,
    evaluate_grounding,
    aggregate,
)

logger = logging.getLogger(__name__)


class RecordingRetriever:
    """Ghi lại tài liệu mà pipeline thực sự truy xuất."""

    def __init__(self, retriever):
        self.retriever = retriever
        self.documents = []

    def search(self, *args, **kwargs):
        self.documents = self.retriever.search(
            *args, **kwargs
        )
        return self.documents


def load_gold_set(path: Path) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError(path)

    cases = []
    seen = set()

    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue

            case = json.loads(line)
            case_id = case["case_id"]

            if not isinstance(case_id, str) or not case_id:
                raise ValueError(
                    f"Invalid case_id at line {line_number}"
                )

            if case_id in seen:
                raise ValueError(
                    f"Duplicate case_id: {case_id}"
                )

            seen.add(case_id)

            request = MaintenanceBriefRequest.model_validate(
                case["request"]
            )

            expected = case.get("expected_sources")
            if not isinstance(expected, list) or not expected:
                raise ValueError(
                    f"{case_id}: expected_sources must be nonempty"
                )

            for source in expected:
                if not source.get("document_id") or not source.get("chunk_id"):
                    raise ValueError(
                        f"{case_id}: invalid expected source"
                    )

            actions = case.get("expected_actions")
            if actions is not None:
                if not isinstance(actions, list):
                    raise ValueError(
                        f"{case_id}: expected_actions must be a list"
                    )
                for action in actions:
                    if (
                        not isinstance(action.get("step"), int)
                        or not isinstance(
                            action.get("supporting_sources"), list
                        )
                        or not action["supporting_sources"]
                    ):
                        raise ValueError(
                            f"{case_id}: invalid expected action"
                        )
                    for source in action["supporting_sources"]:
                        if not source.get("document_id") or not source.get("chunk_id"):
                            raise ValueError(
                                f"{case_id}: invalid action source"
                            )

            cases.append({
                "case_id": case_id,
                "request": request,
                "expected_sources": expected,
                "expected_actions": actions,
            })

    if not cases:
        raise ValueError("Gold set is empty")

    return cases


def run_evaluation(
    dataset_path: str,
    output_path: str,
) -> dict:
    cases = load_gold_set(Path(dataset_path))

    # Pipeline thật, truy xuất đủ top 5 theo đặc tả.
    pipeline = RAGPipeline(top_k=5)

    recorder = RecordingRetriever(pipeline.retriever)
    pipeline.retriever = recorder

    results = []

    for case in cases:
        recorder.documents = []

        try:
            response = pipeline.run(case["request"])
            response_data = response.model_dump(mode="json")

            retrieved = [
                {
                    "document_id": str(
                        doc.metadata.get("document_id", "")
                    ),
                    "chunk_id": str(
                        doc.metadata.get("chunk_id", "")
                    ),
                }
                for doc in recorder.documents
            ]

            retrieval_result = evaluate_retrieval(
                retrieved,
                case["expected_sources"],
            )

            grounding_result = evaluate_grounding(
                response_data,
                retrieved,
                case["expected_actions"],
            )

            results.append({
                "case_id": case["case_id"],
                "retrieved_sources": retrieved,
                "retrieval": retrieval_result,
                "grounding": grounding_result,
                "response": response_data,
            })

        except Exception as exc:
            logger.exception(
                "Evaluation failed for %s",
                case["case_id"],
            )
            results.append({
                "case_id": case["case_id"],
                "error": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
            })

    report = {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "contract_version": "v1.4",
        "dataset": dataset_path,
        "metrics": aggregate(results),
        "cases": results,
    }

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    temporary = output.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(output)

    return report


def main():
    parser = argparse.ArgumentParser(
        description="MaintAI RAG contract v1.4 evaluation"
    )
    parser.add_argument(
        "--dataset",
        default="evaluation/gold_set.jsonl",
    )
    parser.add_argument(
        "--output",
        default="evaluation/report.json",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    report = run_evaluation(
        args.dataset,
        args.output,
    )

    print(json.dumps(
        report["metrics"],
        ensure_ascii=False,
        indent=2,
    ))

    if report["metrics"]["failed_cases"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
