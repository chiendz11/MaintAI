import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from evaluation.evaluator import evaluate_retrieval
from vector_retriever.retriever import VectorRetriever


def run(dataset_path: str, output_path: str, top_k: int = 5):
    if top_k != 5:
        raise ValueError("This benchmark currently supports top_k=5")

    cases = [
        json.loads(line)
        for line in Path(dataset_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    if not cases:
        raise ValueError("Gold set is empty")

    retriever = VectorRetriever()
    results = []
    seen_ids = set()

    for case in cases:
        case_id = case["case_id"]

        if case_id in seen_ids:
            raise ValueError(f"Duplicate case_id: {case_id}")
        seen_ids.add(case_id)

        expected = case["expected_sources"]
        if not expected:
            raise ValueError(f"{case_id}: expected_sources is empty")

        document_ids = {str(s["document_id"]) for s in expected}

        filters = (
            {"document_id": next(iter(document_ids))}
            if len(document_ids) == 1 else None
        )

        documents = retriever.search(
            query=case["query"],
            top_k=top_k,
            filters=filters,
        )

        retrieved = [
            {
                "document_id": str(doc.metadata.get("document_id", "")),
                "chunk_id": str(doc.metadata.get("chunk_id", "")),
            }
            for doc in documents
        ]

        gold = {
            (str(s["document_id"]), str(s["chunk_id"]))
            for s in expected
        }
        found = {
            (s["document_id"], s["chunk_id"])
            for s in retrieved
        }

        metrics = evaluate_retrieval(retrieved, expected)

        recall = metrics["recall_at_5"]
        hit = metrics["hit_at_5"]
        mrr = metrics["mrr_at_5"]
        ndcg = metrics["ndcg_at_5"]

        result = {
        "case_id": case_id,
        "retrieved_sources": retrieved,
        "metrics": metrics,
}
        results.append(result)

        print(
            f"{case_id}: "
            f"Recall@5={recall:.3f}, "
            f"Hit@5={hit:.3f}, "
            f"MRR@5={mrr:.3f}, "
            f"nDCG@5={ndcg:.3f}"
        )

    metric_names = (
        "recall_at_1",
        "recall_at_3",
        "recall_at_5",
        "hit_at_5",
        "mrr_at_5",
        "ndcg_at_5",
    )
    averages = {
        name: sum(r["metrics"][name] for r in results) / len(results)
        for name in metric_names
    }

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset_path,
        "top_k": top_k,
        "total_cases": len(results),
        "metrics": averages,
        "cases": results,
    }

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\nBENCHMARK SUMMARY")
    print(json.dumps(averages, indent=2))
    print("REPORT SAVED:", output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MaintAI Retrieval Benchmark")
    parser.add_argument(
        "--dataset",
        default="evaluation/gold_set_retrieval.jsonl",
    )
    parser.add_argument(
        "--output",
        default="evaluation/retrieval_report.json",
    )
    args = parser.parse_args()
    run(args.dataset, args.output)
