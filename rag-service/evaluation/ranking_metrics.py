"""Ranking metrics for MaintAI RAG evaluation (no third-party dependencies)."""
from __future__ import annotations
import math
from typing import Any


def _key(item: dict[str, Any]) -> tuple[str, str]:
    return str(item['document_id']), str(item['chunk_id'])


def reciprocal_rank(retrieved: list[dict], expected: list[dict], k: int | None = None) -> float | None:
    if not expected:
        return None
    gold = {_key(x) for x in expected}
    for rank, item in enumerate(retrieved[:k] if k is not None else retrieved, 1):
        if _key(item) in gold:
            return 1.0 / rank
    return 0.0


def ndcg_at_k(retrieved: list[dict], expected: list[dict], k: int) -> float | None:
    if k <= 0:
        raise ValueError('k must be positive')
    if not expected:
        return None
    gold = {_key(x) for x in expected}
    seen: set[tuple[str, str]] = set()
    dcg = 0.0
    for rank, item in enumerate(retrieved[:k], 1):
        key = _key(item)
        if key in gold and key not in seen:
            dcg += 1 / math.log2(rank + 1)
        seen.add(key)
    ideal = sum(1 / math.log2(rank + 1) for rank in range(1, min(k, len(gold)) + 1))
    return dcg / ideal


def retrieval_ranking_metrics(retrieved: list[dict], expected: list[dict]) -> dict:
    return {'mrr_at_5': reciprocal_rank(retrieved, expected, 5),
            'ndcg_at_5': ndcg_at_k(retrieved, expected, 5)}
