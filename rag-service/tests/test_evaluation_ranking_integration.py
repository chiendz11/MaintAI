import pytest
from evaluation.evaluator import evaluate_retrieval, aggregate

A = {'document_id': 'manual', 'chunk_id': 'a'}
B = {'document_id': 'manual', 'chunk_id': 'b'}
C = {'document_id': 'manual', 'chunk_id': 'c'}


def test_evaluate_retrieval_contains_new_metrics():
    metrics = evaluate_retrieval([C, A, B], [A, B])
    assert metrics['recall_at_1'] == 0
    assert metrics['recall_at_3'] == 1
    assert metrics['mrr_at_5'] == pytest.approx(0.5)
    assert 0 < metrics['ndcg_at_5'] < 1


def test_aggregate_contains_ranking_metrics():
    case = {'retrieval': evaluate_retrieval([A], [A]), 'grounding': None}
    report = aggregate([case])
    assert report['mrr_at_5'] == 1
    assert report['ndcg_at_5'] == 1
    assert report['completed_cases'] == 1


def test_no_expected_sources_unscored():
    metrics = evaluate_retrieval([A], [])
    assert metrics['mrr_at_5'] is None
    assert metrics['ndcg_at_5'] is None
