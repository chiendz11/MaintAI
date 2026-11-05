import pytest
from evaluation.ranking_metrics import reciprocal_rank, ndcg_at_k, retrieval_ranking_metrics

A = {'document_id': 'manual', 'chunk_id': 'a'}
B = {'document_id': 'manual', 'chunk_id': 'b'}
C = {'document_id': 'manual', 'chunk_id': 'c'}

def test_perfect_ranking():
    assert reciprocal_rank([A, B], [A, B], 5) == 1.0
    assert ndcg_at_k([A, B], [A, B], 5) == pytest.approx(1.0)

def test_second_place():
    assert reciprocal_rank([C, A], [A], 5) == pytest.approx(0.5)
    assert 0 < ndcg_at_k([C, A], [A], 5) < 1

def test_missed_source():
    assert reciprocal_rank([C], [A], 5) == 0
    assert ndcg_at_k([C], [A], 5) == 0

def test_empty_gold_is_not_scored():
    assert retrieval_ranking_metrics([A], []) == {'mrr_at_5': None, 'ndcg_at_5': None}

def test_duplicate_hits_do_not_inflate_ndcg():
    assert ndcg_at_k([A, A], [A, B], 5) < 1

def test_invalid_k():
    with pytest.raises(ValueError):
        ndcg_at_k([A], [A], 0)
