"""Kiểm tra metrics và chính sách chọn threshold bằng ví dụ biết đáp án."""

import importlib.util
import unittest
from pathlib import Path

import numpy as np
from sklearn.metrics import (
    confusion_matrix,
    fbeta_score,
    precision_score,
    recall_score,
)


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "model"
    / "benchmark_candidates.py"
)
SPEC = importlib.util.spec_from_file_location(
    "maintai_benchmark_candidates", MODULE_PATH
)
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


class TestBenchmarkCandidates(unittest.TestCase):
    def test_metrics_match_known_confusion_matrix(self):
        # TP=2, FP=1, FN=1, TN=2.
        y = np.array([1, 1, 1, 0, 0, 0])
        predicted = np.array([1, 1, 0, 1, 0, 0])

        result = benchmark.metrics(y, predicted)

        self.assertEqual(
            (result["tn"], result["fp"], result["fn"], result["tp"]),
            (2, 1, 1, 2),
        )
        self.assertAlmostEqual(result["precision"], 2 / 3)
        self.assertAlmostEqual(result["recall"], 2 / 3)
        self.assertAlmostEqual(result["f2"], 2 / 3)
        self.assertAlmostEqual(result["fnr"], 1 / 3)
        self.assertAlmostEqual(result["fp_per_1000"], 1000 / 6)

    def test_metrics_match_sklearn_on_many_predictions(self):
        # Đối chiếu công thức tự viết với implementation độc lập.
        rng = np.random.default_rng(42)
        y = np.r_[np.zeros(80, dtype=int), np.ones(20, dtype=int)]

        for _ in range(30):
            predicted = rng.integers(0, 2, size=len(y))
            result = benchmark.metrics(y, predicted)

            tn, fp, fn, tp = confusion_matrix(
                y, predicted, labels=[0, 1]
            ).ravel()

            self.assertEqual(
                (result["tn"], result["fp"], result["fn"], result["tp"]),
                (tn, fp, fn, tp),
            )
            self.assertAlmostEqual(
                result["precision"],
                precision_score(y, predicted, zero_division=0),
            )
            self.assertAlmostEqual(
                result["recall"],
                recall_score(y, predicted, zero_division=0),
            )
            self.assertAlmostEqual(
                result["f2"],
                fbeta_score(y, predicted, beta=2, zero_division=0),
            )

    def test_always_normal_fails_gate(self):
        result = benchmark.metrics([0, 0, 1, 1], [0, 0, 0, 0])

        self.assertEqual(result["precision"], 0)
        self.assertEqual(result["recall"], 0)
        self.assertEqual(result["fn"], 2)
        self.assertFalse(result["gate_pass"])

    def test_gate_accepts_exact_boundary(self):
        # 20 Failure, bắt đúng 17 => recall=0.85.
        # 17 TP và 17 FP => precision=0.50.
        y = np.r_[np.ones(20, dtype=int), np.zeros(20, dtype=int)]
        predicted = np.r_[
            np.ones(17, dtype=int),
            np.zeros(3, dtype=int),
            np.ones(17, dtype=int),
            np.zeros(3, dtype=int),
        ]

        result = benchmark.metrics(y, predicted)

        self.assertAlmostEqual(result["recall"], 0.85)
        self.assertAlmostEqual(result["precision"], 0.50)
        self.assertTrue(result["gate_pass"])

    def test_gate_rejects_high_recall_with_low_precision(self):
        y = np.r_[np.ones(20, dtype=int), np.zeros(80, dtype=int)]
        result = benchmark.metrics(y, np.ones(100, dtype=int))

        self.assertEqual(result["recall"], 1.0)
        self.assertAlmostEqual(result["precision"], 0.2)
        self.assertFalse(result["gate_pass"])

    def test_threshold_boundary_uses_greater_or_equal(self):
        rows = benchmark.threshold_table(
            np.array([1, 0]),
            np.array([0.4, 0.2]),
        )
        selected = next(row for row in rows if row["threshold"] == 0.4)

        self.assertEqual(selected["tp"], 1)
        self.assertEqual(selected["fp"], 0)
        self.assertEqual(selected["fn"], 0)

    def test_threshold_table_includes_edges_and_unique_scores(self):
        rows = benchmark.threshold_table(
            np.array([0, 1, 1, 0]),
            np.array([0.2, 0.4, 0.4, 0.8]),
        )

        self.assertEqual(
            [row["threshold"] for row in rows],
            [0.0, 0.2, 0.4, 0.8, 1.0],
        )

    def test_gate_has_priority_over_higher_f2(self):
        # Score chỉ có hai mức 0.2 và 0.8.
        #
        # Threshold 0.2:
        # TP=20, FP=30, FN=0.
        # Precision=0.40, recall=1.00, F2≈0.7692 => FAIL gate.
        #
        # Threshold 0.8:
        # TP=17, FP=17, FN=3.
        # Precision=0.50, recall=0.85, F2≈0.7456 => PASS gate.
        y = np.r_[
            np.ones(20, dtype=int),
            np.zeros(30, dtype=int),
        ]
        scores = np.r_[
            np.full(17, 0.8),
            np.full(3, 0.2),
            np.full(17, 0.8),
            np.full(13, 0.2),
        ]

        rows = benchmark.threshold_table(y, scores)
        selected = benchmark.choose_operating_point(rows)

        lower = next(
            row for row in rows if row["threshold"] == 0.2
        )
        higher = next(
            row for row in rows if row["threshold"] == 0.8
        )

        self.assertFalse(lower["gate_pass"])
        self.assertAlmostEqual(lower["precision"], 0.40)
        self.assertAlmostEqual(lower["recall"], 1.00)

        self.assertTrue(higher["gate_pass"])
        self.assertAlmostEqual(higher["precision"], 0.50)
        self.assertAlmostEqual(higher["recall"], 0.85)

        self.assertGreater(lower["f2"], higher["f2"])
        self.assertEqual(selected, higher)

    def test_no_feasible_threshold_remains_failed(self):
        # Hai lớp có cùng score nên không thể tách bằng threshold.
        rows = benchmark.threshold_table(
            np.array([1, 0, 0, 0]),
            np.array([0.5, 0.5, 0.5, 0.5]),
        )
        selected = benchmark.choose_operating_point(rows)

        self.assertFalse(selected["gate_pass"])
        self.assertEqual(
            selected["f2"],
            max(row["f2"] for row in rows),
        )

    def test_row_order_does_not_change_operating_point(self):
        y = np.array([1, 0, 1, 0, 1, 0])
        scores = np.array([0.9, 0.1, 0.7, 0.4, 0.5, 0.2])
        order = np.array([5, 2, 0, 4, 1, 3])

        original = benchmark.choose_operating_point(
            benchmark.threshold_table(y, scores)
        )
        reordered = benchmark.choose_operating_point(
            benchmark.threshold_table(y[order], scores[order])
        )

        self.assertEqual(original, reordered)


if __name__ == "__main__":
    unittest.main()