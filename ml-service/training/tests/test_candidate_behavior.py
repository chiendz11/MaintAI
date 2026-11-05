"""Kiểm tra hành vi pipeline thật bằng dữ liệu validation."""

import hashlib
import json
import os
import unittest
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
SHARED_PREPROCESSING = ROOT / "ml-service/shared/preprocessing"
sys.path.insert(0, str(SHARED_PREPROCESSING))

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}
FEATURES = list(COLUMN_MAP.values())


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestCandidateBehavior(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configured = os.environ.get("MAINTAI_BENCHMARK_DIR")
        if not configured:
            raise RuntimeError("Set MAINTAI_BENCHMARK_DIR before running.")

        cls.directory = Path(configured).resolve()
        metadata = json.loads(
            (cls.directory / "benchmark_metadata.json").read_text(
                encoding="utf-8"
            )
        )

        source = ROOT / "sensor/data/ai4i2020.csv"
        split_path = (
            ROOT / "ml-service/training/data_preparation/splits"
            / "ai4i-stratified-v1-seed42.json"
        )

        if sha256(source) != metadata["source_sha256"]:
            raise ValueError("Dataset checksum mismatch.")
        if sha256(split_path) != metadata["split_sha256"]:
            raise ValueError("Split checksum mismatch.")

        manifest = json.loads(split_path.read_text(encoding="utf-8"))
        cls.indices = manifest["indices"]["validation"]

        data = pd.read_csv(source)
        cls.features = (
            data.iloc[cls.indices][list(COLUMN_MAP)]
            .rename(columns=COLUMN_MAP)
            .reset_index(drop=True)
        )

        cls.pipelines = {}
        cls.saved_scores = {}

        candidates = [
            name.strip()
            for name in os.environ.get(
                "MAINTAI_CANDIDATES",
                "random_forest,hist_gradient_boosting",
            ).split(",")
            if name.strip()
        ]

        allowed = {
            "random_forest",
            "hist_gradient_boosting",
            "rf_interactions",
            "hgb_interactions",
        }
        if not candidates or not set(candidates).issubset(allowed):
            raise ValueError("Invalid candidate list.")

        for candidate in candidates:
            folder = cls.directory / candidate
            experiment = json.loads(
                (folder / "experiment.json").read_text(encoding="utf-8")
            )
            if "transformer_sha256" in experiment:
                transformer = SHARED_PREPROCESSING / "feature_interactions.py"
                if sha256(transformer) != experiment["transformer_sha256"]:
                    raise ValueError(
                        f"{candidate}: transformer source checksum mismatch."
                    )
            artifact = folder / "pipeline.joblib"

            if sha256(artifact) != experiment["pipeline_sha256"]:
                raise ValueError(f"{candidate}: artifact checksum mismatch.")

            # Chỉ load các artifact tin cậy do chính nhóm tạo.
            cls.pipelines[candidate] = joblib.load(artifact)
            saved = pd.read_csv(folder / "validation_predictions.csv")

            if saved["row_index"].tolist() != cls.indices:
                raise ValueError(f"{candidate}: saved indices mismatch.")

            cls.saved_scores[candidate] = saved["failure_score"].to_numpy()

    def test_classes_and_feature_identity(self):
        for name, pipeline in self.pipelines.items():
            with self.subTest(candidate=name):
                self.assertEqual(list(pipeline.classes_), [0, 1])
                self.assertEqual(list(pipeline.feature_names_in_), FEATURES)

    def test_probability_output_is_valid(self):
        for name, pipeline in self.pipelines.items():
            with self.subTest(candidate=name):
                probabilities = pipeline.predict_proba(self.features)

                self.assertEqual(
                    probabilities.shape, (len(self.features), 2)
                )
                self.assertTrue(np.all(np.isfinite(probabilities)))
                self.assertTrue(np.all(probabilities >= 0))
                self.assertTrue(np.all(probabilities <= 1))
                np.testing.assert_allclose(
                    probabilities.sum(axis=1),
                    1.0,
                    rtol=1e-10,
                    atol=1e-10,
                )

    def test_loaded_scores_match_benchmark(self):
        for name, pipeline in self.pipelines.items():
            with self.subTest(candidate=name):
                scores = pipeline.predict_proba(self.features)[:, 1]
                np.testing.assert_allclose(
                    scores, self.saved_scores[name],
                    rtol=1e-10, atol=1e-10,
                )

    def test_column_order_does_not_change_scores(self):
        reordered = self.features[list(reversed(FEATURES))]

        for name, pipeline in self.pipelines.items():
            with self.subTest(candidate=name):
                np.testing.assert_allclose(
                    pipeline.predict_proba(self.features),
                    pipeline.predict_proba(reordered),
                    rtol=1e-10, atol=1e-10,
                )

    def test_row_order_only_reorders_predictions(self):
        order = np.random.default_rng(42).permutation(len(self.features))

        for name, pipeline in self.pipelines.items():
            with self.subTest(candidate=name):
                original = pipeline.predict_proba(self.features)
                reordered = pipeline.predict_proba(
                    self.features.iloc[order]
                )
                np.testing.assert_allclose(
                    original[order], reordered,
                    rtol=1e-10, atol=1e-10,
                )

    def test_single_prediction_matches_batch(self):
        # Chọn cố định theo vị trí, không chọn theo kết quả tốt/xấu.
        positions = [0, 1, 10, 100, len(self.features) - 1]

        for name, pipeline in self.pipelines.items():
            with self.subTest(candidate=name):
                batch = pipeline.predict_proba(self.features)
                for position in positions:
                    single = pipeline.predict_proba(
                        self.features.iloc[[position]]
                    )
                    np.testing.assert_allclose(
                        batch[position], single[0],
                        rtol=1e-10, atol=1e-10,
                    )


if __name__ == "__main__":
    unittest.main()