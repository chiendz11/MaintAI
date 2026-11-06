"""Tạo bundle model HGB Candidate hoàn chỉnh cho thử nghiệm Shadow Rollout."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# Thêm đường dẫn tới shared preprocessing
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "ml-service/shared/preprocessing"))
from feature_interactions import SensorInteractions  # noqa: E402

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}
RAW_FEATURES = list(COLUMN_MAP.values())
DERIVED_FEATURES = [
    "temperature_difference_k",
    "mechanical_power_w",
    "wear_torque_interaction",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def histogram(values: np.ndarray, bins: np.ndarray) -> list[float]:
    indexes = np.searchsorted(bins, values, side="right") - 1
    indexes = np.clip(indexes, 0, len(bins) - 2)
    counts = np.bincount(indexes, minlength=len(bins) - 1)
    return (counts / len(values)).tolist()


def main() -> None:
    split_path = (
        ROOT
        / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    data_path = ROOT / "sensor/data/ai4i2020.csv"
    transformer_path = (
        ROOT / "ml-service/shared/preprocessing/feature_interactions.py"
    )

    split = json.loads(split_path.read_text(encoding="utf-8"))
    data = pd.read_csv(data_path)
    features = data[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    labels = data["Machine failure"]

    train_indices = split["indices"]["train"]
    val_indices = split["indices"]["validation"]

    x_train = features.iloc[train_indices].copy()
    y_train = labels.iloc[train_indices].to_numpy(dtype=int)
    x_val = features.iloc[val_indices].copy()
    y_val = labels.iloc[val_indices].to_numpy(dtype=int)

    numeric_features = RAW_FEATURES[1:] + DERIVED_FEATURES
    preprocessing = ColumnTransformer(
        transformers=[
            (
                "category",
                OneHotEncoder(
                    categories=[["L", "M", "H"]],
                    handle_unknown="error",
                    sparse_output=False,
                ),
                ["product_type"],
            ),
            ("numeric", "passthrough", numeric_features),
        ],
        remainder="drop",
    )

    model_params = {
        "early_stopping": False,
        "random_state": 42,
        "l2_regularization": 1.0,
        "max_iter": 300,
        "learning_rate": 0.05,
        "max_leaf_nodes": 31,
        "min_samples_leaf": 10,
    }

    pipeline = Pipeline(
        [
            ("interactions", SensorInteractions(enabled=True)),
            ("preprocessing", preprocessing),
            ("model", HistGradientBoostingClassifier(**model_params)),
        ]
    )

    print("Fitting HGB pipeline...")
    pipeline.fit(x_train, y_train)

    # Đánh giá trên validation
    scores = pipeline.predict_proba(x_val)[:, 1]
    threshold = 0.08943418823732646
    predictions = (scores >= threshold).astype(int)

    tp = int(np.sum((y_val == 1) & (predictions == 1)))
    fp = int(np.sum((y_val == 0) & (predictions == 1)))
    fn = int(np.sum((y_val == 1) & (predictions == 0)))
    tn = int(np.sum((y_val == 0) & (predictions == 0)))

    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    f1 = 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) > 0 else 0.0
    f2 = 5 * tp / (5 * tp + fp + 4 * fn) if (5 * tp + fp + 4 * fn) > 0 else 0.0

    print(
        f"Validation Evaluation: TP={tp}, FP={fp}, FN={fn}, TN={tn}, "
        f"Recall={recall:.4f}, Precision={precision:.4f}, F2={f2:.4f}"
    )

    # Thư mục đích
    targets = [
        ROOT / "ml-service/artifacts/integration/hgb-interactions-m56w5ff4",
        Path("/models/candidate_dest"),
    ]

    for target_dir in targets:
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / "source").mkdir(parents=True, exist_ok=True)

        joblib.dump(pipeline, target_dir / "pipeline.joblib")
        shutil.copyfile(
            transformer_path, target_dir / "source/feature_interactions.py"
        )

        pipeline_sha = sha256(target_dir / "pipeline.joblib")
        transformer_sha = sha256(transformer_path)
        source_sha = sha256(data_path)
        split_sha = sha256(split_path)

        metadata = {
            "status": "local_integration_candidate_not_approved_release",
            "approved_for_release": False,
            "model_version": "hgb-interactions-candidate-v1",
            "preprocessor_version": "sensor-interactions-onehot-v1",
            "dataset_version": "ai4i2020-train-split-v1-seed42",
            "threshold": threshold,
            "threshold_status": "selected_on_reused_validation",
            "features": RAW_FEATURES,
            "derived_features": DERIVED_FEATURES,
            "transformer_module": "feature_interactions",
            "transformer_sha256": transformer_sha,
            "pipeline_sha256": pipeline_sha,
            "source_sha256": source_sha,
            "split_sha256": split_sha,
            "train_rows": len(train_indices),
            "validation_rows": len(val_indices),
            "evaluation_split": "validation",
            "sklearn_version": sklearn.__version__,
            "python_version": sys.version,
            "model_params": model_params,
            "validation_metrics": {
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "f2": f2,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "tn": tn,
            },
        }

        (target_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
        )

        evaluation = {
            "report_kind": "local_hgb_validation_evaluation",
            "model_version": "hgb-interactions-candidate-v1",
            "threshold": threshold,
            "metrics": {
                "precision_failure": precision,
                "recall_failure": recall,
                "f1_failure": f1,
                "f2_failure": f2,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "tn": tn,
            },
            "quality_gate": {
                "status": "PASS" if recall >= 0.85 and precision >= 0.50 else "FAIL",
                "checks": {
                    "recall_failure": {
                        "minimum": 0.85,
                        "actual": recall,
                        "passed": recall >= 0.85,
                    },
                    "precision_failure": {
                        "minimum": 0.50,
                        "actual": precision,
                        "passed": precision >= 0.50,
                    },
                },
            },
            "approved_for_release": False,
            "intended_use": "shadow_pipeline_integration",
        }

        (target_dir / "evaluation.json").write_text(
            json.dumps(evaluation, indent=2) + "\n", encoding="utf-8"
        )

        # Tính drift reference trên train
        train_df = x_train.copy()
        categories = ["L", "M", "H"]
        profiles = {
            "product_type": {
                "type": "categorical",
                "metric": "PSI",
                "categories": categories,
                "probabilities": [
                    float((train_df["product_type"] == c).mean())
                    for c in categories
                ],
            }
        }
        for name in RAW_FEATURES[1:]:
            vals = train_df[name].to_numpy(dtype=float)
            bins = np.unique(np.quantile(vals, np.linspace(0, 1, 11)))
            if len(bins) < 3:
                lo, hi = float(vals.min()), float(vals.max())
                pad = max(1.0, abs(lo) * 0.01)
                bins = np.linspace(lo - pad, hi + pad, 11)
            profiles[name] = {
                "type": "numeric",
                "metric": "PSI",
                "bins": bins.tolist(),
                "probabilities": histogram(vals, bins),
            }

        train_scores = pipeline.predict_proba(train_df)[:, 1]
        failure_rate = float((train_scores >= threshold).mean())

        drift_ref = {
            "schema_version": "1.0.0",
            "dataset_version": "ai4i2020-train-split-v1-seed42",
            "model_version": "hgb-interactions-candidate-v1",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "sample_count": len(train_df),
            "features": profiles,
            "prediction_reference": {"failure_rate": failure_rate},
        }

        (target_dir / "drift_reference.json").write_text(
            json.dumps(drift_ref, indent=2) + "\n", encoding="utf-8"
        )

        manifest = {
            "scope": "local_integration_not_approved_release",
            "model_version": "hgb-interactions-candidate-v1",
            "files": {
                p.relative_to(target_dir).as_posix(): sha256(p)
                for p in sorted(target_dir.rglob("*"))
                if p.is_file()
            },
        }
        (target_dir / "bundle-manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        print(f"Exported bundle to {target_dir}")


if __name__ == "__main__":
    main()
