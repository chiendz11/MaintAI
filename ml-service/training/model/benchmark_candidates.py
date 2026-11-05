"""So sánh candidates trên validation; không đánh giá test."""

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from time import perf_counter

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.tree import DecisionTreeClassifier


ROOT = Path(__file__).resolve().parents[3]
SEED = 42

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}
FEATURES = list(COLUMN_MAP.values())
NUMERIC = FEATURES[1:]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(
        json.dumps(
            value, indent=2, ensure_ascii=False,
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def metrics(y, prediction):
    """Metrics lớp Failure; quy ước precision=0 nếu không báo Failure."""
    y = np.asarray(y, dtype=int)
    prediction = np.asarray(prediction, dtype=int)

    tp = int(np.sum((y == 1) & (prediction == 1)))
    fp = int(np.sum((y == 0) & (prediction == 1)))
    fn = int(np.sum((y == 1) & (prediction == 0)))
    tn = int(np.sum((y == 0) & (prediction == 0)))

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0
    f2 = 5 * tp / (5 * tp + fp + 4 * fn) if 5 * tp + fp + 4 * fn else 0.0

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "f2": f2,
        "fnr": fn / (fn + tp) if fn + tp else 0.0,
        "fp_per_1000": 1000 * fp / len(y),
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
        "gate_pass": recall >= 0.85 and precision >= 0.50,
    }


def threshold_table(y, scores):
    """Xét mọi score khác nhau và hai điểm biên."""
    thresholds = np.unique(np.r_[0.0, scores, 1.0])
    rows = []

    for threshold in thresholds:
        row = metrics(y, scores >= threshold)
        row["threshold"] = float(threshold)
        rows.append(row)

    return rows


def choose_operating_point(rows):
    feasible = [row for row in rows if row["gate_pass"]]
    pool = feasible if feasible else rows

    selected = max(
        pool,
        key=lambda row: (
            row["f2"],
            row["recall"],
            row["precision"],
            row["threshold"],
        ),
    )
    return dict(selected)


def make_pipeline(model, scale):
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
            (
                "numeric",
                StandardScaler() if scale else "passthrough",
                NUMERIC,
            ),
        ],
        remainder="drop",
    )
    return Pipeline([
        ("preprocessing", preprocessing),
        ("model", model),
    ])


def main():
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    source_path = ROOT / "sensor/data/ai4i2020.csv"
    verifier = ROOT / "ml-service/training/data_preparation/create_split.py"

    subprocess.run(
        [sys.executable, str(verifier), "--verify-only"],
        cwd=ROOT,
        check=True,
    )

    manifest = json.loads(split_path.read_text(encoding="utf-8"))
    data = pd.read_csv(source_path)
    features = data[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    labels = data["Machine failure"]

    # Không lấy test để predict, chọn threshold hoặc đánh giá.
    train_indices = manifest["indices"]["train"]
    validation_indices = manifest["indices"]["validation"]

    x_train = features.iloc[train_indices].copy()
    y_train = labels.iloc[train_indices].to_numpy(dtype=int)
    x_val = features.iloc[validation_indices].copy()
    y_val = labels.iloc[validation_indices].to_numpy(dtype=int)

    output_root = ROOT / "ml-service/artifacts/benchmarks"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="candidates-", dir=output_root))

    candidates = [
        (
            "logreg",
            LogisticRegression(
                max_iter=1000, solver="lbfgs", random_state=SEED,
            ),
            True,
        ),
        (
            "logreg_balanced",
            LogisticRegression(
                max_iter=1000, solver="lbfgs",
                class_weight="balanced", random_state=SEED,
            ),
            True,
        ),
        (
            "decision_tree",
            DecisionTreeClassifier(
                max_depth=6, min_samples_leaf=5,
                class_weight="balanced", random_state=SEED,
            ),
            False,
        ),
        (
            "random_forest",
            RandomForestClassifier(
                n_estimators=300,
                min_samples_leaf=2,
                class_weight="balanced_subsample",
                random_state=SEED,
                n_jobs=1,
            ),
            False,
        ),
        (
            "hist_gradient_boosting",
            HistGradientBoostingClassifier(
                max_iter=200,
                learning_rate=0.1,
                max_leaf_nodes=15,
                min_samples_leaf=20,
                l2_regularization=1.0,
                early_stopping=False,
                random_state=SEED,
            ),
            False,
        ),
    ]

    summary = []

    dummy = metrics(y_val, np.zeros(len(y_val), dtype=int))
    summary.append({
        "candidate": "always_normal",
        "operating_point": "fixed_label",
        "threshold": None,
        "average_precision": None,
        **dummy,
    })

    torque_limit = float(x_train["torque_nm"].quantile(0.99))
    rule = metrics(y_val, x_val["torque_nm"].to_numpy() >= torque_limit)
    summary.append({
        "candidate": "rule_torque_train_q99",
        "operating_point": "fixed_rule",
        "threshold": None,
        "average_precision": None,
        **rule,
    })

    write_json(output / "rule.json", {
        "rule": "FAILURE if torque_nm >= torque_limit_nm else NORMAL",
        "torque_limit_nm": torque_limit,
        "quantile": 0.99,
        "quantile_source": "train",
        "status": "statistical_comparator_not_engineering_safety_limit",
    })

    for name, model, scale in candidates:
        print(f"Training: {name}", flush=True)
        candidate_dir = output / name
        candidate_dir.mkdir()

        pipeline = make_pipeline(model, scale)

        start = perf_counter()
        pipeline.fit(x_train, y_train)
        fit_seconds = perf_counter() - start

        if list(pipeline.classes_) != [0, 1]:
            raise ValueError(f"{name}: unexpected classes")

        # Warm-up trước khi đo thời gian dự đoán cả batch validation.
        pipeline.predict_proba(x_val.iloc[:10])

        start = perf_counter()
        scores = pipeline.predict_proba(x_val)[:, 1]
        predict_seconds = perf_counter() - start

        if (
            not np.all(np.isfinite(scores))
            or np.any(scores < 0)
            or np.any(scores > 1)
        ):
            raise ValueError(f"{name}: invalid scores")

        ap = float(average_precision_score(y_val, scores))
        rows = threshold_table(y_val, scores)
        selected = choose_operating_point(rows)

        default = metrics(y_val, scores >= 0.5)
        default["threshold"] = 0.5

        artifact = candidate_dir / "pipeline.joblib"
        joblib.dump(pipeline, artifact)
        restored = joblib.load(artifact)
        restored_scores = restored.predict_proba(x_val)[:, 1]

        np.testing.assert_allclose(
            scores, restored_scores, rtol=1e-12, atol=1e-12,
        )

        for operating_point, result in [
            ("default_0.5", default),
            ("validation_selected", selected),
        ]:
            summary.append({
                "candidate": name,
                "operating_point": operating_point,
                "average_precision": ap,
                "fit_seconds": fit_seconds,
                "predict_batch_seconds": predict_seconds,
                "artifact_bytes": artifact.stat().st_size,
                **result,
            })

        pd.DataFrame(rows).to_csv(
            candidate_dir / "thresholds.csv", index=False,
        )
        pd.DataFrame({
            "row_index": validation_indices,
            "target": y_val,
            "failure_score": scores,
        }).to_csv(candidate_dir / "validation_predictions.csv", index=False)

        write_json(candidate_dir / "experiment.json", {
            "candidate": name,
            "status": "candidate_not_approved_release",
            "model_params": model.get_params(),
            "numeric_scaling": scale,
            "categorical_order": ["L", "M", "H"],
            "features": FEATURES,
            "default_operating_point": default,
            "validation_selected_operating_point": selected,
            "average_precision": ap,
            "pipeline_sha256": sha256(artifact),
            "save_load_check": "PASS",
        })

    table = pd.DataFrame(summary)
    table.to_csv(output / "comparison.csv", index=False)

    git_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
    ).strip()
    git_status = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=ROOT, text=True,
    )

    write_json(output / "benchmark_metadata.json", {
        "status": "validation_benchmark_not_final_selection",
        "evaluation_split": "validation",
        "seed": SEED,
        "train_rows": len(y_train),
        "validation_rows": len(y_val),
        "source_sha256": sha256(source_path),
        "split_sha256": sha256(split_path),
        "script_sha256": sha256(Path(__file__)),
        "protocol_sha256": sha256(
            ROOT / "ml-service/training/evaluation/BENCHMARK_PROTOCOL.md"
        ),
        "source_git_commit": git_commit,
        "working_tree_status": git_status,
        "python_version": sys.version,
        "sklearn_version": sklearn.__version__,
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
        "joblib_version": joblib.__version__,
        "threshold_policy": "gate_then_f2_recall_precision_threshold",
        "timing_scope": "single_warmed_validation_batch_not_API_latency",
        "test_evaluated": False,
    })

    columns = [
        "candidate", "operating_point", "threshold",
        "precision", "recall", "f2", "average_precision",
        "fp", "fn", "gate_pass",
    ]
    print("\nVALIDATION COMPARISON")
    print(table[columns].round(5).to_string(index=False))
    print("\nOutput:", output)
    print("Test evaluated: NO")
    print("Model selected or released: NO")


if __name__ == "__main__":
    main()