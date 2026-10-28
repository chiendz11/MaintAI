"""Train baseline Logistic Regression và đánh giá trên validation."""

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import joblib
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_fscore_support,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


ROOT = Path(__file__).resolve().parents[3]
TARGET = "Machine failure"

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


def main():
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    verifier = (
        ROOT / "ml-service/training/data_preparation/create_split.py"
    )

    subprocess.run(
        [sys.executable, str(verifier), "--verify-only"],
        cwd=ROOT,
        check=True,
    )

    manifest = json.loads(split_path.read_text(encoding="utf-8"))
    data = pd.read_csv(ROOT / "sensor/data/ai4i2020.csv")

    # Chỉ lấy sáu feature được phép. Không dùng IDs/fault labels.
    features = data[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    labels = data[TARGET]

    train_indices = manifest["indices"]["train"]
    validation_indices = manifest["indices"]["validation"]

    x_train = features.iloc[train_indices]
    y_train = labels.iloc[train_indices]
    x_validation = features.iloc[validation_indices]
    y_validation = labels.iloc[validation_indices]

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
            ("numeric", StandardScaler(), NUMERIC),
        ],
        remainder="drop",
    )

    pipeline = Pipeline([
        ("preprocessing", preprocessing),
        (
            "model",
            LogisticRegression(
                max_iter=1000,
                solver="lbfgs",
                random_state=42,
            ),
        ),
    ])

    # Fit toàn bộ preprocessing và model chỉ trên train.
    pipeline.fit(x_train, y_train)

    classes = list(pipeline.named_steps["model"].classes_)
    failure_index = classes.index(1)
    probabilities = pipeline.predict_proba(x_validation)[:, failure_index]

    # Mốc ban đầu, chưa phải threshold được tối ưu/chốt để release.
    threshold = 0.5
    predictions = (probabilities >= threshold).astype(int)

    precision, recall, f1, _ = precision_recall_fscore_support(
        y_validation,
        predictions,
        average="binary",
        zero_division=0,
    )
    tn, fp, fn, tp = confusion_matrix(
        y_validation, predictions, labels=[0, 1]
    ).ravel()

    metrics = {
        "precision_failure": float(precision),
        "recall_failure": float(recall),
        "f1_failure": float(f1),
        "pr_auc_average_precision": float(
            average_precision_score(y_validation, probabilities)
        ),
        "false_negative_rate": float(fn / (fn + tp)),
        "false_positives_per_1000_records": float(
            1000 * fp / len(y_validation)
        ),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
    }

    output_root = ROOT / "ml-service/artifacts/baselines"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="logreg-", dir=output_root))

    artifact_path = output / "pipeline.joblib"
    joblib.dump(pipeline, artifact_path)

    # Kiểm tra artifact load lại cho score nhất quán trên validation.
    restored = joblib.load(artifact_path)
    restored_probabilities = restored.predict_proba(
        x_validation
    )[:, failure_index]

    import numpy as np

    if not np.allclose(
        probabilities, restored_probabilities, rtol=1e-12, atol=1e-12
    ):
        raise ValueError("Prediction thay đổi sau save/load")

    metadata = {
        "status": "local_baseline_not_approved_release",
        "model_version": "logreg-baseline-v0",
        "preprocessor_version": "onehot-standardscaler-v0",
        "threshold": threshold,
        "threshold_status": "initial_default_not_tuned",
        "features": FEATURES,
        "evaluation_split": "validation",
        "train_rows": len(x_train),
        "validation_rows": len(x_validation),
        "source_sha256": manifest["source_sha256"],
        "split_sha256": hashlib.sha256(split_path.read_bytes()).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "pipeline_sha256": hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
        "source_git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "sklearn_version": sklearn.__version__,
        "python_version": sys.version,
        "model_params": pipeline.named_steps["model"].get_params(),
        "metrics": metrics,
    }

    (output / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print("\nBASELINE: Logistic Regression")
    print("Threshold: 0.5 — initial default, not tuned")
    print("Evaluation: validation only")
    print(json.dumps(metrics, indent=2))
    print("Artifact save/load: PASS")
    print("Output:", output)


if __name__ == "__main__":
    main()