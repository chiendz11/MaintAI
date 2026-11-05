"""Đối chứng features tương tác bằng cùng folds CV trên train."""

import argparse
import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.metrics import average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


ROOT = Path(__file__).resolve().parents[3]
SHARED = ROOT / "ml-service/shared/preprocessing"
sys.path.insert(0, str(SHARED))

from feature_interactions import (
    DERIVED_FEATURES,
    RAW_FEATURES,
    SensorInteractions,
)

HELPER_PATH = Path(__file__).with_name("benchmark_candidates.py")
SPEC = importlib.util.spec_from_file_location("benchmark_helpers", HELPER_PATH)
helpers = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helpers)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def make_pipeline(model, enabled):
    numeric = RAW_FEATURES[1:] + (
        DERIVED_FEATURES if enabled else []
    )

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
            ("numeric", "passthrough", numeric),
        ],
        remainder="drop",
    )

    return Pipeline([
        ("interactions", SensorInteractions(enabled=enabled)),
        ("preprocessing", preprocessing),
        ("model", model),
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cv-dir", required=True, type=Path)
    args = parser.parse_args()
    cv_dir = args.cv_dir.resolve()

    cv_protocol = read_json(cv_dir / "protocol.json")
    saved_folds = read_json(cv_dir / "folds.json")
    old_fold_metrics = pd.read_csv(cv_dir / "fold_metrics.csv")

    if sklearn.__version__ != cv_protocol["sklearn_version"]:
        raise ValueError("Scikit-learn version changed.")

    source_path = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )

    if helpers.sha256(source_path) != cv_protocol["source_sha256"]:
        raise ValueError("Dataset changed.")
    if helpers.sha256(split_path) != cv_protocol["split_sha256"]:
        raise ValueError("Split changed.")

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "ml-service/training/data_preparation/create_split.py"),
            "--verify-only",
        ],
        cwd=ROOT,
        check=True,
    )

    manifest = read_json(split_path)
    train_indices = manifest["indices"]["train"]
    train_set = set(train_indices)

    if len(saved_folds) != 5:
        raise ValueError("Expected exactly five CV folds.")

    # Kiểm tra fold chỉ nằm trong train và mỗi dòng được đánh giá một lần.
    evaluation_counts = {index: 0 for index in train_indices}

    for fold in saved_folds:
        fit = fold["fit_source_indices"]
        evaluation = fold["evaluation_source_indices"]

        if len(fit) != len(set(fit)) or len(evaluation) != len(set(evaluation)):
            raise ValueError("Duplicate indices inside a fold.")
        if set(fit) & set(evaluation):
            raise ValueError("Fold overlap.")
        if set(fit) | set(evaluation) != train_set:
            raise ValueError("Fold does not partition the original train.")

        for index in evaluation:
            evaluation_counts[index] += 1

    if any(count != 1 for count in evaluation_counts.values()):
        raise ValueError("Each train row must be evaluated exactly once.")

    data = pd.read_csv(source_path)
    # Bảng chỉ chứa train; giữ source row index để dùng folds đã lưu.
    train = data.iloc[train_indices].copy()
    x = train[list(helpers.COLUMN_MAP)].rename(columns=helpers.COLUMN_MAP)
    y = train["Machine failure"]

    configs = {item["name"]: item for item in cv_protocol["candidates"]}
    families = [
        ("rf", "rf_all_features", RandomForestClassifier),
        ("hgb", "hgb_more_capacity", HistGradientBoostingClassifier),
    ]

    output_root = ROOT / "ml-service/artifacts/ablations"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="interactions-", dir=output_root))

    helpers.write_json(output / "protocol.json", {
        "scope": "train_only_feature_ablation",
        "factors": "raw versus raw plus three fixed interactions",
        "selection_policy": (
            "Within each family prefer higher mean fold AP; "
            "on an exact tie retain raw features. "
            "CV selection is development evidence, not independent validation."
        ),
        "source_sha256": helpers.sha256(source_path),
        "split_sha256": helpers.sha256(split_path),
        "folds_sha256": helpers.sha256(cv_dir / "folds.json"),
        "cv_protocol_sha256": helpers.sha256(cv_dir / "protocol.json"),
        "script_sha256": helpers.sha256(Path(__file__)),
        "transformer_sha256": helpers.sha256(
            SHARED / "feature_interactions.py"
        ),
        "helper_sha256": helpers.sha256(HELPER_PATH),
        "sklearn_version": sklearn.__version__,
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
        "python_version": sys.version,
        "raw_features": RAW_FEATURES,
        "derived_features": DERIVED_FEATURES,
        "candidate_configs": {
            short: configs[config_name]
            for short, config_name, _ in families
        },
        "test_previously_opened_for_v1": True,
        "test_evaluated": False,
    })

    results = []

    for short, config_name, constructor in families:
        params = configs[config_name]["params"]

        for enabled in [False, True]:
            candidate = f"{short}_{'interactions' if enabled else 'raw'}"
            oof_rows = []

            for fold in saved_folds:
                fold_number = fold["fold"]
                fit_indices = fold["fit_source_indices"]
                evaluation_indices = fold["evaluation_source_indices"]

                pipeline = make_pipeline(
                    constructor(**params), enabled=enabled
                )

                start = perf_counter()
                pipeline.fit(x.loc[fit_indices], y.loc[fit_indices])
                fit_seconds = perf_counter() - start

                # One-hot Type tạo 3 cột; numeric có 5 hoặc 8 cột.
                expected_columns = 11 if enabled else 8
                if pipeline.named_steps["model"].n_features_in_ != expected_columns:
                    raise ValueError("Derived features were dropped or misconfigured.")

                if list(pipeline.classes_) != [0, 1]:
                    raise ValueError("Unexpected class order.")

                scores = pipeline.predict_proba(
                    x.loc[evaluation_indices]
                )[:, 1]

                if (
                    not np.all(np.isfinite(scores))
                    or np.any(scores < 0)
                    or np.any(scores > 1)
                ):
                    raise ValueError("Invalid model scores.")

                labels = y.loc[evaluation_indices].to_numpy(dtype=int)
                ap = float(average_precision_score(labels, scores))

                # Raw branch phải tái hiện kết quả CV trước.
                if not enabled:
                    previous = old_fold_metrics.loc[
                        (old_fold_metrics["candidate"] == config_name)
                        & (old_fold_metrics["fold"] == fold_number),
                        "average_precision",
                    ]
                    if len(previous) != 1:
                        raise ValueError("Missing previous raw CV result.")
                    if not np.isclose(
                        ap, float(previous.iloc[0]), rtol=1e-10, atol=1e-10
                    ):
                        raise ValueError(
                            f"{candidate} fold {fold_number}: "
                            "raw baseline differs from previous CV."
                        )

                results.append({
                    "candidate": candidate,
                    "family": short,
                    "interactions": enabled,
                    "fold": fold_number,
                    "average_precision": ap,
                    "fit_seconds": fit_seconds,
                })

                oof_rows.extend([
                    {
                        "source_row_index": int(index),
                        "fold": fold_number,
                        "target": int(label),
                        "failure_score": float(score),
                    }
                    for index, label, score in zip(
                        evaluation_indices, labels, scores
                    )
                ])

                print(
                    f"{candidate}: fold {fold_number}/5, AP={ap:.5f}",
                    flush=True,
                )

            pd.DataFrame(oof_rows).to_csv(
                output / f"{candidate}_oof.csv", index=False
            )
            pd.DataFrame(results).to_csv(
                output / "fold_metrics.csv", index=False
            )

    table = pd.DataFrame(results)
    summary = table.groupby("candidate", sort=False).agg(
        mean_fold_ap=("average_precision", "mean"),
        std_fold_ap=("average_precision", "std"),
        min_fold_ap=("average_precision", "min"),
        max_fold_ap=("average_precision", "max"),
        total_fit_seconds=("fit_seconds", "sum"),
    ).reset_index()
    summary.to_csv(output / "summary.csv", index=False)

    paired = table.pivot(
        index=["family", "fold"],
        columns="interactions",
        values="average_precision",
    ).rename(columns={False: "raw_ap", True: "interaction_ap"})
    paired["delta_ap"] = paired["interaction_ap"] - paired["raw_ap"]
    paired.to_csv(output / "paired_fold_deltas.csv")

    print("\nFEATURE ABLATION SUMMARY")
    print(summary.round(5).to_string(index=False))
    print("\nPAIRED FOLD DIFFERENCES")
    print(paired.round(5).to_string())
    print("\nOutput:", output)
    print("Raw baseline reproduction: PASS")
    print("Validation evaluated: NO")
    print("Test evaluated: NO")


if __name__ == "__main__":
    main()