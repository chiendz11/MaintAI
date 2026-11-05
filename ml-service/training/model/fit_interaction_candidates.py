"""Fit candidates có tương tác; đánh giá validation, không dùng test."""

import argparse
import importlib.util
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
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.metrics import average_precision_score

import os
import re

ROOT = Path(__file__).resolve().parents[3]
ABLATION_SCRIPT = Path(__file__).with_name("ablate_interactions.py")

SPEC = importlib.util.spec_from_file_location(
    "interaction_ablation", ABLATION_SCRIPT
)
ablation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ablation)

helpers = ablation.helpers


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ablation-dir", required=True, type=Path)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=ROOT / "ml-service/artifacts/benchmarks",
        help="Thư mục cha chứa output riêng cho mỗi lần chạy.",
    )
    args = parser.parse_args()
    ablation_dir = args.ablation_dir.resolve()

    # Image nhận revision tại build time; chạy local lấy từ Git.
    image_revision = os.environ.get("MAINTAI_SOURCE_REVISION")
    if image_revision is not None:
        if re.fullmatch(r"[0-9a-f]{40}", image_revision) is None:
            raise ValueError("Invalid MAINTAI_SOURCE_REVISION")
        source_commit = image_revision
        source_commit_origin = "image_build_revision"
        working_tree_status = None
    else:
        source_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        source_commit_origin = "local_git"
        working_tree_status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=ROOT, text=True
        )

    # Revision là thông tin provenance, không phải chữ ký phê duyệt.
    # Hash các script/input vẫn được ghi riêng trong metadata.
    protocol_path = ablation_dir / "protocol.json"
    protocol = read_json(protocol_path)

    if protocol["scope"] != "train_only_feature_ablation":
        raise ValueError("Unexpected ablation scope.")
    if sklearn.__version__ != protocol["sklearn_version"]:
        raise ValueError("Scikit-learn version changed.")

    transformer_path = (
        ROOT / "ml-service/shared/preprocessing/feature_interactions.py"
    )
    helper_path = Path(__file__).with_name("benchmark_candidates.py")

    for path, expected in [
        (ABLATION_SCRIPT, protocol["script_sha256"]),
        (transformer_path, protocol["transformer_sha256"]),
        (helper_path, protocol["helper_sha256"]),
    ]:
        if helpers.sha256(path) != expected:
            raise ValueError(f"Source changed since ablation: {path}")

    source_path = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )

    if helpers.sha256(source_path) != protocol["source_sha256"]:
        raise ValueError("Dataset changed.")
    if helpers.sha256(split_path) != protocol["split_sha256"]:
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

    # Xác nhận nhánh interactions thắng raw theo AP trung bình của từng họ.
    summary = pd.read_csv(ablation_dir / "summary.csv").set_index("candidate")
    for family in ["rf", "hgb"]:
        if (
            summary.loc[f"{family}_interactions", "mean_fold_ap"]
            <= summary.loc[f"{family}_raw", "mean_fold_ap"]
        ):
            raise ValueError(f"{family}: interactions did not win screening.")

    manifest = read_json(split_path)
    data = pd.read_csv(source_path)
    features = data[list(helpers.COLUMN_MAP)].rename(columns=helpers.COLUMN_MAP)
    labels = data["Machine failure"]

    train_indices = manifest["indices"]["train"]
    val_indices = manifest["indices"]["validation"]

    x_train = features.iloc[train_indices]
    y_train = labels.iloc[train_indices].to_numpy(dtype=int)
    x_val = features.iloc[val_indices]
    y_val = labels.iloc[val_indices].to_numpy(dtype=int)

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(
        prefix="interactions-validation-", dir=output_root
    ))

    candidates = [
        ("rf", RandomForestClassifier),
        ("hgb", HistGradientBoostingClassifier),
    ]
    results = []

    for family, constructor in candidates:
        name = f"{family}_interactions"
        config = protocol["candidate_configs"][family]
        pipeline = ablation.make_pipeline(
            constructor(**config["params"]), enabled=True
        )

        print("Training:", name, flush=True)
        start = perf_counter()
        pipeline.fit(x_train, y_train)
        fit_seconds = perf_counter() - start

        if list(pipeline.classes_) != [0, 1]:
            raise ValueError("Unexpected class order.")
        if pipeline.named_steps["model"].n_features_in_ != 11:
            raise ValueError("Expected 11 transformed features.")

        pipeline.predict_proba(x_val.iloc[:10])
        start = perf_counter()
        scores = pipeline.predict_proba(x_val)[:, 1]
        predict_seconds = perf_counter() - start

        if (
            not np.all(np.isfinite(scores))
            or np.any(scores < 0)
            or np.any(scores > 1)
        ):
            raise ValueError("Invalid probabilities.")

        threshold_rows = helpers.threshold_table(y_val, scores)
        selected = helpers.choose_operating_point(threshold_rows)
        default = helpers.metrics(y_val, scores >= 0.5)
        default["threshold"] = 0.5
        ap = float(average_precision_score(y_val, scores))

        folder = output / name
        folder.mkdir()
        artifact = folder / "pipeline.joblib"
        joblib.dump(pipeline, artifact)

        restored = joblib.load(artifact)
        np.testing.assert_allclose(
            restored.predict_proba(x_val)[:, 1],
            scores, rtol=1e-12, atol=1e-12,
        )

        pd.DataFrame(threshold_rows).to_csv(
            folder / "thresholds.csv", index=False
        )
        pd.DataFrame({
            "row_index": val_indices,
            "target": y_val,
            "failure_score": scores,
        }).to_csv(folder / "validation_predictions.csv", index=False)

        helpers.write_json(folder / "experiment.json", {
            "candidate": name,
            "status": "candidate_not_approved_release",
            "features": helpers.FEATURES,
            "derived_features": ablation.DERIVED_FEATURES,
            "interaction_module": "feature_interactions",
            "transformer_sha256": helpers.sha256(transformer_path),
            "numeric_scaling": False,
            "categorical_order": ["L", "M", "H"],
            "model_params": pipeline.named_steps["model"].get_params(),
            "default_operating_point": default,
            "validation_selected_operating_point": selected,
            "average_precision": ap,
            "pipeline_sha256": helpers.sha256(artifact),
            "save_load_check": "PASS",
        })

        for operating_point, point in [
            ("default_0.5", default),
            ("validation_selected", selected),
        ]:
            results.append({
                "candidate": name,
                "operating_point": operating_point,
                "average_precision": ap,
                "fit_seconds": fit_seconds,
                "predict_batch_seconds": predict_seconds,
                "artifact_bytes": artifact.stat().st_size,
                **point,
            })

    table = pd.DataFrame(results)
    table.to_csv(output / "comparison.csv", index=False)

    helpers.write_json(output / "benchmark_metadata.json", {
        "status": "interaction_candidates_not_approved_release",
        "evaluation_split": "validation",
        "train_rows": len(y_train),
        "validation_rows": len(y_val),
        "seed": 42,
        "source_sha256": helpers.sha256(source_path),
        "split_sha256": helpers.sha256(split_path),
        "ablation_protocol_sha256": helpers.sha256(protocol_path),
        "ablation_summary_sha256": helpers.sha256(ablation_dir / "summary.csv"),
        "script_sha256": helpers.sha256(Path(__file__)),
        "helper_sha256": helpers.sha256(helper_path),
        "transformer_sha256": helpers.sha256(transformer_path),
        "sklearn_version": sklearn.__version__,
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
        "python_version": sys.version,
        "source_git_commit": source_commit,
        "source_commit_origin": source_commit_origin,
        "working_tree_status": working_tree_status,
        "threshold_policy": "gate_then_f2_recall_precision_threshold",
        "validation_reused": True,
        "test_previously_opened_for_v1": True,
        "test_evaluated": False,
    })

    columns = [
        "candidate", "operating_point", "threshold",
        "precision", "recall", "f2", "average_precision",
        "fp", "fn", "gate_pass",
    ]
    print("\nINTERACTION VALIDATION COMPARISON")
    print(table[columns].round(5).to_string(index=False))
    print("\nOutput:", output)
    print("Validation reused: YES — development evidence")
    print("Test evaluated: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()