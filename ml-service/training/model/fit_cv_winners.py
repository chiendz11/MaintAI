"""Fit CV winners trên train; chọn threshold trên validation, không dùng test."""

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


ROOT = Path(__file__).resolve().parents[3]
HELPER_PATH = Path(__file__).with_name("benchmark_candidates.py")

SPEC = importlib.util.spec_from_file_location("benchmark_helpers", HELPER_PATH)
helpers = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helpers)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cv-dir", required=True, type=Path)
    args = parser.parse_args()
    cv_dir = args.cv_dir.resolve()

    protocol_path = cv_dir / "protocol.json"
    summary_path = cv_dir / "cv_summary.csv"
    winners_path = cv_dir / "screening_winners.json"

    protocol = read_json(protocol_path)
    recorded_winners = read_json(winners_path)
    summary = pd.read_csv(summary_path)

    if protocol["scope"] != "original_train_split_only":
        raise ValueError("Unexpected CV scope.")

    if sklearn.__version__ != protocol["sklearn_version"]:
        raise ValueError("Scikit-learn version changed.")

    if helpers.sha256(HELPER_PATH) != protocol["helper_sha256"]:
        raise ValueError("Benchmark helper changed since CV.")

    # Tính lại winners theo đúng quy tắc sàng lọc.
    ranked = summary.sort_values(
        ["family", "mean_fold_ap", "std_fold_ap", "candidate_order"],
        ascending=[True, False, True, True],
    )
    winner_names = (
        ranked.groupby("family", sort=False)
        .head(1)["candidate"].tolist()
    )

    if winner_names != recorded_winners["winners"]:
        raise ValueError("CV winners do not match ranking.")

    configs = {item["name"]: item for item in protocol["candidates"]}
    if set(summary["candidate"]) != set(configs):
        raise ValueError("Candidate list mismatch.")

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

    manifest = read_json(split_path)
    data = pd.read_csv(source_path)
    features = data[list(helpers.COLUMN_MAP)].rename(
        columns=helpers.COLUMN_MAP
    )
    labels = data["Machine failure"]

    train_indices = manifest["indices"]["train"]
    validation_indices = manifest["indices"]["validation"]

    x_train = features.iloc[train_indices]
    y_train = labels.iloc[train_indices].to_numpy(dtype=int)
    x_val = features.iloc[validation_indices]
    y_val = labels.iloc[validation_indices].to_numpy(dtype=int)

    output_root = ROOT / "ml-service/artifacts/benchmarks"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="cv-winners-v2-", dir=output_root))

    constructors = {
        "random_forest": RandomForestClassifier,
        "boosting": HistGradientBoostingClassifier,
    }

    rows = []

    for name in winner_names:
        config = configs[name]
        model = constructors[config["family"]](**config["params"])
        pipeline = helpers.make_pipeline(model, scale=False)

        print("Training:", name, flush=True)
        start = perf_counter()
        pipeline.fit(x_train, y_train)
        fit_seconds = perf_counter() - start

        if list(pipeline.classes_) != [0, 1]:
            raise ValueError("Unexpected classes.")

        pipeline.predict_proba(x_val.iloc[:10])
        start = perf_counter()
        scores = pipeline.predict_proba(x_val)[:, 1]
        predict_seconds = perf_counter() - start

        if (
            not np.all(np.isfinite(scores))
            or np.any(scores < 0)
            or np.any(scores > 1)
        ):
            raise ValueError("Invalid scores.")

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
            scores,
            rtol=1e-12,
            atol=1e-12,
        )

        pd.DataFrame(threshold_rows).to_csv(
            folder / "thresholds.csv", index=False
        )
        pd.DataFrame({
            "row_index": validation_indices,
            "target": y_val,
            "failure_score": scores,
        }).to_csv(folder / "validation_predictions.csv", index=False)

        helpers.write_json(folder / "experiment.json", {
            "candidate": name,
            "family": config["family"],
            "status": "candidate_not_approved_release",
            "model_params": model.get_params(),
            "features": helpers.FEATURES,
            "numeric_scaling": False,
            "categorical_order": ["L", "M", "H"],
            "average_precision": ap,
            "default_operating_point": default,
            "validation_selected_operating_point": selected,
            "pipeline_sha256": helpers.sha256(artifact),
            "save_load_check": "PASS",
        })

        for operating_point, result in [
            ("default_0.5", default),
            ("validation_selected", selected),
        ]:
            rows.append({
                "candidate": name,
                "operating_point": operating_point,
                "average_precision": ap,
                "fit_seconds": fit_seconds,
                "predict_batch_seconds": predict_seconds,
                "artifact_bytes": artifact.stat().st_size,
                **result,
            })

    table = pd.DataFrame(rows)
    table.to_csv(output / "comparison.csv", index=False)

    helpers.write_json(output / "benchmark_metadata.json", {
        "status": "v2_validation_comparison_not_approved_release",
        "evaluation_split": "validation",
        "seed": 42,
        "train_rows": len(y_train),
        "validation_rows": len(y_val),
        "source_sha256": helpers.sha256(source_path),
        "split_sha256": helpers.sha256(split_path),
        "cv_protocol_sha256": helpers.sha256(protocol_path),
        "cv_summary_sha256": helpers.sha256(summary_path),
        "cv_winners_sha256": helpers.sha256(winners_path),
        "script_sha256": helpers.sha256(Path(__file__)),
        "helper_sha256": helpers.sha256(HELPER_PATH),
        "sklearn_version": sklearn.__version__,
        "python_version": sys.version,
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
        "source_git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "working_tree_status": subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=ROOT, text=True
        ),
        "test_evaluated": False,
        "test_previously_opened_for_v1": True,
        "validation_reused_from_v1": True,
        "threshold_policy": "gate_then_f2_recall_precision_threshold",
    })

    columns = [
        "candidate", "operating_point", "threshold",
        "precision", "recall", "f2", "average_precision",
        "fp", "fn", "gate_pass",
    ]
    print("\nV2 VALIDATION COMPARISON")
    print(table[columns].round(5).to_string(index=False))
    print("\nOutput:", output)
    print("Validation reused: YES — development evidence")
    print("Test evaluated in this script: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()