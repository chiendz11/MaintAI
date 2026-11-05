"""Vòng v2: sàng lọc RF/Boosting bằng CV trên train, không đánh giá test."""

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
from sklearn.base import clone
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.metrics import average_precision_score
from sklearn.model_selection import StratifiedKFold


ROOT = Path(__file__).resolve().parents[3]

# Tái sử dụng mapping và pipeline đã dùng trong benchmark v1.
HELPER_PATH = Path(__file__).with_name("benchmark_candidates.py")
SPEC = importlib.util.spec_from_file_location("benchmark_helpers", HELPER_PATH)
helpers = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helpers)


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
    original_indices = np.asarray(manifest["indices"]["train"])
    train = pd.read_csv(source_path).iloc[original_indices]

    x = train[list(helpers.COLUMN_MAP)].rename(
        columns=helpers.COLUMN_MAP
    ).reset_index(drop=True)
    y = train["Machine failure"].to_numpy(dtype=int)

    # RF: lần lượt kiểm tra cấu hình cũ, bỏ weighting,
    # và cho mỗi split xem toàn bộ features.
    rf_base = {
        "n_estimators": 300,
        "min_samples_leaf": 2,
        "random_state": 42,
        "n_jobs": 1,
    }

    # Boosting: kiểm tra cấu hình cũ, cây đơn giản hơn,
    # và cây có khả năng học tương tác phức tạp hơn.
    hgb_base = {
        "early_stopping": False,
        "random_state": 42,
        "l2_regularization": 1.0,
    }

    candidates = [
        (
            "rf_v1",
            "random_forest",
            RandomForestClassifier(
                **rf_base,
                class_weight="balanced_subsample",
            ),
        ),
        (
            "rf_unweighted",
            "random_forest",
            RandomForestClassifier(
                **rf_base,
                class_weight=None,
            ),
        ),
        (
            "rf_all_features",
            "random_forest",
            RandomForestClassifier(
                **rf_base,
                class_weight="balanced_subsample",
                max_features=1.0,
            ),
        ),
        (
            "hgb_v1",
            "boosting",
            HistGradientBoostingClassifier(
                **hgb_base,
                max_iter=200,
                learning_rate=0.1,
                max_leaf_nodes=15,
                min_samples_leaf=20,
            ),
        ),
        (
            "hgb_small_leaves",
            "boosting",
            HistGradientBoostingClassifier(
                **hgb_base,
                max_iter=300,
                learning_rate=0.05,
                max_leaf_nodes=7,
                min_samples_leaf=20,
            ),
        ),
        (
            "hgb_more_capacity",
            "boosting",
            HistGradientBoostingClassifier(
                **hgb_base,
                max_iter=300,
                learning_rate=0.05,
                max_leaf_nodes=31,
                min_samples_leaf=10,
            ),
        ),
    ]

    splitter = StratifiedKFold(
        n_splits=5, shuffle=True, random_state=42
    )
    folds = list(splitter.split(x, y))

    output_root = ROOT / "ml-service/artifacts/cross_validation"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="cv-v2-", dir=output_root))

    # Lưu kế hoạch và cấu hình trước khi fit.
    helpers.write_json(output / "protocol.json", {
        "version": "v2-train-cv-screening",
        "scope": "original_train_split_only",
        "folds": 5,
        "seed": 42,
        "selection": (
            "Within each family: highest mean fold AP, "
            "then lowest AP std, then declaration order."
        ),
        "next_stage": "Refit family winners on train; threshold on validation.",
        "test_previously_opened_for_v1": True,
        "test_evaluated_in_this_script": False,
        "candidates": [
            {
                "name": name,
                "family": family,
                "params": model.get_params(),
            }
            for name, family, model in candidates
        ],
        "source_sha256": helpers.sha256(source_path),
        "split_sha256": helpers.sha256(split_path),
        "script_sha256": helpers.sha256(Path(__file__)),
        "helper_sha256": helpers.sha256(HELPER_PATH),
        "sklearn_version": sklearn.__version__,
        "python_version": sys.version,
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
    })

    helpers.write_json(output / "folds.json", [
        {
            "fold": number,
            "fit_source_indices": original_indices[fit].tolist(),
            "evaluation_source_indices": original_indices[evaluation].tolist(),
        }
        for number, (fit, evaluation) in enumerate(folds, start=1)
    ])

    fold_results = []
    summary = []

    for candidate_order, (name, family, model) in enumerate(candidates):
        oof = np.full(len(y), np.nan)
        seen = np.zeros(len(y), dtype=int)
        candidate_scores = []
        total_fit_seconds = 0.0

        for fold_number, (fit_indices, eval_indices) in enumerate(
            folds, start=1
        ):
            if np.intersect1d(fit_indices, eval_indices).size:
                raise ValueError("Fold overlap detected.")

            pipeline = helpers.make_pipeline(clone(model), scale=False)

            start = perf_counter()
            pipeline.fit(x.iloc[fit_indices], y[fit_indices])
            fit_seconds = perf_counter() - start
            total_fit_seconds += fit_seconds

            if list(pipeline.classes_) != [0, 1]:
                raise ValueError("Unexpected model classes.")

            scores = pipeline.predict_proba(x.iloc[eval_indices])[:, 1]
            if (
                not np.all(np.isfinite(scores))
                or np.any(scores < 0)
                or np.any(scores > 1)
            ):
                raise ValueError("Invalid scores.")

            oof[eval_indices] = scores
            seen[eval_indices] += 1
            ap = float(average_precision_score(y[eval_indices], scores))
            candidate_scores.append(ap)

            fold_results.append({
                "candidate": name,
                "family": family,
                "fold": fold_number,
                "evaluation_rows": len(eval_indices),
                "evaluation_failures": int(y[eval_indices].sum()),
                "average_precision": ap,
                "fit_seconds": fit_seconds,
            })

            print(
                f"{name}: fold {fold_number}/5, AP={ap:.5f}",
                flush=True,
            )

        if not np.all(seen == 1) or not np.all(np.isfinite(oof)):
            raise ValueError("Each train row must have exactly one OOF score.")

        pd.DataFrame({
            "source_row_index": original_indices,
            "target": y,
            "oof_failure_score": oof,
        }).to_csv(output / f"{name}_oof.csv", index=False)

        summary.append({
            "candidate": name,
            "family": family,
            "candidate_order": candidate_order,
            "mean_fold_ap": float(np.mean(candidate_scores)),
            "std_fold_ap": float(np.std(candidate_scores, ddof=1)),
            "min_fold_ap": float(np.min(candidate_scores)),
            "max_fold_ap": float(np.max(candidate_scores)),
            "pooled_oof_ap": float(average_precision_score(y, oof)),
            "total_fit_seconds": total_fit_seconds,
        })

        # Lưu tiến độ sau mỗi candidate.
        pd.DataFrame(fold_results).to_csv(
            output / "fold_metrics.csv", index=False
        )
        pd.DataFrame(summary).to_csv(
            output / "cv_summary.csv", index=False
        )

    table = pd.DataFrame(summary)
    ranked = table.sort_values(
        ["family", "mean_fold_ap", "std_fold_ap", "candidate_order"],
        ascending=[True, False, True, True],
    )
    winners = ranked.groupby("family", sort=False).head(1)

    helpers.write_json(output / "screening_winners.json", {
        "status": "screening_only_not_release_selection",
        "winners": winners["candidate"].tolist(),
    })

    print("\nTRAIN-ONLY CV SUMMARY")
    print(table.round(5).to_string(index=False))
    print("\nFamily screening winners:", winners["candidate"].tolist())
    print("Output:", output)
    print("Validation evaluated: NO")
    print("Test evaluated: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()