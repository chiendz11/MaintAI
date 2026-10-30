"""Xuất báo cáo validation đã lưu; không train hoặc đánh giá lại model."""

import argparse
import hashlib
import json
import math
from pathlib import Path


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    args = parser.parse_args()

    model_dir = args.model_dir.resolve()
    metadata_path = model_dir / "metadata.json"
    pipeline_path = model_dir / "pipeline.joblib"

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    # Xác nhận báo cáo thuộc đúng file model đang bàn giao.
    if sha256(pipeline_path) != metadata["pipeline_sha256"]:
        raise ValueError("Pipeline checksum mismatch")

    if metadata["evaluation_split"] != "validation":
        raise ValueError("Expected validation evaluation")

    metrics = metadata["metrics"]
    counts = {}

    for name in ["tn", "fp", "fn", "tp"]:
        value = metrics[name]
        if type(value) is not int or value < 0:
            raise ValueError(f"Invalid confusion-matrix count: {name}")
        counts[name] = value

    sample_count = metadata["validation_rows"]
    if type(sample_count) is not int or sample_count <= 0:
        raise ValueError("Invalid validation sample count")

    if sum(counts.values()) != sample_count:
        raise ValueError("Confusion matrix does not match validation_rows")

    tp, fp = counts["tp"], counts["fp"]
    fn = counts["fn"]

    if tp + fn == 0:
        raise ValueError("Validation contains no failure samples")

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn)

    # Đối chiếu các metric có thể tính lại từ confusion matrix.
    expected = {
        "precision_failure": precision,
        "recall_failure": recall,
        "f1_failure": (
            2 * tp / (2 * tp + fp + fn)
            if 2 * tp + fp + fn else 0.0
        ),
        "false_negative_rate": fn / (fn + tp),
        "false_positives_per_1000_records": 1000 * fp / sample_count,
    }

    for name, value in expected.items():
        recorded = metrics[name]
        if (
            type(recorded) not in (int, float)
            or not math.isfinite(recorded)
            or not math.isclose(recorded, value, rel_tol=1e-9, abs_tol=1e-12)
        ):
            raise ValueError(f"Recorded metric is inconsistent: {name}")

    ap = metrics["pr_auc_average_precision"]
    if (
        type(ap) not in (int, float)
        or not math.isfinite(ap)
        or not 0 <= ap <= 1
    ):
        raise ValueError("Invalid average precision")

    threshold = metadata["threshold"]
    if (
        type(threshold) not in (int, float)
        or not math.isfinite(threshold)
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid threshold")

    # Mục tiêu đã chốt theo yêu cầu thầy.
    checks = {
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
    }
    passed = all(item["passed"] for item in checks.values())

    report = {
        "report_kind": "local_baseline_validation_evaluation",
        "model_version": metadata["model_version"],
        "preprocessor_version": metadata["preprocessor_version"],
        "evaluation_split": "validation",
        "sample_count": sample_count,
        "failure_sample_count": tp + fn,
        "threshold": threshold,
        "threshold_status": metadata["threshold_status"],
        "metrics": metrics,
        "quality_gate": {
            "status": "PASS" if passed else "FAIL",
            "checks": checks,
        },
        # Gate metric đạt cũng chưa đồng nghĩa được duyệt release.
        "approved_for_release": False,
        "intended_use": "baseline_pipeline_integration",
        "provenance": {
            "source_sha256": metadata["source_sha256"],
            "split_sha256": metadata["split_sha256"],
            "pipeline_sha256": metadata["pipeline_sha256"],
            "metadata_sha256": sha256(metadata_path),
        },
        "limitations": [
            "Exported from recorded validation metrics; not recomputed inference.",
            "Average precision copied from metadata, not recomputed from scores.",
            "No test-set evaluation performed by this exporter.",
            "Quality-gate result is not production release approval.",
        ],
    }

    content = json.dumps(
        report, indent=2, ensure_ascii=False, allow_nan=False
    ) + "\n"
    output = model_dir / "evaluation.json"

    if output.exists():
        if output.read_text(encoding="utf-8") != content:
            raise ValueError(
                "Existing evaluation differs; inspect it before replacing"
            )
        result = "UNCHANGED"
    else:
        # Không ghi đè báo cáo đã có.
        with output.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
        result = "CREATED"

    print("Result:", result)
    print("Output:", output)
    print("Quality gate:", report["quality_gate"]["status"])
    print("Recall Failure:", recall, "| required >= 0.85")
    print("Precision Failure:", precision, "| required >= 0.50")
    print("Approved for release: False")
    print("Evaluation SHA-256:", sha256(output))


if __name__ == "__main__":
    main()