"""Đóng gói HGB kèm transformer; kiểm tra load trong tiến trình mới."""

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path

import joblib
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.models import infer_signature


ROOT = Path(__file__).resolve().parents[3]
SHARED = ROOT / "ml-service/shared/preprocessing"

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}

# Chạy bằng python -I -B:
# -I: không lấy PYTHONPATH hoặc thư mục hiện tại làm import path.
# -B: không tạo __pycache__ trong package.
CHILD_CHECK = r'''
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import mlflow.sklearn
import mlflow.pyfunc

model_dir = Path(sys.argv[1]).resolve()
frame = pd.read_pickle(sys.argv[2])
expected = np.load(sys.argv[3], allow_pickle=False)
flavor = sys.argv[4]

if "feature_interactions" in sys.modules:
    raise RuntimeError("Transformer was already imported before model load.")

if flavor == "sklearn":
    model = mlflow.sklearn.load_model(str(model_dir))
    actual = model.predict_proba(frame)
else:
    model = mlflow.pyfunc.load_model(str(model_dir))
    actual = model.predict(frame)

import feature_interactions

module_path = Path(feature_interactions.__file__).resolve()
if not module_path.is_relative_to(model_dir / "code"):
    raise RuntimeError(
        f"Transformer was loaded outside the package: {module_path}"
    )

np.testing.assert_allclose(
    np.asarray(actual), expected, rtol=1e-10, atol=1e-10
)
print(f"{flavor}: fresh-process load / scores / packaged source PASS")
'''


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    decision_path = directory / "integration_candidate.json"
    report_path = directory / "reused_test_report.json"
    receipt_path = directory / "mlflow_hgb_tracking_receipt.json"

    decision = read_json(decision_path)
    receipt = read_json(receipt_path)
    report = read_json(report_path)

    if receipt["state"] != "VERIFIED":
        raise ValueError("HGB tracking is not verified.")
    if sha256(decision_path) != receipt["decision_sha256"]:
        raise ValueError("Decision changed.")
    if sha256(report_path) != receipt["report_sha256"]:
        raise ValueError("Evaluation report changed.")
    if decision["candidate"] != "hgb_interactions":
        raise ValueError("Expected HGB interactions.")
    if report["independent_new_holdout"] is not False:
        raise ValueError("Incorrect test independence declaration.")
    if report["quality_gate"]["passed"] is not True:
        raise ValueError("Unexpected reused-test gate.")
    if report["approved_for_release"] is not False:
        raise ValueError("Unexpected release approval.")
    if report["threshold"] != decision["threshold"]:
        raise ValueError("Threshold mismatch.")

    benchmark_path = directory / "benchmark_metadata.json"
    if sha256(benchmark_path) != decision["benchmark_metadata_sha256"]:
        raise ValueError("Benchmark metadata changed.")
    benchmark = read_json(benchmark_path)

    if version("scikit-learn") != benchmark["sklearn_version"]:
        raise ValueError("Scikit-learn version differs from training.")

    source = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    transformer = SHARED / "feature_interactions.py"
    artifact = directory / "hgb_interactions/pipeline.joblib"

    for path, expected in [
        (source, decision["source_sha256"]),
        (split_path, decision["split_sha256"]),
        (transformer, decision["transformer_sha256"]),
        (artifact, decision["pipeline_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path}")

    output = directory / "registry-model"
    manifest_path = directory / "registry-model-manifest.json"
    if output.exists() or manifest_path.exists():
        raise FileExistsError(
            "Package exists or packaging was attempted. Inspect before retrying."
        )

    # Source đã được kiểm tra hash trước khi load artifact tin cậy.
    sys.path.insert(0, str(SHARED))
    pipeline = joblib.load(artifact)

    if list(pipeline.classes_) != [0, 1]:
        raise ValueError("Unexpected classes.")
    if list(pipeline.feature_names_in_) != list(COLUMN_MAP.values()):
        raise ValueError("Feature identity mismatch.")

    split = read_json(split_path)
    frame = (
        pd.read_csv(source)
        .iloc[split["indices"]["train"]][list(COLUMN_MAP)]
        .rename(columns=COLUMN_MAP)
        .reset_index(drop=True)
    )
    for column in list(COLUMN_MAP.values())[1:]:
        frame[column] = frame[column].astype("float64")

    expected = np.asarray(pipeline.predict_proba(frame), dtype=float)
    if (
        expected.shape != (len(frame), 2)
        or not np.all(np.isfinite(expected))
        or np.any(expected < 0)
        or np.any(expected > 1)
        or not np.allclose(expected.sum(axis=1), 1.0)
    ):
        raise ValueError("Invalid reference predictions.")

    example = frame.iloc[:5].copy()
    requirements = [
        f"{package}=={version(package)}"
        for package in [
            "mlflow", "scikit-learn", "numpy", "pandas",
            "scipy", "joblib", "cloudpickle",
        ]
    ]

    mlflow.sklearn.save_model(
        sk_model=pipeline,
        path=str(output),
        serialization_format="cloudpickle",
        code_paths=[str(transformer)],
        signature=infer_signature(
            example, pipeline.predict_proba(example)
        ),
        input_example=example,
        pip_requirements=requirements,
        pyfunc_predict_fn="predict_proba",
        metadata={
            "candidate": "hgb_interactions",
            "model_version": decision["model_version"],
            "threshold": decision["threshold"],
            "output_class_order": [0, 1],
            "failure_score_column": 1,
            "source_run_id": receipt["run_id"],
            "original_pipeline_sha256": decision["pipeline_sha256"],
            "transformer_sha256": decision["transformer_sha256"],
            "decision_sha256": sha256(decision_path),
            "evaluation_scope": "previously_used_test",
            "independent_new_holdout": False,
            "reused_test_gate": "PASS",
            "approved_for_release": False,
        },
    )

    packaged_source = output / "code/feature_interactions.py"
    if sha256(packaged_source) != decision["transformer_sha256"]:
        raise ValueError("Packaged transformer differs from trained source.")

    with tempfile.TemporaryDirectory() as temporary:
        temporary = Path(temporary)
        input_path = temporary / "train_inputs.pkl"
        expected_path = temporary / "expected.npy"

        # File pickle tạm do chính script tạo, chỉ dùng cho kiểm thử nội bộ.
        frame.to_pickle(input_path)
        np.save(expected_path, expected, allow_pickle=False)

        for flavor in ["sklearn", "pyfunc"]:
            subprocess.run(
                [
                    sys.executable, "-I", "-B", "-c", CHILD_CHECK,
                    str(output), str(input_path), str(expected_path), flavor,
                ],
                cwd=temporary,
                check=True,
                timeout=180,
            )

    files = {
        path.relative_to(output).as_posix(): sha256(path)
        for path in sorted(output.rglob("*"))
        if path.is_file()
    }
    manifest = {
        "scope": "local_registry_package_not_approved_release",
        "candidate": "hgb_interactions",
        "source_run_id": receipt["run_id"],
        "decision_sha256": sha256(decision_path),
        "report_sha256": sha256(report_path),
        "original_pipeline_sha256": decision["pipeline_sha256"],
        "transformer_sha256": decision["transformer_sha256"],
        "packaging_script_sha256": sha256(Path(__file__)),
        "verification_split": "train",
        "verification_samples": len(frame),
        "sklearn_fresh_process": "PASS",
        "pyfunc_fresh_process": "PASS",
        "packaged_source_verified": True,
        "fresh_dependency_environment": False,
        "files": files,
    }

    with manifest_path.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, allow_nan=False)
        stream.write("\n")

    print("\nHGB MLflow packaging: PASS")
    print("Verification samples:", len(frame))
    print("Transformer packaged and verified: PASS")
    print("Files hashed:", len(files))
    print("Package:", output)
    print("Manifest SHA-256:", sha256(manifest_path))
    print("Registered: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()