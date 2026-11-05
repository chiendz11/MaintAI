"""Đóng gói RF thành MLflow Model local; chưa upload/register/release."""

import argparse
import hashlib
import json
from importlib.metadata import version
from pathlib import Path

import joblib
import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.models import infer_signature


ROOT = Path(__file__).resolve().parents[3]

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    selection_path = directory / "frozen_selection.json"
    selection = read_json(selection_path)
    receipt = read_json(directory / "mlflow_rf_tracking_receipt.json")

    if receipt["state"] != "VERIFIED":
        raise ValueError("Tracking round-trip has not been verified.")
    if sha256(selection_path) != receipt["selection_sha256"]:
        raise ValueError("Frozen selection changed.")
    if selection["candidate"] != "random_forest":
        raise ValueError("Expected RF candidate.")

    benchmark_path = directory / "benchmark_metadata.json"
    if sha256(benchmark_path) != selection["benchmark_metadata_sha256"]:
        raise ValueError("Benchmark metadata changed.")
    benchmark = read_json(benchmark_path)

    if version("scikit-learn") != benchmark["sklearn_version"]:
        raise ValueError("Scikit-learn version differs from training.")

    source = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    if sha256(source) != selection["source_sha256"]:
        raise ValueError("Dataset changed.")
    if sha256(split_path) != selection["split_sha256"]:
        raise ValueError("Split changed.")

    artifact = directory / "random_forest/pipeline.joblib"
    if sha256(artifact) != selection["pipeline_sha256"]:
        raise ValueError("Pipeline changed.")

    output = directory / "registry-model"
    manifest_path = directory / "registry-model-manifest.json"
    if output.exists() or manifest_path.exists():
        raise FileExistsError(
            "Package already exists or packaging was attempted. "
            "Inspect it before retrying."
        )

    # Chỉ load artifact tin cậy đã kiểm tra hash.
    pipeline = joblib.load(artifact)
    if list(pipeline.classes_) != [0, 1]:
        raise ValueError("Unexpected classes.")
    if list(pipeline.feature_names_in_) != list(COLUMN_MAP.values()):
        raise ValueError("Feature order mismatch.")

    split = read_json(split_path)
    data = pd.read_csv(source)

    # Dùng train để kiểm tra đóng gói; không mở lại test.
    frame = (
        data.iloc[split["indices"]["train"]][list(COLUMN_MAP)]
        .rename(columns=COLUMN_MAP)
        .reset_index(drop=True)
    )

    # Signature số dùng double, phù hợp raw numeric features.
    for column in list(COLUMN_MAP.values())[1:]:
        frame[column] = frame[column].astype("float64")

    expected = pipeline.predict_proba(frame)
    example = frame.iloc[:5].copy()
    signature = infer_signature(
        example, pipeline.predict_proba(example)
    )

    packages = [
        "mlflow",
        "scikit-learn",
        "numpy",
        "pandas",
        "scipy",
        "joblib",
        "cloudpickle",
    ]
    requirements = [
        f"{package}=={version(package)}" for package in packages
    ]

    mlflow.sklearn.save_model(
        sk_model=pipeline,
        path=str(output),
        serialization_format="cloudpickle",
        signature=signature,
        input_example=example,
        pip_requirements=requirements,
        pyfunc_predict_fn="predict_proba",
        metadata={
            "candidate": "random_forest",
            "threshold": selection["threshold"],
            "output_class_order": [0, 1],
            "failure_score_column": 1,
            "source_run_id": receipt["run_id"],
            "original_pipeline_sha256": selection["pipeline_sha256"],
            "selection_sha256": sha256(selection_path),
            "test_gate": "FAIL",
            "approved_for_release": False,
        },
    )

    # Kiểm tra cả hai cách load trên cùng 6000 mẫu train.
    restored_sklearn = mlflow.sklearn.load_model(str(output))
    restored_pyfunc = mlflow.pyfunc.load_model(str(output))

    np.testing.assert_allclose(
        restored_sklearn.predict_proba(frame),
        expected,
        rtol=1e-10,
        atol=1e-10,
    )
    np.testing.assert_allclose(
        np.asarray(restored_pyfunc.predict(frame)),
        expected,
        rtol=1e-10,
        atol=1e-10,
    )

    # Manifest nằm ngoài package, tránh tự hash chính nó.
    files = {
        path.relative_to(output).as_posix(): sha256(path)
        for path in sorted(output.rglob("*"))
        if path.is_file()
    }
    manifest = {
        "scope": "local_registry_package_not_approved_release",
        "source_run_id": receipt["run_id"],
        "selection_sha256": sha256(selection_path),
        "original_pipeline_sha256": selection["pipeline_sha256"],
        "packaging_script_sha256": sha256(Path(__file__)),
        "verification_split": "train",
        "verification_samples": len(frame),
        "sklearn_roundtrip": "PASS",
        "pyfunc_roundtrip": "PASS",
        "files": files,
    }

    with manifest_path.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, allow_nan=False)
        stream.write("\n")

    print("MLflow Model packaging: PASS")
    print("Sklearn load / predict_proba: PASS")
    print("Pyfunc load / predict: PASS")
    print("Verification samples:", len(frame))
    print("Output columns: [P(Normal), P(Failure)]")
    print("Files hashed:", len(files))
    print("Package:", output)
    print("Manifest:", manifest_path)
    print("Manifest SHA-256:", sha256(manifest_path))
    print("Registered: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()