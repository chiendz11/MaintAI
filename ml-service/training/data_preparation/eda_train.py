"""EDA trên train đã khóa; không phân tích validation hoặc test."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
DATA_PATH = ROOT / "sensor/data/ai4i2020.csv"
SPLIT_PATH = (
    ROOT
    / "ml-service/training/data_preparation/splits"
    / "ai4i-stratified-v1-seed42.json"
)
TARGET = "Machine failure"
NUMERIC = [
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]",
]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    # Dừng nếu split không còn khớp dữ liệu hoặc protocol.
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("create_split.py")),
            "--verify-only",
        ],
        cwd=ROOT,
        check=True,
    )

    manifest = json.loads(SPLIT_PATH.read_text(encoding="utf-8"))
    data = pd.read_csv(DATA_PATH)

    # Chỉ chọn train. IDs và fault labels không nằm trong bảng phân tích.
    indices = manifest["indices"]["train"]
    train = data.iloc[indices][["Type", *NUMERIC, TARGET]].copy()

    if len(train) != manifest["counts"]["train"]["total"]:
        raise ValueError("Số dòng train không khớp manifest")

    numeric_values = train[NUMERIC].to_numpy(dtype=float)
    if not np.isfinite(numeric_values).all():
        raise ValueError("Train có sensor không hữu hạn")

    if not train["Type"].isin(["L", "M", "H"]).all():
        raise ValueError("Train có Type không hợp lệ")

    if not train[TARGET].isin([0, 1]).all():
        raise ValueError("Train có target không hợp lệ")

    if train.isna().any().any():
        raise ValueError("Train có missing value")

    class_counts = (
        train[TARGET]
        .value_counts()
        .reindex([0, 1], fill_value=0)
        .rename_axis("target")
        .rename("count")
    )

    type_summary = (
        train.groupby("Type", observed=True)[TARGET]
        .agg(samples="count", failures="sum")
        .reindex(["L", "M", "H"])
    )
    type_summary["failure_rate_percent"] = (
        100 * type_summary["failures"] / type_summary["samples"]
    )

    numeric_summary = train[NUMERIC].describe(
        percentiles=[0.01, 0.25, 0.50, 0.75, 0.99]
    ).T

    # Tạo folder mới cho mỗi lần chạy để không ghi đè kết quả cũ.
    output_root = ROOT / "ml-service/artifacts/eda"
    output_root.mkdir(parents=True, exist_ok=True)

    import tempfile

    output = Path(tempfile.mkdtemp(prefix="train-", dir=output_root))

    class_counts.to_csv(output / "class_counts.csv")
    type_summary.to_csv(output / "type_summary.csv")
    numeric_summary.to_csv(output / "numeric_summary.csv")

    fig, axes = plt.subplots(2, 3, figsize=(14, 8))

    for ax, column in zip(axes.flat, NUMERIC):
        ax.hist(train[column], bins=30, edgecolor="white")
        ax.set_title(column)
        ax.set_ylabel("So ban ghi")

    axes.flat[-1].axis("off")
    fig.suptitle("Phan phoi sensor — train split")
    fig.tight_layout()
    fig.savefig(output / "sensor_distributions.png", dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 5))
    class_counts.plot.bar(ax=ax, rot=0)
    ax.set_xticklabels(["Normal", "Failure"])
    ax.set_ylabel("So ban ghi")
    ax.set_title("Phan bo target — train split")
    fig.tight_layout()
    fig.savefig(output / "class_distribution.png", dpi=160)
    plt.close(fig)
    # Thống kê riêng từng sensor theo nhãn, chỉ trên train.
    by_class_records = []

    for column in NUMERIC:
        for label in [0, 1]:
            values = train.loc[train[TARGET] == label, column]

            by_class_records.append({
                "feature": column,
                "target": label,
                "count": int(values.count()),
                "mean": float(values.mean()),
                "std": float(values.std()),
                "min": float(values.min()),
                "q25": float(values.quantile(0.25)),
                "median": float(values.median()),
                "q75": float(values.quantile(0.75)),
                "max": float(values.max()),
            })

    by_class_summary = pd.DataFrame(by_class_records)
    by_class_summary.to_csv(
        output / "numeric_by_class.csv",
        index=False,
    )

    fig, axes = plt.subplots(2, 3, figsize=(14, 8))

    for ax, column in zip(axes.flat, NUMERIC):
        # Hai lớp dùng cùng bin edges để so sánh đúng cùng khoảng giá trị.
        edges = np.histogram_bin_edges(
            train[column].to_numpy(dtype=float),
            bins=30,
        )

        for label, name, color in [
            (0, "Normal", "tab:blue"),
            (1, "Failure", "tab:orange"),
        ]:
            values = train.loc[train[TARGET] == label, column]

            ax.hist(
                values,
                bins=edges,
                density=True,
                histtype="step",
                linewidth=1.8,
                color=color,
                label=f"{name} (n={len(values)})",
            )

        ax.set_title(column)
        ax.set_ylabel("Mat do trong tung lop")
        ax.legend(fontsize=8)

    axes.flat[-1].axis("off")
    fig.suptitle("Sensor theo Normal/Failure — train split")
    fig.tight_layout()
    fig.savefig(output / "sensor_by_class.png", dpi=160)
    plt.close(fig)

    print("\nNUMERIC BY CLASS")
    print(by_class_summary.round(3).to_string(index=False))
    provenance = {
        "analysis_scope": "train_only",
        "source_sha256": sha256(DATA_PATH),
        "split_file_sha256": sha256(SPLIT_PATH),
        "script_sha256": sha256(Path(__file__)),
        "source_git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
        ).strip(),
        "python_version": sys.version,
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
        "matplotlib_version": matplotlib.__version__,
        "train_rows": len(train),
        "train_failures": int(class_counts.loc[1]),
        "histogram_bins": 30,
    }

    (output / "provenance.json").write_text(
        json.dumps(provenance, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print("\nCLASS COUNTS")
    print(class_counts.to_string())
    print("\nTYPE SUMMARY")
    print(type_summary.round(3).to_string())
    print("\nNUMERIC SUMMARY")
    print(numeric_summary.round(3).to_string())
    print("\nOutput:", output)
    print("Scope: train only; no rows removed; no model trained.")


if __name__ == "__main__":
    main()