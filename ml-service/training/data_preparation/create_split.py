"""Tạo split AI4I tái lập; không huấn luyện hoặc lựa chọn model."""

import argparse
import csv
import hashlib
import json
import random
import subprocess
import sys
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "sensor/data/ai4i2020.csv"
EXPECTED_SHA256 = (
    "dc6630cd9b1f0f853922fad78a1b6436570d3f1ec863f1dd5c4340ac56bc8a8e"
)
FEATURE_COLUMNS = [
    "Type",
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]",
]


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def build_split(rows, seed):
    """Chia theo lớp; indices là vị trí dòng dữ liệu, bắt đầu từ 0."""
    by_label = {0: [], 1: []}

    for index, row in enumerate(rows):
        label = row["Machine failure"]
        if label not in {"0", "1"}:
            raise ValueError(f"Target không hợp lệ tại index {index}")
        by_label[int(label)].append(index)

    splits = {"train": [], "validation": [], "test": []}

    for label, indices in by_label.items():
        # RNG riêng cho từng lớp, không phụ thuộc trạng thái random toàn cục.
        shuffled = indices.copy()
        random.Random(seed + label).shuffle(shuffled)

        n_test = round(len(shuffled) * 0.20)
        n_validation = round(len(shuffled) * 0.20)

        splits["test"].extend(shuffled[:n_test])
        splits["validation"].extend(
            shuffled[n_test:n_test + n_validation]
        )
        splits["train"].extend(shuffled[n_test + n_validation:])

    # Sắp xếp để file indices có biểu diễn ổn định.
    return {name: sorted(indices) for name, indices in splits.items()}


def validate_split(rows, splits):
    """Kiểm tra đủ dữ liệu, không giao nhau và không trùng feature xuyên tập."""
    memberships = [set(indices) for indices in splits.values()]
    expected = set(range(len(rows)))

    if any(len(indices) != len(set(indices)) for indices in splits.values()):
        raise ValueError("Một tập chứa index trùng")

    if set.union(*memberships) != expected:
        raise ValueError("Split thiếu bản ghi hoặc chứa index không hợp lệ")

    if sum(map(len, memberships)) != len(expected):
        raise ValueError("Có bản ghi xuất hiện trong nhiều tập")

    feature_owner = {}
    for name, indices in splits.items():
        if not indices:
            raise ValueError(f"Tập {name} rỗng")

        labels = {rows[index]["Machine failure"] for index in indices}
        if labels != {"0", "1"}:
            raise ValueError(f"Tập {name} không có đủ hai lớp")

        for index in indices:
            row = rows[index]
            key = (
                row["Type"],
                *(float(row[column]) for column in FEATURE_COLUMNS[1:]),
            )
            previous = feature_owner.setdefault(key, name)
            if previous != name:
                raise ValueError("Feature giống nhau xuất hiện xuyên tập")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Xác minh split đã lưu, không ghi hoặc cập nhật manifest.",
    )
    args = parser.parse_args()

    source_bytes = SOURCE.read_bytes()
    if sha256(source_bytes) != EXPECTED_SHA256:
        raise ValueError("Dataset checksum không khớp bản đã pin")

    with SOURCE.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))

    if len(rows) != 10_000:
        raise ValueError("Số dòng không khớp dataset đã kiểm tra")

    splits = build_split(rows, args.seed)
    validate_split(rows, splits)

    if splits != build_split(rows, args.seed):
        raise ValueError("Split không tái lập trong cùng lần chạy")

    counts = {}
    for name, indices in splits.items():
        labels = Counter(rows[index]["Machine failure"] for index in indices)
        counts[name] = {
            "total": len(indices),
            "normal": labels["0"],
            "failure": labels["1"],
        }
    if args.verify_only:
        output = (
            ROOT
            / "ml-service/training/data_preparation/splits"
            / f"ai4i-stratified-v1-seed{args.seed}.json"
        )

        saved_bytes = output.read_bytes()
        saved = json.loads(saved_bytes)

        # Kiểm tra các thông tin quyết định danh tính và cách chia dữ liệu.
        expected_fields = {
            "split_protocol_version": "ai4i-stratified-60-20-20-v1",
            "source_sha256": EXPECTED_SHA256,
            "row_count": len(rows),
            "index_semantics": "zero_based_data_row_excluding_header",
            "seed": args.seed,
            "method": "per_class_python_random_shuffle",
            "requested_ratios": {
                "train": 0.60,
                "validation": 0.20,
                "test": 0.20,
            },
            "rounding": (
                "round_each_class_for_test_and_validation_train_remainder"
            ),
            "feature_columns": FEATURE_COLUMNS,
            "target_column": "Machine failure",
            "counts": counts,
            "indices": splits,
        }

        for field, expected in expected_fields.items():
            if saved.get(field) != expected:
                raise ValueError(f"Split đã lưu không khớp: {field}")

        schema_path = ROOT / "contracts/v1.3/schemas/features.schema.json"
        if saved.get("feature_schema_sha256") != sha256(schema_path.read_bytes()):
            raise ValueError("Feature schema hiện tại khác schema lúc tạo split")

        validate_split(rows, saved["indices"])

        print("Result: VERIFIED")
        print("Output:", output.relative_to(ROOT))
        print("Split file SHA-256:", sha256(saved_bytes))
        print("Manifest preserved; no file written.")
        return
    feature_schema = ROOT / "contracts/v1.3/schemas/features.schema.json"
    source_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
    ).strip()

    payload = {
        "split_protocol_version": "ai4i-stratified-60-20-20-v1",
        "status": "development_protocol",
        "source_path": SOURCE.relative_to(ROOT).as_posix(),
        "source_sha256": EXPECTED_SHA256,
        "row_count": len(rows),
        "index_semantics": "zero_based_data_row_excluding_header",
        "seed": args.seed,
        "method": "per_class_python_random_shuffle",
        "requested_ratios": {
            "train": 0.60,
            "validation": 0.20,
            "test": 0.20,
        },
        "rounding": "round_each_class_for_test_and_validation_train_remainder",
        "feature_columns": FEATURE_COLUMNS,
        "target_column": "Machine failure",
        "feature_schema_sha256": sha256(feature_schema.read_bytes()),
        "generator_sha256": sha256(Path(__file__).read_bytes()),
        "source_git_commit": source_commit,
        "python_version": sys.version,
        "counts": counts,
        "indices": splits,
    }

    output = (
        ROOT
        / "ml-service/training/data_preparation/splits"
        / f"ai4i-stratified-v1-seed{args.seed}.json"
    )
    encoded = (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")

    if output.exists():
        if output.read_bytes() != encoded:
            raise FileExistsError(
                "Split đã tồn tại nhưng nội dung khác. "
                "Không ghi đè; cần review và tạo phiên bản mới nếu thay đổi."
            )
        result = "UNCHANGED"
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as stream:
            stream.write(encoded)
        result = "CREATED"

    print("Result:", result)
    print("Output:", output.relative_to(ROOT))
    print("Split file SHA-256:", sha256(encoded))
    for name, summary in counts.items():
        print(name, summary)
    print("Checks: coverage, disjointness, feature overlap, repeatability PASS")


if __name__ == "__main__":
    main()