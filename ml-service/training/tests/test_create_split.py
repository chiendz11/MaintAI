"""Kiểm thử split bằng fixture nhỏ, không cần dataset thật."""

import importlib.util
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "data_preparation"
    / "create_split.py"
)

spec = importlib.util.spec_from_file_location("maintai_create_split", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def make_rows():
    """Tạo 100 dòng với feature khác nhau, gồm 80 Normal và 20 Failure."""
    rows = []

    for index in range(100):
        rows.append({
            "Type": "M",
            "Air temperature [K]": str(290 + index / 10),
            "Process temperature [K]": str(300 + index / 10),
            "Rotational speed [rpm]": str(1000 + index),
            "Torque [Nm]": str(20 + index / 10),
            "Tool wear [min]": str(index),
            "Machine failure": "0" if index < 80 else "1",
        })

    return rows


class TestCreateSplit(unittest.TestCase):
    def setUp(self):
        self.rows = make_rows()

    def test_same_seed_same_split(self):
        """Cùng dữ liệu và seed phải tạo cùng indices."""
        first = module.build_split(self.rows, seed=42)
        second = module.build_split(self.rows, seed=42)
        self.assertEqual(first, second)

    def test_expected_counts_and_class_balance(self):
        """Fixture phải được chia đúng tổng và số mẫu từng lớp."""
        splits = module.build_split(self.rows, seed=42)

        expected = {
            "train": (60, 12),
            "validation": (20, 4),
            "test": (20, 4),
        }

        for name, (total, failures) in expected.items():
            with self.subTest(split=name):
                self.assertEqual(len(splits[name]), total)
                actual_failures = sum(
                    int(self.rows[index]["Machine failure"])
                    for index in splits[name]
                )
                self.assertEqual(actual_failures, failures)

        module.validate_split(self.rows, splits)

    def test_every_row_belongs_to_exactly_one_split(self):
        """Mỗi dòng xuất hiện đúng một lần trong toàn bộ split."""
        splits = module.build_split(self.rows, seed=42)
        indices = [
            index
            for partition in splits.values()
            for index in partition
        ]

        self.assertEqual(len(indices), len(self.rows))
        self.assertEqual(set(indices), set(range(len(self.rows))))

    def test_rejects_overlap_between_splits(self):
        splits = module.build_split(self.rows, seed=42)
        splits["validation"].append(splits["train"][0])

        with self.assertRaises(ValueError):
            module.validate_split(self.rows, splits)

    def test_rejects_missing_row(self):
        splits = module.build_split(self.rows, seed=42)
        splits["train"].pop()

        with self.assertRaises(ValueError):
            module.validate_split(self.rows, splits)

    def test_rejects_duplicate_index_inside_split(self):
        splits = module.build_split(self.rows, seed=42)
        splits["train"].append(splits["train"][0])

        with self.assertRaises(ValueError):
            module.validate_split(self.rows, splits)

    def test_rejects_out_of_range_index(self):
        splits = module.build_split(self.rows, seed=42)
        splits["train"][0] = len(self.rows)

        with self.assertRaises(ValueError):
            module.validate_split(self.rows, splits)

    def test_rejects_cross_split_feature_duplicate(self):
        """Hai dòng khác index nhưng cùng feature không được xuyên tập."""
        splits = module.build_split(self.rows, seed=42)
        source = splits["train"][0]
        destination = splits["validation"][0]

        for column in module.FEATURE_COLUMNS:
            self.rows[destination][column] = self.rows[source][column]

        with self.assertRaises(ValueError):
            module.validate_split(self.rows, splits)

    def test_rejects_partition_without_both_classes(self):
        splits = module.build_split(self.rows, seed=42)

        for index in splits["validation"]:
            self.rows[index]["Machine failure"] = "0"

        with self.assertRaises(ValueError):
            module.validate_split(self.rows, splits)

    def test_rejects_invalid_target(self):
        self.rows[0]["Machine failure"] = "unknown"

        with self.assertRaises(ValueError):
            module.build_split(self.rows, seed=42)

    def test_changing_ids_does_not_change_split(self):
        """IDs không được ảnh hưởng cách chia dựa trên target."""
        original = module.build_split(self.rows, seed=42)

        for index, row in enumerate(self.rows):
            row["UDI"] = str(99999 - index)
            row["Product ID"] = f"unrelated-{index}"

        changed = module.build_split(self.rows, seed=42)
        self.assertEqual(original, changed)


if __name__ == "__main__":
    unittest.main()