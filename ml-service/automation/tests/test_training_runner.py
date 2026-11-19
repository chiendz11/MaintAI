import hashlib
import tempfile
import unittest
from pathlib import Path

from maintai_training.runner import COLUMN_MAP, FEATURES, load_verified_rows


class TrainingRunnerGuardTest(unittest.TestCase):
    def test_allowlist_has_only_six_physical_features_no_leakage_columns(self):
        self.assertEqual(len(FEATURES), 6)
        self.assertEqual(
            FEATURES,
            [
                "product_type",
                "air_temperature_k",
                "process_temperature_k",
                "rotational_speed_rpm",
                "torque_nm",
                "tool_wear_min",
            ],
        )
        self.assertNotIn("UDI", COLUMN_MAP)
        self.assertNotIn("Product ID", COLUMN_MAP)
        self.assertNotIn("Machine failure", COLUMN_MAP)

    def test_load_verified_rows_enforces_sha256_and_required_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "ai4i.csv"
            content = (
                "Type,Air temperature [K],Process temperature [K],Rotational speed [rpm],"
                "Torque [Nm],Tool wear [min],Machine failure\n"
                "M,298.1,308.6,1551,42.8,108,0\n"
            )
            csv_path.write_text(content, encoding="utf-8")
            sha = hashlib.sha256(content.encode("utf-8")).hexdigest()

            rows = load_verified_rows(csv_path, sha)
            self.assertEqual(len(rows), 1)
            with self.assertRaises(ValueError):
                load_verified_rows(csv_path, "0" * 64)
