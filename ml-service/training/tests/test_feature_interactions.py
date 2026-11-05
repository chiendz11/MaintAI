"""Kiểm thử transformer dùng chung trước thí nghiệm đặc trưng."""

import sys
import tempfile
import unittest
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from sklearn.base import clone
from sklearn.exceptions import NotFittedError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(
    0, str(ROOT / "ml-service/shared/preprocessing")
)

from feature_interactions import (
    DERIVED_FEATURES,
    RAW_FEATURES,
    SensorInteractions,
)


def sample():
    return pd.DataFrame([{
        "product_type": "M",
        "air_temperature_k": 300.0,
        "process_temperature_k": 310.0,
        "rotational_speed_rpm": 60.0,
        "torque_nm": 10.0,
        "tool_wear_min": 5.0,
    }])


class TestFeatureInteractions(unittest.TestCase):
    def test_known_formulas(self):
        result = SensorInteractions().fit_transform(sample())
        np.testing.assert_allclose(
            result[DERIVED_FEATURES].to_numpy(dtype=float),
            [[10.0, 20 * np.pi, 50.0]],
        )

    def test_input_is_not_modified(self):
        frame = sample()
        original = frame.copy(deep=True)
        SensorInteractions().fit_transform(frame)
        assert_frame_equal(frame, original)

    def test_column_order_invariance(self):
        frame = sample()
        transformer = SensorInteractions().fit(frame)
        assert_frame_equal(
            transformer.transform(frame),
            transformer.transform(frame[list(reversed(RAW_FEATURES))]),
        )

    def test_disabled_mode_preserves_raw_features(self):
        frame = sample()
        result = SensorInteractions(enabled=False).fit_transform(frame)
        assert_frame_equal(result, frame[RAW_FEATURES])

    def test_transform_requires_fit(self):
        with self.assertRaises(NotFittedError):
            SensorInteractions().transform(sample())

    def test_rejects_missing_extra_and_duplicate_columns(self):
        frame = sample()
        bad_frames = [
            frame.drop(columns=["torque_nm"]),
            frame.assign(event_id="evt_001"),
            pd.concat([frame, frame[["torque_nm"]]], axis=1),
        ]
        for bad in bad_frames:
            with self.subTest(columns=list(bad.columns)):
                with self.assertRaises(ValueError):
                    SensorInteractions().fit(bad)

    def test_rejects_invalid_numeric_inputs(self):
        for value in [np.nan, np.inf, -np.inf, "10", True]:
            with self.subTest(value=value):
                bad = sample()
                bad["torque_nm"] = [value]
                with self.assertRaises(ValueError):
                    SensorInteractions().fit(bad)

    def test_rejects_invalid_category_and_ranges(self):
        cases = [
            ("product_type", "UNKNOWN"),
            ("product_type", None),
            ("rotational_speed_rpm", 0.0),
            ("rotational_speed_rpm", -1.0),
            ("tool_wear_min", -1.0),
        ]
        for column, value in cases:
            with self.subTest(column=column, value=value):
                bad = sample()
                bad[column] = [value]
                with self.assertRaises(ValueError):
                    SensorInteractions().fit(bad)

    def test_revalidates_input_during_transform(self):
        transformer = SensorInteractions().fit(sample())
        bad = sample()
        bad["rotational_speed_rpm"] = [-1.0]
        with self.assertRaises(ValueError):
            transformer.transform(bad)

    def test_zero_tool_wear_is_valid(self):
        frame = sample()
        frame["tool_wear_min"] = [0.0]
        result = SensorInteractions().fit_transform(frame)
        self.assertEqual(result["wear_torque_interaction"].iloc[0], 0.0)

    def test_rejects_derived_overflow(self):
        frame = sample()
        frame["torque_nm"] = [1e308]
        frame["rotational_speed_rpm"] = [1e308]
        with self.assertRaises(ValueError):
            SensorInteractions().fit_transform(frame)

    def test_batch_matches_individual_transforms(self):
        frame = pd.concat([sample(), sample()], ignore_index=True)
        frame.loc[1, "torque_nm"] = 20.0
        transformer = SensorInteractions().fit(frame)

        batch = transformer.transform(frame)
        separate = pd.concat([
            transformer.transform(frame.iloc[[0]]),
            transformer.transform(frame.iloc[[1]]),
        ])
        assert_frame_equal(batch, separate)

    def test_clone_preserves_configuration_but_not_fitted_state(self):
        fitted = SensorInteractions(enabled=False).fit(sample())
        copied = clone(fitted)

        self.assertFalse(copied.enabled)
        self.assertFalse(hasattr(copied, "feature_names_in_"))

    def test_save_load_preserves_transform(self):
        transformer = SensorInteractions().fit(sample())

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transformer.joblib"
            joblib.dump(transformer, path)
            restored = joblib.load(path)

            assert_frame_equal(
                transformer.transform(sample()),
                restored.transform(sample()),
            )

    def test_feature_names_match_output(self):
        for enabled in [False, True]:
            with self.subTest(enabled=enabled):
                transformer = SensorInteractions(enabled=enabled).fit(sample())
                result = transformer.transform(sample())
                self.assertEqual(
                    transformer.get_feature_names_out().tolist(),
                    result.columns.tolist(),
                )


if __name__ == "__main__":
    unittest.main()