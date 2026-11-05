"""Tạo đặc trưng tương tác từ đúng sáu đầu vào của MaintAI."""

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted


RAW_FEATURES = [
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",
]

DERIVED_FEATURES = [
    "temperature_difference_k",
    "mechanical_power_w",
    "wear_torque_interaction",
]


class SensorInteractions(TransformerMixin, BaseEstimator):
    """Biến đổi xác định; không học ngưỡng hoặc dùng target.

    enabled=False: giữ sáu features, dùng làm đối chứng.
    enabled=True: giữ sáu features và thêm ba tương tác.
    """

    def __init__(self, enabled=True):
        self.enabled = enabled

    @staticmethod
    def _validate_frame(frame):
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("Expected a pandas DataFrame.")

        if not frame.columns.is_unique:
            raise ValueError("Duplicate feature names.")

        if set(frame.columns) != set(RAW_FEATURES):
            raise ValueError("Expected exactly the six raw features.")

        if len(frame) == 0:
            raise ValueError("Empty feature batch.")

        if not frame["product_type"].isin(["L", "M", "H"]).all():
            raise ValueError("Invalid product_type.")

        for name in RAW_FEATURES[1:]:
            column = frame[name]
            if is_bool_dtype(column.dtype) or not is_numeric_dtype(column.dtype):
                raise ValueError(f"{name} must be numeric, not bool/string.")

        numeric = frame[RAW_FEATURES[1:]].to_numpy(dtype=float)
        if not np.all(np.isfinite(numeric)):
            raise ValueError("Numeric features must be finite.")

        if (frame["rotational_speed_rpm"] <= 0).any():
            raise ValueError("rotational_speed_rpm must be positive.")

        if (frame["tool_wear_min"] < 0).any():
            raise ValueError("tool_wear_min must be non-negative.")

    def fit(self, X, y=None):
        if type(self.enabled) is not bool:
            raise ValueError("enabled must be bool.")

        self._validate_frame(X)
        self.n_features_in_ = len(RAW_FEATURES)
        self.feature_names_in_ = np.asarray(RAW_FEATURES, dtype=object)
        return self

    def transform(self, X):
        check_is_fitted(self, "feature_names_in_")
        self._validate_frame(X)

        # Giữ thứ tự chuẩn; không thay đổi DataFrame gốc.
        result = X.loc[:, RAW_FEATURES].copy()
        if not self.enabled:
            return result

        # Dùng float để tránh tràn phép nhân số nguyên.
        air = result["air_temperature_k"].to_numpy(dtype=float)
        process = result["process_temperature_k"].to_numpy(dtype=float)
        rpm = result["rotational_speed_rpm"].to_numpy(dtype=float)
        torque = result["torque_nm"].to_numpy(dtype=float)
        wear = result["tool_wear_min"].to_numpy(dtype=float)

        with np.errstate(over="ignore", invalid="ignore"):
            result["temperature_difference_k"] = process - air

            # omega = rpm * 2*pi/60 (rad/s); P = torque * omega.
            result["mechanical_power_w"] = torque * rpm * (2 * np.pi / 60)

            # Tương tác thống kê, không phải đại lượng độ hỏng vật lý.
            result["wear_torque_interaction"] = wear * torque

        derived = result[DERIVED_FEATURES].to_numpy(dtype=float)
        if not np.all(np.isfinite(derived)):
            raise ValueError("Derived features overflowed or became non-finite.")

        return result

    def get_feature_names_out(self, input_features=None):
        check_is_fitted(self, "feature_names_in_")

        if input_features is not None:
            names = list(input_features)
            if len(names) != len(RAW_FEATURES) or set(names) != set(RAW_FEATURES):
                raise ValueError("Input feature names mismatch.")

        output = RAW_FEATURES + (DERIVED_FEATURES if self.enabled else [])
        return np.asarray(output, dtype=object)