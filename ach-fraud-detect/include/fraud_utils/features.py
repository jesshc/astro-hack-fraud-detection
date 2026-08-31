"""Feature engineering for ACH payment fraud detection."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


# Order matters: this is what the trained model expects.
TRAINING_FEATURES = [
    "amount",
    "log_amount",
    "hour",
    "day_of_week",
    "is_night",
    "is_weekend",
    "account_age_days",
    "sec_code_encoded",
    "payment_type_encoded",
    "channel_encoded",
    "state_mismatch",
]


_SEC_CODE_MAP = {"PPD": 0, "CCD": 1, "WEB": 2, "TEL": 3, "IAT": 4}
_PAYMENT_TYPE_MAP = {
    "Payroll": 0, "Vendor payment": 1, "Bill payment": 2,
    "Direct deposit": 3, "Tax payment": 4,
}
_CHANNEL_MAP = {"Online banking": 0, "API": 1, "File upload": 2, "Branch initiated": 3}


def _encode_state(state: str) -> int:
    """Simple stable hash into a small integer space."""
    if not state:
        return 0
    return (abs(hash(state)) % 997) + 1


def build_feature_frame(records: Iterable[dict]) -> pd.DataFrame:
    """Turn an iterable of transaction dicts into a feature DataFrame."""
    df = pd.DataFrame(list(records))
    if df.empty:
        return pd.DataFrame(columns=TRAINING_FEATURES)

    ts = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["hour"] = ts.dt.hour.fillna(0).astype(int)
    df["day_of_week"] = ts.dt.dayofweek.fillna(0).astype(int)
    df["is_night"] = ((df["hour"] < 6) | (df["hour"] >= 22)).astype(int)
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    df["amount"] = df["amount"].astype(float)
    df["log_amount"] = np.log1p(df["amount"].clip(lower=0))
    df["account_age_days"] = df["account_age_days"].astype(int)
    df["sec_code_encoded"] = df["sec_code"].map(_SEC_CODE_MAP).fillna(0).astype(int)
    df["payment_type_encoded"] = df["payment_type"].map(_PAYMENT_TYPE_MAP).fillna(0).astype(int)
    df["channel_encoded"] = df["channel"].map(_CHANNEL_MAP).fillna(0).astype(int)
    df["state_mismatch"] = (
        df["originator_state"].astype(str) != df["receiver_state"].astype(str)
    ).astype(int)

    return df[TRAINING_FEATURES]
