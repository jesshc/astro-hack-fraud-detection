"""Feature engineering for the fraud detection model.

We convert raw IBM-style credit-card transactions into a small numeric
feature matrix suitable for scikit-learn.
"""

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
    "mcc",
    "use_chip_encoded",
    "state_encoded",
]


_USE_CHIP_MAP = {
    "Chip Transaction": 0,
    "Swipe Transaction": 1,
    "Online Transaction": 2,
}


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
    df["mcc"] = df["mcc"].astype(int)
    df["use_chip_encoded"] = df["use_chip"].map(_USE_CHIP_MAP).fillna(0).astype(int)
    df["state_encoded"] = (
        df["merchant_state"].astype(str).map(_encode_state).astype(int)
    )

    return df[TRAINING_FEATURES]
