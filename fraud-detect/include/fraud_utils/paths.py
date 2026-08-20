"""Filesystem paths shared across DAGs and plugin."""

from __future__ import annotations

import os
from pathlib import Path

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
INCLUDE_DIR = AIRFLOW_HOME / "include"

DATA_DIR = INCLUDE_DIR / "data"
MODEL_DIR = INCLUDE_DIR / "models"

DB_PATH = DATA_DIR / "fraud.db"
MODEL_PATH = MODEL_DIR / "fraud_model.joblib"
TRAINING_CSV = DATA_DIR / "ibm_cc_seed.csv"


def ensure_dirs() -> None:
    """Create the data + models directories if they don't yet exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
