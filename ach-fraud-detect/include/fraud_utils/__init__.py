"""Shared helpers for the ACH fraud detection demo."""

from .paths import DB_PATH, MODEL_PATH, ensure_dirs
from .db import (
    init_db,
    insert_transactions,
    update_decision,
    fetch_summary,
    fetch_recent,
    fetch_flagged,
    fetch_pending_flagged,
    fetch_transaction,
)
from .features import build_feature_frame, TRAINING_FEATURES
from .generator import (
    seed_training_dataset,
    generate_batch,
)

__all__ = [
    "DB_PATH",
    "MODEL_PATH",
    "ensure_dirs",
    "init_db",
    "insert_transactions",
    "update_decision",
    "fetch_summary",
    "fetch_recent",
    "fetch_flagged",
    "fetch_pending_flagged",
    "fetch_transaction",
    "build_feature_frame",
    "TRAINING_FEATURES",
    "seed_training_dataset",
    "generate_batch",
]
