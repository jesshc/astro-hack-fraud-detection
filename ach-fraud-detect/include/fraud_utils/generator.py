"""Synthetic IBM-style credit-card transaction generator.

We can't fetch the real IBM "Credit Card Transactions" dataset in an
air-gapped demo, so we simulate one with the same schema:
    User, Card, Year, Month, Day, Time, Amount, Use Chip,
    Merchant Name, Merchant City, Merchant State, Zip, MCC, Is Fraud?

The generator is deterministic per-seed so runs are reproducible.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


MERCHANTS = [
    ("Amazon", "Seattle", "WA", 5942),
    ("Walmart", "Bentonville", "AR", 5411),
    ("Target", "Minneapolis", "MN", 5411),
    ("Starbucks", "Seattle", "WA", 5814),
    ("Shell", "Houston", "TX", 5541),
    ("Uber", "San Francisco", "CA", 4121),
    ("Netflix", "Los Gatos", "CA", 4899),
    ("Apple Store", "Cupertino", "CA", 5732),
    ("Home Depot", "Atlanta", "GA", 5200),
    ("Best Buy", "Richfield", "MN", 5732),
    ("McDonald's", "Chicago", "IL", 5814),
    ("Delta Airlines", "Atlanta", "GA", 4511),
    ("CVS Pharmacy", "Woonsocket", "RI", 5912),
    ("Costco", "Issaquah", "WA", 5411),
    ("Whole Foods", "Austin", "TX", 5411),
]

# "Suspicious" merchants used to bias generated fraud toward believable stories.
FRAUD_MERCHANTS = [
    ("CryptoExchange OU", "Tallinn", "FOREIGN", 6051),
    ("QuickCash ATM", "Unknown", "NV", 6011),
    ("LuxuryGoods LLC", "Miami", "FL", 5944),
    ("OnlineElectronics", "Shenzhen", "FOREIGN", 5732),
    ("AnonymousGiftCards", "Panama City", "FOREIGN", 5947),
]

USE_CHIP_OPTIONS = ["Chip Transaction", "Swipe Transaction", "Online Transaction"]


@dataclass
class TransactionRow:
    id: str
    ts: str
    user_id: int
    card_id: int
    amount: float
    merchant: str
    merchant_city: str
    merchant_state: str
    mcc: int
    use_chip: str
    is_fraud: int  # ground-truth label used only for training


def _pick_merchant(rng: random.Random, fraud: bool) -> tuple[str, str, str, int]:
    pool = FRAUD_MERCHANTS if fraud else MERCHANTS
    return rng.choice(pool)


def _sample_amount(rng: random.Random, fraud: bool) -> float:
    """Legit transactions cluster around small amounts; fraud skews large."""
    if fraud:
        # heavy-tailed
        return round(max(1.0, rng.lognormvariate(6.5, 1.2)), 2)
    return round(max(1.0, rng.lognormvariate(3.2, 0.9)), 2)


def _sample_transaction(
    rng: random.Random,
    ts: datetime,
    force_fraud: bool | None = None,
) -> TransactionRow:
    is_fraud = force_fraud if force_fraud is not None else (rng.random() < 0.03)
    merchant, city, state, mcc = _pick_merchant(rng, fraud=is_fraud)
    amount = _sample_amount(rng, fraud=is_fraud)
    use_chip = rng.choices(
        USE_CHIP_OPTIONS,
        weights=[0.55, 0.25, 0.20] if not is_fraud else [0.1, 0.15, 0.75],
        k=1,
    )[0]
    user_id = rng.randint(1, 250)
    card_id = rng.randint(0, 4)
    return TransactionRow(
        id=str(uuid.uuid4()),
        ts=ts.replace(tzinfo=timezone.utc).isoformat(),
        user_id=user_id,
        card_id=card_id,
        amount=amount,
        merchant=merchant,
        merchant_city=city,
        merchant_state=state,
        mcc=mcc,
        use_chip=use_chip,
        is_fraud=int(is_fraud),
    )


def seed_training_dataset(n_rows: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Create a small IBM-style training dataset with a Is Fraud? label."""
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    rows: list[TransactionRow] = []
    for _i in range(n_rows):
        # spread over the previous 60 days
        offset = timedelta(minutes=rng.randint(0, 60 * 24 * 60))
        ts = now - offset
        rows.append(_sample_transaction(rng, ts))
    df = pd.DataFrame([r.__dict__ for r in rows])
    return df


def generate_batch(batch_size: int = 15, fraud_rate: float = 0.15) -> list[dict]:
    """Generate a fresh batch of 'live' incoming transactions.

    Uses time.time() as entropy so each stream tick produces new rows.
    A slightly elevated fraud_rate makes the demo interesting.
    """
    import time

    rng = random.Random()
    rng.seed(int(time.time() * 1000) ^ int(np.random.randint(0, 1 << 30)))
    now = datetime.now(timezone.utc)
    rows: list[TransactionRow] = []
    for _i in range(batch_size):
        # slight jitter so transactions have unique timestamps
        ts = now - timedelta(seconds=rng.randint(0, 90))
        force = True if rng.random() < fraud_rate else None
        rows.append(_sample_transaction(rng, ts, force_fraud=force))
    # Return without the ground-truth label - we don't have that in prod.
    return [{k: v for k, v in r.__dict__.items() if k != "is_fraud"} for r in rows]
