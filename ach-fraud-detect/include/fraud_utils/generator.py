"""Synthetic ACH payment generator for the fraud detection demo.

The generator creates labeled ACH payment records with realistic payment
attributes such as SEC code, payment type, account age, channel, and
originator/receiver geography. The generator is deterministic per seed so
training runs are reproducible.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


STATES = ["CA", "CO", "FL", "GA", "IL", "MA", "NC", "NY", "OH", "TX", "VA", "WA"]
PAYMENT_TYPES = ["Payroll", "Vendor payment", "Bill payment", "Direct deposit", "Tax payment"]
# Standard Entry Class codes identify the type and authorization method of an ACH transaction.
SEC_CODES = ["PPD", "CCD", "WEB", "TEL", "IAT"]
CHANNELS = ["Online banking", "API", "File upload", "Branch initiated"]


@dataclass
class TransactionRow:
    id: str  # Unique identifier for this transaction.
    ts: str  # UTC timestamp when the payment was generated.
    originator_id: str  # Synthetic ID of the sender.
    receiver_id: str  # Synthetic ID of the recipient.
    amount: float  # Payment amount in US dollars.
    originator_name: str  # Display name of the sender.
    receiver_name: str  # Display name of the recipient.
    originator_state: str  # State or country of the sender.
    receiver_state: str  # State or country of the recipient.
    account_age_days: int  # Age of the sender's account in days.
    payment_type: str  # Business purpose of the payment.
    sec_code: str  # ACH Standard Entry Class code.
    channel: str  # Channel through which the payment was initiated.
    is_fraud: int  # Synthetic training label: 1 for fraud, 0 otherwise.


def _pick_parties(rng: random.Random, fraud: bool) -> tuple[str, str, str, str, str, str]:
    """Choose synthetic originator and receiver identities and locations."""
    originator_state = rng.choice(STATES)
    
    # Choose a US state, adding FOREIGN as an option for fraud samples.
    receiver_state = rng.choice(STATES if not fraud else STATES + ["FOREIGN"])
    
    # Choose a five-digit number and add the originator ID prefix.
    originator_id = f"ORIG-{rng.randint(10000, 99999)}"
    
    # Choose a five-digit number and add the receiver ID prefix.
    receiver_id = f"RCVR-{rng.randint(10000, 99999)}"
    originator_name = rng.choice(["Northstar Services", "Pine Valley Foods", "Harbor Manufacturing", "Summit Health"])
    receiver_name = rng.choice(["Avery Johnson", "Brightline Supply", "Cedar Utilities", "Maple Consulting"])
    return originator_id, receiver_id, originator_name, receiver_name, originator_state, receiver_state


def _sample_amount(rng: random.Random, fraud: bool) -> float:
    """Generate a payment amount. Fraudulent payments tend to be larger."""
    if fraud:
        # heavy-tailed
        return round(max(25.0, rng.lognormvariate(7.2, 0.9)), 2)
    return round(max(10.0, rng.lognormvariate(6.0, 0.75)), 2)


def _sample_transaction(
    rng: random.Random,
    ts: datetime,
    force_fraud: bool | None = None,
) -> TransactionRow:
    """Assemble one transaction, biasing attributes when it is fraudulent."""
    is_fraud = force_fraud if force_fraud is not None else (rng.random() < 0.04)
    originator_id, receiver_id, originator_name, receiver_name, originator_state, receiver_state = _pick_parties(rng, fraud=is_fraud)
    amount = _sample_amount(rng, fraud=is_fraud)
    account_age_days = rng.randint(2, 3650) if not is_fraud else rng.randint(1, 180)
    payment_type = rng.choice(PAYMENT_TYPES)
    sec_code = rng.choice(SEC_CODES if is_fraud else ["PPD", "CCD", "WEB"])
    channel = rng.choices(
        CHANNELS,
        weights=[0.45, 0.25, 0.2, 0.1] if not is_fraud else [0.2, 0.4, 0.3, 0.1],
        k=1,
    )[0]
    return TransactionRow(
        id=str(uuid.uuid4()),
        ts=ts.replace(tzinfo=timezone.utc).isoformat(),
        originator_id=originator_id,
        receiver_id=receiver_id,
        amount=amount,
        originator_name=originator_name,
        receiver_name=receiver_name,
        originator_state=originator_state,
        receiver_state=receiver_state,
        account_age_days=account_age_days,
        payment_type=payment_type,
        sec_code=sec_code,
        channel=channel,
        is_fraud=int(is_fraud),
    )


def seed_training_dataset(n_rows: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Create reproducible labeled records for training the fraud model."""
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
    """Generate a batch of new ACH payments to simulate incoming transactions.

    Each batch contains a mix of legitimate and potentially fraudulent
    payments. The fraud label is removed because real incoming payments
    do not come with a known fraud label.
    """
    import time

    rng = random.Random()
    rng.seed(int(time.time() * 1000) ^ int(np.random.randint(0, 1 << 30)))
    now = datetime.now(timezone.utc)
    rows: list[TransactionRow] = []
    for _i in range(batch_size):
        # Slight jitter keeps payment timestamps unique.
        ts = now - timedelta(seconds=rng.randint(0, 90))
        force = True if rng.random() < fraud_rate else None
        rows.append(_sample_transaction(rng, ts, force_fraud=force))
    # Return without the ground-truth label: live payments are unlabeled.
    return [{k: v for k, v in r.__dict__.items() if k != "is_fraud"} for r in rows]
