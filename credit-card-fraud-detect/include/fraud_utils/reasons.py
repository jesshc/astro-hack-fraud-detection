"""Rule-based reason explanations that accompany each fraud score.

The ML model gives us a probability but doesn't explain itself. To
support the 'why was this flagged?' view in the dashboard we run a set
of interpretable rules over each transaction and any recent user
history and emit human-readable reasons.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .db import connect


LARGE_AMOUNT_USD = 500.0
VERY_LARGE_AMOUNT_USD = 2000.0
NIGHT_START_HOUR = 22
NIGHT_END_HOUR = 6
HIGH_FREQ_WINDOW_MIN = 10
HIGH_FREQ_THRESHOLD = 3


def _user_history(user_id: int) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT ts, amount, merchant, merchant_city, merchant_state
              FROM transactions
             WHERE user_id = ?
             ORDER BY ts DESC
             LIMIT 200
            """,
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def _parse_ts(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def explain(tx: dict[str, Any], score: float) -> list[str]:
    """Return a list of human-readable reasons for flagging this tx."""
    reasons: list[str] = []
    amt = float(tx["amount"])
    ts = _parse_ts(tx["ts"])
    history = _user_history(int(tx["user_id"]))

    # Amount rules
    if amt >= VERY_LARGE_AMOUNT_USD:
        reasons.append(f"Very large amount (${amt:,.2f})")
    elif amt >= LARGE_AMOUNT_USD:
        reasons.append(f"Unusually large amount (${amt:,.2f})")

    # Compare vs user's typical spend
    if history:
        avg = sum(h["amount"] for h in history) / len(history)
        if avg > 0 and amt > 5 * avg:
            reasons.append(
                f"Amount is {amt / avg:.1f}x this user's average (${avg:,.2f})"
            )

    # Night-time rule
    hour = ts.hour
    if hour >= NIGHT_START_HOUR or hour < NIGHT_END_HOUR:
        reasons.append(f"Unusual hour of day ({hour:02d}:00 UTC)")

    # New merchant for this user
    known_merchants = {h["merchant"] for h in history}
    if history and tx["merchant"] not in known_merchants:
        reasons.append(f"New merchant for this user ({tx['merchant']})")

    # Unusual location: state the user has never transacted in
    known_states = {h["merchant_state"] for h in history}
    if history and tx["merchant_state"] not in known_states:
        reasons.append(
            f"Unusual location ({tx['merchant_city']}, {tx['merchant_state']})"
        )

    # Foreign location
    if str(tx["merchant_state"]).upper() == "FOREIGN":
        reasons.append("Foreign merchant")

    # Online / card-not-present
    if tx["use_chip"] == "Online Transaction" and amt >= LARGE_AMOUNT_USD:
        reasons.append("Card-not-present transaction over $500")

    # High frequency in short window
    if history:
        window_start = ts - timedelta(minutes=HIGH_FREQ_WINDOW_MIN)
        recent = [h for h in history if _parse_ts(h["ts"]) >= window_start]
        if len(recent) >= HIGH_FREQ_THRESHOLD:
            reasons.append(
                f"Unusual transaction frequency ({len(recent)} tx in {HIGH_FREQ_WINDOW_MIN} min)"
            )

    # Model-score fallback
    if not reasons and score >= 0.8:
        reasons.append(f"Model risk score {score:.2f} above threshold")

    return reasons
