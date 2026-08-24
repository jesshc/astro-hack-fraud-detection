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


LARGE_AMOUNT_USD = 5000.0
VERY_LARGE_AMOUNT_USD = 25000.0
NIGHT_START_HOUR = 22
NIGHT_END_HOUR = 6
HIGH_FREQ_WINDOW_MIN = 10
HIGH_FREQ_THRESHOLD = 3


def _originator_history(originator_id: str) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
                        SELECT ts, amount, receiver_id, receiver_state
                            FROM ach_payments
                         WHERE originator_id = ?
             ORDER BY ts DESC
             LIMIT 200
            """,
            (originator_id,),
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
    history = _originator_history(str(tx["originator_id"]))

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

    # New receiver for this originator
    known_receivers = {h["receiver_id"] for h in history}
    if history and tx["receiver_id"] not in known_receivers:
        reasons.append(f"New receiver for this originator ({tx['receiver_id']})")

    # Cross-state payment rule
    if tx["originator_state"] != tx["receiver_state"]:
        reasons.append(
            f"Cross-state payment ({tx['originator_state']} to {tx['receiver_state']})"
        )

    if str(tx["receiver_state"]).upper() == "FOREIGN":
        reasons.append("International receiver requires review")

    if tx["sec_code"] in {"WEB", "TEL"} and amt >= LARGE_AMOUNT_USD:
        reasons.append(f"{tx['sec_code']} payment over ${LARGE_AMOUNT_USD:,.0f}")

    if int(tx["account_age_days"]) < 30:
        reasons.append("Originator account is less than 30 days old")

    if tx["channel"] == "File upload" and amt >= VERY_LARGE_AMOUNT_USD:
        reasons.append("Large batch-file payment")

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
