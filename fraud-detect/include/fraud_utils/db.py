"""SQLite persistence layer for transactions + human decisions.

Uses SQLite as a lightweight demo store. Not for production use.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable

from .paths import DB_PATH, ensure_dirs


SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id                TEXT PRIMARY KEY,
    ts                TEXT NOT NULL,
    user_id           INTEGER NOT NULL,
    card_id           INTEGER NOT NULL,
    amount            REAL NOT NULL,
    merchant          TEXT NOT NULL,
    merchant_city     TEXT NOT NULL,
    merchant_state    TEXT NOT NULL,
    mcc               INTEGER NOT NULL,
    use_chip          TEXT NOT NULL,
    fraud_score       REAL,
    is_suspicious     INTEGER DEFAULT 0,
    reasons           TEXT,
    human_decision    TEXT,
    human_notes       TEXT,
    reviewed_at       TEXT,
    created_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tx_ts ON transactions(ts DESC);
CREATE INDEX IF NOT EXISTS ix_tx_flag ON transactions(is_suspicious, human_decision);
"""


@contextmanager
def connect():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)


def insert_transactions(rows: Iterable[dict[str, Any]]) -> int:
    """Insert a batch of scored transactions. Returns count inserted."""
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for r in rows:
        payload.append(
            (
                r["id"],
                r["ts"],
                int(r["user_id"]),
                int(r["card_id"]),
                float(r["amount"]),
                r["merchant"],
                r["merchant_city"],
                r["merchant_state"],
                int(r["mcc"]),
                r["use_chip"],
                float(r.get("fraud_score", 0.0)),
                int(bool(r.get("is_suspicious", False))),
                json.dumps(r.get("reasons", [])),
                None,
                None,
                None,
                now,
            )
        )
    with connect() as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO transactions
                (id, ts, user_id, card_id, amount, merchant, merchant_city,
                 merchant_state, mcc, use_chip, fraud_score, is_suspicious,
                 reasons, human_decision, human_notes, reviewed_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    return len(payload)


def update_decision(tx_id: str, decision: str, notes: str | None = None) -> bool:
    """Record a human-in-the-loop decision for a transaction."""
    if decision not in {"Legitimate", "Fraud", "Needs further investigation"}:
        raise ValueError(f"Invalid decision: {decision}")
    now = datetime.now(timezone.utc).isoformat()
    with connect() as conn:
        cur = conn.execute(
            """
            UPDATE transactions
               SET human_decision = ?,
                   human_notes = ?,
                   reviewed_at = ?
             WHERE id = ?
            """,
            (decision, notes, now, tx_id),
        )
        return cur.rowcount > 0


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    if d.get("reasons"):
        try:
            d["reasons"] = json.loads(d["reasons"])
        except (TypeError, json.JSONDecodeError):
            d["reasons"] = []
    else:
        d["reasons"] = []
    return d


def fetch_summary() -> dict[str, Any]:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_tx,
                COALESCE(SUM(is_suspicious), 0) AS suspicious_tx,
                COALESCE(AVG(fraud_score), 0.0) AS avg_score,
                COALESCE(MAX(fraud_score), 0.0) AS max_score,
                COALESCE(SUM(CASE WHEN human_decision = 'Fraud' THEN 1 ELSE 0 END), 0) AS confirmed_fraud,
                COALESCE(SUM(CASE WHEN human_decision = 'Legitimate' THEN 1 ELSE 0 END), 0) AS confirmed_legit,
                COALESCE(SUM(CASE WHEN human_decision = 'Needs further investigation' THEN 1 ELSE 0 END), 0) AS needs_investigation
            FROM transactions
            """
        ).fetchone()
        return dict(row) if row else {}


def fetch_recent(limit: int = 50) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM transactions ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def fetch_flagged(limit: int = 100) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM transactions
             WHERE is_suspicious = 1
             ORDER BY fraud_score DESC, ts DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def fetch_pending_flagged(limit: int = 25) -> list[dict[str, Any]]:
    """Flagged transactions without a human decision yet."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM transactions
             WHERE is_suspicious = 1 AND human_decision IS NULL
             ORDER BY fraud_score DESC, ts DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def fetch_transaction(tx_id: str) -> dict[str, Any] | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM transactions WHERE id = ?", (tx_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None
