"""SQLite persistence layer for ACH payments and human decisions.

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
CREATE TABLE IF NOT EXISTS ach_payments (
    id                TEXT PRIMARY KEY,
    ts                TEXT NOT NULL,
    originator_id     TEXT NOT NULL,
    receiver_id       TEXT NOT NULL,
    amount            REAL NOT NULL,
    originator_name   TEXT NOT NULL,
    receiver_name     TEXT NOT NULL,
    originator_state  TEXT NOT NULL,
    receiver_state    TEXT NOT NULL,
    account_age_days  INTEGER NOT NULL,
    payment_type      TEXT NOT NULL,
    sec_code          TEXT NOT NULL,
    channel           TEXT NOT NULL,
    fraud_score       REAL,
    is_suspicious     INTEGER DEFAULT 0,
    reasons           TEXT,
    human_decision    TEXT,
    human_notes       TEXT,
    reviewed_at       TEXT,
    hitl_dag_id       TEXT,
    hitl_run_id       TEXT,
    hitl_task_id      TEXT,
    hitl_map_index    INTEGER,
    created_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ach_ts ON ach_payments(ts DESC);
CREATE INDEX IF NOT EXISTS ix_ach_flag ON ach_payments(is_suspicious, human_decision);
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
        existing_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(ach_payments)")
        }
        for column, column_type in (
            ("hitl_dag_id", "TEXT"),
            ("hitl_run_id", "TEXT"),
            ("hitl_task_id", "TEXT"),
            ("hitl_map_index", "INTEGER"),
        ):
            if column not in existing_columns:
                conn.execute(
                    f"ALTER TABLE ach_payments ADD COLUMN {column} {column_type}"
                )


def insert_transactions(rows: Iterable[dict[str, Any]]) -> int:
    """Insert a batch of scored ACH payments. Returns count inserted."""
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for r in rows:
        payload.append(
            (
                r["id"],
                r["ts"],
                r["originator_id"],
                r["receiver_id"],
                float(r["amount"]),
                r["originator_name"],
                r["receiver_name"],
                r["originator_state"],
                r["receiver_state"],
                int(r["account_age_days"]),
                r["payment_type"],
                r["sec_code"],
                r["channel"],
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
            INSERT OR IGNORE INTO ach_payments
                (id, ts, originator_id, receiver_id, amount, originator_name,
                 receiver_name, originator_state, receiver_state, account_age_days,
                 payment_type, sec_code, channel, fraud_score, is_suspicious,
                 reasons, human_decision, human_notes, reviewed_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            UPDATE ach_payments
               SET human_decision = ?,
                   human_notes = ?,
                   reviewed_at = ?
             WHERE id = ?
            """,
            (decision, notes, now, tx_id),
        )
        return cur.rowcount > 0


def register_hitl_tasks(
    tx_ids: list[str], dag_id: str, run_id: str, task_id: str
) -> None:
    """Associate transactions with their dynamically mapped HITL tasks."""
    with connect() as conn:
        for map_index, tx_id in enumerate(tx_ids):
            conn.execute(
                """
                UPDATE ach_payments
                   SET hitl_dag_id = ?, hitl_run_id = ?, hitl_task_id = ?,
                       hitl_map_index = ?
                 WHERE id = ? AND human_decision IS NULL
                """,
                (dag_id, run_id, task_id, map_index, tx_id),
            )


def fetch_hitl_reference(tx_id: str) -> dict[str, Any] | None:
    """Return the Airflow identity associated with a transaction."""
    with connect() as conn:
        row = conn.execute(
            """
            SELECT hitl_dag_id, hitl_run_id, hitl_task_id, hitl_map_index
              FROM ach_payments
             WHERE id = ?
            """,
            (tx_id,),
        ).fetchone()
        return dict(row) if row else None


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
                COALESCE(AVG(CASE WHEN is_suspicious = 1 THEN fraud_score END), 0.0) AS avg_flagged_score,
                COALESCE(MAX(fraud_score), 0.0) AS max_score,
                COALESCE(SUM(CASE WHEN human_decision = 'Fraud' THEN 1 ELSE 0 END), 0) AS confirmed_fraud,
                COALESCE(SUM(CASE WHEN human_decision = 'Legitimate' THEN 1 ELSE 0 END), 0) AS confirmed_legit,
                COALESCE(SUM(CASE WHEN human_decision = 'Needs further investigation' THEN 1 ELSE 0 END), 0) AS needs_investigation
            FROM ach_payments
            """
        ).fetchone()
        return dict(row) if row else {}


def fetch_recent(limit: int = 50) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM ach_payments ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def fetch_flagged(limit: int = 100) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM ach_payments
             WHERE is_suspicious = 1 AND human_decision IS NULL
             ORDER BY fraud_score DESC, ts DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def fetch_pending_flagged(limit: int = 25) -> list[dict[str, Any]]:
    """Flagged ACH payments without a human decision yet."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM ach_payments
             WHERE is_suspicious = 1 AND human_decision IS NULL
                             AND hitl_dag_id IS NULL
             ORDER BY fraud_score DESC, ts DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]


def fetch_transaction(tx_id: str) -> dict[str, Any] | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM ach_payments WHERE id = ?", (tx_id,)
        ).fetchone()
        return _row_to_dict(row) if row else None
