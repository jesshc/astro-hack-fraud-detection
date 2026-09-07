"""
## ACH Fraud HITL Review DAG

Triggered by the `flagged_transactions` Asset emitted by `fraud_stream`.

For every currently-unresolved flagged transaction it dynamically spawns
one `HITLOperator` "Required Action" task, letting a human reviewer pick
between:

- Legitimate
- Fraud
- Needs further investigation

The reviewer can respond either in the Airflow UI (**Browse > Required
Actions**) or through the built-in fraud dashboard. The chosen decision
is persisted back to SQLite so it shows up in the dashboard immediately.

Requires Airflow 3.1+ (HITL operators live in the standard provider).
"""

from __future__ import annotations

from datetime import timedelta

from pendulum import datetime

from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Asset, dag, get_current_context, task


FLAGGED_ASSET = Asset("flagged_transactions")

DECISION_OPTIONS = ["Legitimate", "Fraud", "Needs further investigation"]


@dag(
    dag_id="fraud_hitl_review",
    start_date=datetime(2026, 1, 1),
    schedule=[FLAGGED_ASSET],
    catchup=False,
    is_paused_upon_creation=False,
    # Serialize asset-triggered runs so pending transactions cannot be
    # collected by multiple runs before their HITL references are registered.
    max_active_runs=1,
    default_args={
        "owner": "fraud-demo",
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["fraud", "hitl"],
    doc_md=__doc__,
)
def fraud_hitl_review():
    @task
    def collect_pending() -> list[dict]:
        """Grab every unassigned flagged ACH payment for this review run."""
        from include.fraud_utils import fetch_pending_flagged

        pending = fetch_pending_flagged(limit=None)
        print(f"Found {len(pending)} pending flagged ACH payments to review.")
        return pending

    @task
    def build_review_payloads(txs: list[dict]) -> list[dict]:
        """Build one review payload per flagged transaction for human review."""
        rows = []
        for tx in txs:
            subject = (
                f"Review flagged ACH payment "
                f"${tx['amount']:.2f} to {tx['receiver_name']} "
                f"(risk {float(tx['fraud_score']):.2f})"
            )
            reasons_md = (
                "\n".join(f"- {r}" for r in tx.get("reasons") or []) or "- (n/a)"
            )
            body = (
                f"**Payment ID:** `{tx['id']}`\n\n"
                f"**Timestamp:** {tx['ts']}\n\n"
                f"**Originator / Receiver:** {tx['originator_id']} / {tx['receiver_id']}\n\n"
                f"**Amount:** ${tx['amount']:.2f}\n\n"
                f"**Originator / Receiver:** {tx['originator_name']} -> {tx['receiver_name']}\n\n"
                f"**States:** {tx['originator_state']} -> {tx['receiver_state']}\n\n"
                f"**Payment type / SEC code:** {tx['payment_type']} / {tx['sec_code']}\n\n"
                f"**Channel:** {tx['channel']}\n\n"
                f"**Model risk score:** {float(tx['fraud_score']):.2f}\n\n"
                f"**Reasons flagged:**\n{reasons_md}"
            )
            rows.append({"tx_id": tx["id"], "subject": subject, "body": body})
        return rows

    @task
    def extract_tx_ids(rows: list[dict]) -> list[str]:
        """Keep the transaction IDs in the same order as the HITL tasks."""
        return [row["tx_id"] for row in rows]

    @task
    def to_hitl_payloads(rows: list[dict]) -> list[dict]:
        """Strip review rows down to the fields HITLOperator accepts."""
        return [{"subject": row["subject"], "body": row["body"]} for row in rows]

    @task
    def register_hitl_references(tx_ids: list[str]) -> None:
        """Persist each transaction's dynamically mapped HITL task identity."""
        from include.fraud_utils import register_hitl_tasks

        context = get_current_context()
        register_hitl_tasks(
            tx_ids,
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            task_id="await_reviewer_decision",
        )

    review_rows = build_review_payloads(collect_pending())
    tx_ids = extract_tx_ids(review_rows)
    hitl_payloads = to_hitl_payloads(review_rows)
    references = register_hitl_references(tx_ids)

    review = HITLOperator.partial(
        task_id="await_reviewer_decision",
        options=DECISION_OPTIONS,
        defaults=["Needs further investigation"],
        multiple=False,
        # 24-hour SLA: if no one responds, mark as "Needs further investigation".
        response_timeout=timedelta(hours=24),
    ).expand_kwargs(hitl_payloads)

    references >> review

fraud_hitl_review()
