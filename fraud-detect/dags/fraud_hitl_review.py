"""
## Fraud HITL Review DAG

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

from pendulum import datetime, duration

from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Asset, dag, task


FLAGGED_ASSET = Asset("flagged_transactions")

DECISION_OPTIONS = ["Legitimate", "Fraud", "Needs further investigation"]


@dag(
    dag_id="fraud_hitl_review",
    start_date=datetime(2026, 1, 1),
    schedule=[FLAGGED_ASSET],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=3,
    default_args={
        "owner": "fraud-demo",
        "retries": 2,
        "retry_delay": duration(seconds=10),
    },
    tags=["fraud", "hitl"],
    doc_md=__doc__,
)
def fraud_hitl_review():
    @task
    def collect_pending() -> list[dict]:
        """Grab up to 10 flagged transactions with no human decision yet."""
        from include.fraud_utils import fetch_pending_flagged

        pending = fetch_pending_flagged(limit=10)
        print(f"Found {len(pending)} pending flagged transactions to review.")
        return pending

    @task
    def split_pending(txs: list[dict]) -> list[dict]:
        """Build one review payload per flagged transaction."""
        rows = []
        for tx in txs:
            subject = (
                f"Review flagged transaction "
                f"${tx['amount']:.2f} @ {tx['merchant']} "
                f"(risk {float(tx['fraud_score']):.2f})"
            )
            reasons_md = (
                "\n".join(f"- {r}" for r in tx.get("reasons") or []) or "- (n/a)"
            )
            body = (
                f"**Transaction ID:** `{tx['id']}`\n\n"
                f"**Timestamp:** {tx['ts']}\n\n"
                f"**User / Card:** {tx['user_id']} / {tx['card_id']}\n\n"
                f"**Amount:** ${tx['amount']:.2f}\n\n"
                f"**Merchant:** {tx['merchant']} - "
                f"{tx['merchant_city']}, {tx['merchant_state']} (MCC {tx['mcc']})\n\n"
                f"**Channel:** {tx['use_chip']}\n\n"
                f"**Model risk score:** {float(tx['fraud_score']):.2f}\n\n"
                f"**Reasons flagged:**\n{reasons_md}"
            )
            rows.append({"tx_id": tx["id"], "subject": subject, "body": body})
        return rows

    @task
    def extract_tx_ids(rows: list[dict]) -> list[str]:
        """Keep the transaction IDs in the same order as the HITL tasks."""
        return [row["tx_id"] for row in rows]

    review_rows = split_pending(collect_pending())
    tx_ids = extract_tx_ids(review_rows)

    # Dynamically map one HITL task per pending flagged transaction.
    review = HITLOperator.partial(
        task_id="await_reviewer_decision",
        options=DECISION_OPTIONS,
        defaults=["Needs further investigation"],
        multiple=False,
        # 24-hour SLA: if no one responds, mark as "Needs further investigation".
        execution_timeout=duration(hours=24),
    ).expand_kwargs(review_rows)

    @task(trigger_rule="all_done")
    def record_decision(review_output: dict, tx_id: str) -> str:
        """Write the human's chosen decision back to SQLite."""
        from include.fraud_utils import update_decision

        chosen = (review_output or {}).get("chosen_options") or []
        decision = chosen[0] if chosen else "Needs further investigation"
        update_decision(tx_id, decision, notes="Recorded via Airflow HITL")
        print(f"Recorded decision '{decision}' for {tx_id}.")
        return decision

    record_decision.expand(
        review_output=review.output,
        tx_id=tx_ids,
    )


fraud_hitl_review()
