"""
## ACH Fraud Stream DAG

Runs every 2 minutes and simulates the arrival of a batch of live
ACH payments. For each batch it:

1. Generates ~15 ACH payments with a plausible fraud mix.
2. Scores each transaction with the trained RandomForest model.
3. Runs a rule-based explainer to produce human-readable reasons.
4. Flags high-risk transactions and persists everything to SQLite.
5. Emits the `flagged_transactions` Asset when at least one flag
   was raised - this drives the downstream HITL review DAG.
"""

from __future__ import annotations

from pendulum import datetime, duration

from airflow.sdk import Asset, dag, task


FLAGGED_ASSET = Asset("flagged_transactions")

# Score threshold above which a transaction is treated as suspicious.
RISK_THRESHOLD = 0.55


@dag(
    dag_id="fraud_stream",
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "fraud-demo",
        "retries": 2,
        "retry_delay": duration(seconds=15),
    },
    tags=["fraud", "streaming"],
    doc_md=__doc__,
)
def fraud_stream():
    @task
    def simulate_batch() -> list[dict]:
        """Create a new batch of simulated ACH payments for this run."""
        from include.fraud_utils import generate_batch, init_db

        # Make sure the schema exists even if bootstrap hasn't run yet.
        init_db()
        batch = generate_batch(batch_size=15, fraud_rate=0.15)
        print(f"Generated {len(batch)} incoming ACH payments")
        return batch

    @task
    def score_batch(batch: list[dict]) -> list[dict]:
        """Load the saved model and add a fraud score to each payment."""
        import joblib

        from include.fraud_utils import MODEL_PATH, build_feature_frame

        if not MODEL_PATH.exists():
            raise FileNotFoundError(
                f"Model not found at {MODEL_PATH}. "
                "Trigger the `fraud_bootstrap` DAG first."
            )
        model = joblib.load(MODEL_PATH)
        X = build_feature_frame(batch)
        proba = model.predict_proba(X)[:, 1]
        for tx, p in zip(batch, proba, strict=False):
            tx["fraud_score"] = round(float(p), 4)
        return batch

    @task
    def flag_and_persist(batch: list[dict]) -> dict:
        """Explain, flag, and save the scored payments to SQLite."""
        from include.fraud_utils import insert_transactions
        from include.fraud_utils.reasons import explain

        flagged = 0
        for tx in batch:
            score = float(tx.get("fraud_score", 0.0))
            reasons = explain(tx, score) if score >= RISK_THRESHOLD else []
            is_suspicious = score >= RISK_THRESHOLD or bool(reasons)
            tx["is_suspicious"] = is_suspicious
            tx["reasons"] = reasons
            if is_suspicious:
                flagged += 1

        inserted = insert_transactions(batch)
        print(f"Persisted {inserted} ACH payments; flagged {flagged}.")
        return {"inserted": inserted, "flagged": flagged}

    @task(outlets=[FLAGGED_ASSET])
    def announce_if_flagged(summary: dict) -> dict:
        """Emit the Asset only when this batch produced new flags."""
        if summary.get("flagged", 0) > 0:
            print(f"New flags in this batch: {summary['flagged']} - emitting asset.")
        else:
            print("No new flags this batch.")
        return summary

    announce_if_flagged(flag_and_persist(score_batch(simulate_batch())))


fraud_stream()
