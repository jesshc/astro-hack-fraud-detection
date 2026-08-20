# Credit Card Fraud Detection - Airflow Demo

A hackathon-friendly, self-contained credit card fraud detection demo
orchestrated by Apache Airflow (Runtime 3.3 / Airflow 3.3). No external
banking APIs, no paid services, no internet dependency at runtime.

## What's in the box

| Component | File(s) | Description |
| --- | --- | --- |
| Seed dataset + model training | `dags/fraud_bootstrap.py` | Generates a synthetic IBM-style credit card transactions dataset and trains a scikit-learn `RandomForestClassifier`. |
| Live transaction stream | `dags/fraud_stream.py` | Every 2 minutes: simulates a batch of incoming transactions, scores them with the trained model, runs a rule-based explainer, flags suspicious ones, and persists everything to SQLite. Emits the `flagged_transactions` Asset. |
| Human-in-the-loop review | `dags/fraud_hitl_review.py` | Asset-triggered DAG that spawns one `HITLOperator` "Required Action" per unresolved flagged transaction. Reviewers can respond in Airflow's **Browse -> Required Actions** or the dashboard. |
| Dashboard plugin | `plugins/fraud_dashboard.py`, `plugins/fraud_dashboard.html` | FastAPI plugin mounted in the Airflow API server. Serves KPIs, transactions, flags, per-transaction reasons, and a one-click Legitimate / Fraud / Needs Investigation form. |
| Shared code | `include/fraud_utils/` | SQLite persistence, IBM-style transaction generator, feature engineering, and rule-based reason explainer. |

## How to demo

1. Start the environment (`astro dev start` locally, or the Astro IDE
   test deployment).
2. **fraud_bootstrap** runs automatically once - it seeds the training
   data and trains the model into `include/models/fraud_model.joblib`.
3. **fraud_stream** starts ticking every 2 minutes, generating and
   scoring transactions.
4. Open the **Fraud Dashboard** from the Airflow navbar (or hit
   `/fraud-dashboard/` directly).
5. Watch KPIs rise, click any flagged transaction to see why it was
   flagged, and click Legitimate / Fraud / Needs Investigation to
   record a human decision. The decision immediately shows up in the
   dashboard and in the SQLite store.
6. Alternatively, respond via **Browse -> Required Actions** in the
   Airflow UI - `fraud_hitl_review` writes the same decisions back
   through the standard-provider `HITLOperator`.

## Why is a transaction flagged?

Each flagged transaction stores a list of human-readable reasons
computed by `include/fraud_utils/reasons.py`:

- Unusually / very large amount
- Amount is many multiples of the user's historical average
- Unusual hour of day
- New merchant for this user
- Unusual (or foreign) location
- Card-not-present transaction over $500
- Unusual transaction frequency (bursts in a short window)
- Model risk score above threshold (fallback)

## Human decisions

Any of these three decisions gets persisted to the `transactions`
table in SQLite and rendered as a badge in the dashboard:

- Legitimate
- Fraud
- Needs further investigation

## Notes

- Storage is SQLite under `include/data/fraud.db` for demo simplicity.
- Airflow 3.1+ is required for the `HITLOperator` (bundled in
  Runtime 3.3-2).
- The synthetic dataset mimics the schema of IBM's public Credit Card
  Transactions dataset. Swap `seed_training_dataset()` in
  `include/fraud_utils/generator.py` for the real CSV if you have it
  locally.
