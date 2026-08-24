# ACH Fraud Detection - Airflow Demo

A hackathon-friendly, self-contained ACH payment fraud detection demo
orchestrated by Apache Airflow (Runtime 3.3 / Airflow 3.3). No external
banking APIs, no paid services, no internet dependency at runtime.

## What's in the box

| Component                     | File(s)                                                      | Description                                                                                                                                                                                                              |
| ----------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Seed dataset + model training | `scripts/generate_seed_data.py`, `dags/fraud_bootstrap.py`   | Generates the labeled seed CSV once, then trains a scikit-learn `RandomForestClassifier` from that file.                                                                                                                 |
| Live payment stream           | `dags/fraud_stream.py`                                       | Every 2 minutes: simulates incoming ACH payments, scores them with the trained model, runs a rule-based explainer, flags suspicious payments, and persists everything to SQLite. Emits the `flagged_transactions` Asset. |
| Human-in-the-loop review      | `dags/fraud_hitl_review.py`                                  | Asset-triggered DAG that spawns one `HITLOperator` "Required Action" per unresolved flagged transaction. Reviewers can respond in Airflow's **Browse -> Required Actions** or the dashboard.                             |
| Dashboard plugin              | `plugins/fraud_dashboard.py`, `plugins/fraud_dashboard.html` | FastAPI plugin mounted in the Airflow API server. Serves KPIs, ACH payments, flags, per-payment reasons, and a one-click Legitimate / Fraud / Needs Investigation form.                                                  |
| Shared code                   | `include/fraud_utils/`                                       | SQLite persistence, ACH payment generator, feature engineering, and rule-based reason explainer.                                                                                                                         |

## How to demo

1. Start the environment (`astro dev start` locally, or the Astro IDE
   test deployment).
2. Generate the seed data once, outside Airflow:
   `python3 scripts/generate_seed_data.py`
3. **fraud_bootstrap** runs automatically once - it reads the seed CSV and
   trains the model into `include/models/ach_fraud_model.joblib`.
4. **fraud_stream** starts ticking every 2 minutes, generating and
   scoring ACH payments.
5. Open the **ACH Fraud Dashboard** from the Airflow navbar (or hit
   `/fraud-dashboard/` directly).
6. Watch KPIs rise, click any flagged payment to see why it was
   flagged, and click Legitimate / Fraud / Needs Investigation to
   record a human decision. The decision immediately shows up in the
   dashboard and in the SQLite store.
7. Alternatively, respond via **Browse -> Required Actions** in the
   Airflow UI - `fraud_hitl_review` writes the same decisions back
   through the standard-provider `HITLOperator`.

## Why is a payment flagged?

Each flagged payment stores a list of human-readable reasons
computed by `include/fraud_utils/reasons.py`:

- Unusually / very large payment amount
- Amount is many multiples of the originator's historical average
- Unusual hour of day
- New receiver for this originator
- Cross-state or international payment
- WEB/TEL payment above the review threshold
- New originator account
- Large batch-file payment
- Unusual transaction frequency (bursts in a short window)
- Model risk score above threshold (fallback)

## Human decisions

Any of these three decisions gets persisted to the `ach_payments`
table in SQLite and rendered as a badge in the dashboard:

- Legitimate
- Fraud
- Needs further investigation

## Notes

- Storage is SQLite under `include/data/ach_fraud.db` for demo simplicity.
- Airflow 3.1+ is required for the `HITLOperator` (bundled in
  Runtime 3.3-2).
- The included ACH dataset is generated for the demo. Replace
  `seed_training_dataset()` in `include/fraud_utils/generator.py` with a
  real ACH dataset loader when one is available.
