"""
## Fraud Bootstrap DAG

One-shot DAG that:

1. Creates the SQLite tables used by the rest of the demo.
2. Generates a synthetic IBM-style credit-card transactions seed
   dataset with an `is_fraud` label.
3. Trains a scikit-learn RandomForest fraud detection model on that
   seed dataset and persists it to `include/models/fraud_model.joblib`.

Runs once when the environment first starts. Re-trigger it manually to
regenerate the seed data / retrain the model.
"""

from __future__ import annotations

from pendulum import datetime, duration

from airflow.sdk import dag, task


@dag(
    dag_id="fraud_bootstrap",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "fraud-demo",
        "retries": 2,
        "retry_delay": duration(seconds=15),
    },
    tags=["fraud", "setup", "ml"],
    doc_md=__doc__,
)
def fraud_bootstrap():
    @task
    def init_storage() -> str:
        from include.fraud_utils import DB_PATH, ensure_dirs, init_db

        ensure_dirs()
        init_db()
        return str(DB_PATH)

    @task
    def seed_training_data() -> str:
        """Generate + persist a synthetic IBM-style seed dataset."""
        from include.fraud_utils import seed_training_dataset
        from include.fraud_utils.paths import TRAINING_CSV, ensure_dirs

        ensure_dirs()
        df = seed_training_dataset(n_rows=5000, seed=42)
        df.to_csv(TRAINING_CSV, index=False)
        print(
            f"Wrote {len(df)} training rows to {TRAINING_CSV}. "
            f"Fraud rate: {df['is_fraud'].mean():.2%}"
        )
        return str(TRAINING_CSV)

    @task
    def train_model(training_csv: str) -> str:
        """Train a RandomForest fraud classifier + save it to disk."""
        import joblib
        import pandas as pd
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import classification_report, roc_auc_score
        from sklearn.model_selection import train_test_split

        from include.fraud_utils import MODEL_PATH, build_feature_frame
        from include.fraud_utils.paths import ensure_dirs

        ensure_dirs()
        df = pd.read_csv(training_csv)
        X = build_feature_frame(df.to_dict(orient="records"))
        y = df["is_fraud"].astype(int)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, stratify=y, random_state=42
        )

        model = RandomForestClassifier(
            n_estimators=200,
            max_depth=10,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_train, y_train)

        proba = model.predict_proba(X_test)[:, 1]
        preds = (proba >= 0.5).astype(int)
        auc = roc_auc_score(y_test, proba)
        print(f"Holdout ROC-AUC: {auc:.4f}")
        print(classification_report(y_test, preds, digits=3))

        joblib.dump(model, MODEL_PATH)
        print(f"Saved model to {MODEL_PATH}")
        return str(MODEL_PATH)

    db = init_storage()
    csv = seed_training_data()
    trained = train_model(csv)

    db >> csv >> trained


fraud_bootstrap()
