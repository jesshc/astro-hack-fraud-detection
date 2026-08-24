"""
## ACH Fraud Bootstrap DAG

One-shot DAG that:

1. Creates the SQLite tables used by the rest of the demo.
2. Reads the labeled ACH payment seed dataset from `include/data/ach_payments.csv`.
3. Trains a scikit-learn RandomForest fraud detection model on that
    seed dataset and persists it to `include/models/ach_fraud_model.joblib`.

Runs once when the environment first starts. Re-trigger it manually to
retrain the model from the existing seed data.
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
    def train_model() -> str:
        """Train a RandomForest fraud classifier + save it to disk."""
        import joblib
        import pandas as pd
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import classification_report, roc_auc_score
        from sklearn.model_selection import train_test_split

        from include.fraud_utils import MODEL_PATH, build_feature_frame
        from include.fraud_utils.paths import TRAINING_CSV
        from include.fraud_utils.paths import ensure_dirs

        ensure_dirs()
        if not TRAINING_CSV.exists():
            raise FileNotFoundError(
                f"Seed training data not found at {TRAINING_CSV}. "
                "Generate the seed CSV separately before triggering this DAG."
            )
        df = pd.read_csv(TRAINING_CSV)
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
    trained = train_model()

    db >> trained


fraud_bootstrap()
