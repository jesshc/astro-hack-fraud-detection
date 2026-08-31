"""Generate the labeled seed CSV used by the fraud bootstrap DAG."""

from pathlib import Path

from include.fraud_utils.generator import seed_training_dataset


PROJECT_DIR = Path(__file__).resolve().parents[1]
TRAINING_CSV = PROJECT_DIR / "include" / "data" / "ach_payments.csv"


def main() -> None:
    """Generate the seed dataset once and save it for Airflow to read."""
    TRAINING_CSV.parent.mkdir(parents=True, exist_ok=True)
    df = seed_training_dataset(n_rows=5000, seed=42)
    df.to_csv(TRAINING_CSV, index=False)
    print(f"Wrote {len(df)} training rows to {TRAINING_CSV}")
    print(f"ACH fraud rate: {df['is_fraud'].mean():.2%}")


if __name__ == "__main__":
    main()
