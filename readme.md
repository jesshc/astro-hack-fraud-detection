# ACH Payment Fraud Detector

ACH Payment Fraud Detector is an Airflow-powered fraud detection system that combines machine learning with human-in-the-loop (HITL) review to help financial institutions efficiently identify fraudulent payments.

ACH (Automated Clearing House) payments are electronic bank-to-bank money transfer transactions, commonly used for direct deposits, bill payments, payroll, and business transactions. Financial institutions process millions of these payments every day, making it impractical for fraud monitoring personnel to manually investigate every suspicious transaction.

This application helps employees of financial institutions focus their attention on high-risk payments by automating fraud detection, while keeping humans in the loop for final review.

## What does this project do?

This project simulates how financial institutions could use Airflow and machine learning to automate the workflow of detecting suspicious ACH transactions and presenting them to bank personnel for final assessment through a centralized web dashboard. Since real world banking transactions cannot be acquired, all data presented in this project is synthetically generated and modeled based on the real world ACH data defined by the National Automated Clearing House Association (NACHA).

Before the Airflow workflows run, the [`scripts/generate_seed_data.py`](scripts/generate_seed_data.py) script generates an initial labeled training dataset of 5,000 synthetic ACH payment records. The dataset includes payment attributes and synthetic fraud labels that are used to train a RandomForest machine learning model. The generated training data file is included in the repository at [`include/data/ach_payments.csv`](include/data/ach_payments.csv).

To simulate incoming payments, the application continuously generates a batch of 15 synthetic ACH payments every 2 minutes. It then evaluates each transaction using the trained ML model. he ML model evaluates each new payment and tags a fraud risk score, ranging from 0 to 1. Payments with a score of 0.55 or higher are considered potential fraud and flagged for human review. explanations highlighting the factors that contributed to their risk score.

Flagged transactions are displayed on the web dashboard which provides an interface for fraud monitoring personnel to review the transaction details and classify them as:

- **Legitimate**
- **Fraudulent**
- **Needs Further Investigation**

Reviewers' decisions are recorded in the database and reflected on the dashboard at real-time.

## How it works

### 1. Generate the initial training data for machine learning model

The [`scripts/generate_seed_data.py`](scripts/generate_seed_data.py) script is used to create the initial labeled dataset to train the RandomForest machine learning model. It generates 5,000 synthetic ACH payment records with realistic transaction attributes and synthetic fraud labels, then saves them to [`include/data/ach_payments.csv`](include/data/ach_payments.csv).

The seed dataset is already included in the repository, so you do not need to run `generate_seed_data.py` when running the application.

Note: Airflow is not used in this step

### 2. Bootstrap and ML model setup

The project starts with the `fraud_bootstrap` DAG, which handles a one-time setup for the application.

This DAG:

- Creates the SQLite database schema for ACH transactions and metadata for recording human review
- Loads the synthetic training data from [`include/data/ach_payments.csv`](include/data/ach_payments.csv)
- Trains a RandomForest ML model
- Saves the trained model to [`include/models/ach_fraud_model.joblib`](include/models/ach_fraud_model.joblib)

The bootstrap workflow runs before the streaming pipeline so that the database and the trained model are ready before new payments are generated and processed.

### 3. Stream incoming ACH transactions

The `fraud_stream` DAG runs every 2 minutes to simulate a new batch of incoming live ACH payments.

For each batch, it:

- Generates 15 synthetic ACH transactions
- Marks each transaction with a fraud risk score using the trained RandomForest model
- Flags any transaction with a fraud score of 0.55 or higher
- Generates reasons that explain why a transaction is flagged as high-risk
- Stores transaction data, risk scores, and explanations of all transactions in the batch into the SQLite database

If the batch contains any flagged transactions, an Airflow asset named `flagged_transactions` gets emitted to trigger the next `fraud_hitl_review` DAG.

### 4. Create the HITL review tasks

When the `flagged_transactions` asset is emitted, the `fraud_hitl_review` DAG loads the pending flagged transactions from SQLite and creates a human-in-the-loop review task for each payment.

For each transaction, the DAG

- Loads the payment details and fraud-risk information
- Builds a review payload containing the transaction amount, risk score, and reasons for flagging
- Registers the Airflow mapping information needed to identify the corresponding HITL task
- Creates a `HITLOperator` task for the transaction with a `Choice Required` state.

The flagged high-risk transactions are displayed in the Flagged ACH Payments section of the web dashboard. Each flagged transaction is mapped to a HITL review task in Airflow. The human reviewer can select a particular flagged payment from the dashboard to review its transaction details and mark the transaction with one of the following decisions:

- Legitimate
- Fraud
- Needs Further Investigation

Once the reviewer submits the decision, the transaction data in SQLite gets updated with the human decision. The transaction's corresponding HITL task in Airflow also gets updated with a `Choice Received` state.

## What was hard?

**Keeping transactions displayed in dashboard synchronized with Airflow HITL tasks**

One challenge I faced was ensuring that the human decision for each flagged ACH payments submitted through the dashboard remained synchronized with their underlying Airflow HITL tasks in Airflow UI’s Required Actions queue.

It was challenging to coordinate states across three layers:

- **Custom web dashboard** — where reviewers submit decisions
- **SQLite database** — where transaction and review state is persisted
- **Airflow HITL task state** — where the underlying human review tasks is managed

To solve this, when a reviewer submits a decision, the application first updates the database with the human decision and then uses HITL REST API to resolve the corresponding review task in Airflow.

This keeps the dashboard and Airflow’s Required Actions interface consistent, preventing orphaned pending tasks and ensuring both systems reflect the same review outcome.

## How to run locally

### Prerequisites

Make sure both Docker Desktop and Astro CLI are installed on your machine:

- [Docker](https://www.docker.com/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

### Start the Application

Clone the repository and start the local Airflow environment:

```bash
cd ach-fraud-detect
astro dev start
```

The repository already includes the seed ACH payment training dataset at [`include/data/ach_payments.csv`](include/data/ach_payments.csv).

Once the environment is running, open the **Airflow UI** at the URL printed by Astro. The application is designed to initialize and begin processing automatically.

**1. Monitor the DAGs in Airflow UI**

The `fraud_bootstrap` DAG runs the one-time initialization steps, including:

- Initializing the SQLite database
- Training the fraud detection model

After it completes, the following files should be created:

- [`include/data/ach_fraud.db`](include/data/ach_fraud.db)
- [`include/models/ach_fraud_model.joblib`](include/models/ach_fraud_model.joblib)

The `fraud_stream` DAG then begins generating and scoring batches of synthetic ACH payments every **2 minutes**.

**2. Open the ACH Fraud Dashboard link in Airflow UI**

Locate the left-side menu bar in Airflow UI. Click on:

```text
Browse → ACH Fraud Dashboard
```

This step will bring up ACH Payment Fraud Detection Dashboard which is the web interface for users to monitor transactions, view flagged payments, and submit review decisions.

**3. Stop Application **

When done viewing the web dashboard, you can stop the Airflow processes by running:

```bash
astro dev stop
```

## Troubleshooting

If the DAGs fail due to database corruption or stale local state, remove the generated database and ML model files:

```bash
rm include/data/ach_fraud.db
rm include/models/ach_fraud_model.joblib
```

Then reset the local Airflow environment:

```bash
astro dev kill
astro dev start
```

The `fraud_bootstrap` DAG will recreate the database and retrain the ML model.

## Project structure

| Path                                                                   | Purpose                                                             |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------- |
| [`dags/fraud_bootstrap.py`](dags/fraud_bootstrap.py)                   | One-time database initialization and model training                 |
| [`dags/fraud_stream.py`](dags/fraud_stream.py)                         | Recurring payment generation, scoring, explanation, and persistence |
| [`dags/fraud_hitl_review.py`](dags/fraud_hitl_review.py)               | Asset-triggered human review workflow                               |
| [`include/fraud_utils/generator.py`](include/fraud_utils/generator.py) | Synthetic training and live payment generation                      |
| [`include/fraud_utils/features.py`](include/fraud_utils/features.py)   | Shared feature engineering                                          |
| [`include/fraud_utils/reasons.py`](include/fraud_utils/reasons.py)     | Explainable fraud-risk reasons                                      |
| [`include/fraud_utils/db.py`](include/fraud_utils/db.py)               | Database schema, queries, and review persistence                    |
| [`plugins/fraud_dashboard.py`](plugins/fraud_dashboard.py)             | FastAPI dashboard registration and API endpoints                    |
| [`plugins/fraud_dashboard.html`](plugins/fraud_dashboard.html)         | Dashboard UI                                                        |

## Future improvements

The current model is trained once during the bootstrap phase and then used to score incoming transactions. A key next step would be to create a **continuous model retraining loop**.

As transactions are reviewed by humans, their decisions could become new labeled training data. Periodically retraining the ML model on this newly reviewed data would allow it to learn from emerging fraud patterns and reduce reliance on a static model.
