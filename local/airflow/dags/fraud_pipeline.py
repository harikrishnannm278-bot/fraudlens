"""
FraudLens Pipeline DAG
Runs every hour: data quality check → refresh analytics → retrain if needed
"""

from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import psycopg2
import logging

log = logging.getLogger(__name__)

DB_CONN = "dbname=fraud_db user=fraud_user password=fraud_pass host=postgres port=5432"

# ── Task Functions ────────────────────────────────────────────────


def check_data_quality(**context):
    """Verify new data has arrived since last run."""
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM transactions_enriched
        WHERE created_at >= NOW() - INTERVAL '1 hour'
    """)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    log.info(f"New rows in last hour: {count}")
    if count == 0:
        log.warning("No new transactions in last hour — skipping this run.")
    return 0

    context["ti"].xcom_push(key="new_rows", value=count)
    return count


def refresh_daily_summary(**context):
    """Rebuild the daily fraud summary table."""
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fraud_daily_summary (
            summary_date        DATE PRIMARY KEY,
            total_transactions  INT,
            fraud_count         INT,
            fraud_rate_pct      DECIMAL(6,2),
            total_amount_inr    DECIMAL(15,2),
            fraud_amount_inr    DECIMAL(15,2),
            updated_at          TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        INSERT INTO fraud_daily_summary
            (summary_date, total_transactions, fraud_count,
             fraud_rate_pct, total_amount_inr, fraud_amount_inr)
        SELECT
            CURRENT_DATE,
            COUNT(*),
            SUM(is_fraud),
            ROUND(SUM(is_fraud)::numeric / NULLIF(COUNT(*),0) * 100, 2),
            ROUND(SUM(amount_inr)::numeric, 2),
            ROUND(SUM(CASE WHEN is_fraud=1 THEN amount_inr ELSE 0 END)::numeric, 2)
        FROM transactions_enriched
        WHERE DATE(created_at) = CURRENT_DATE
        ON CONFLICT (summary_date) DO UPDATE SET
            total_transactions = EXCLUDED.total_transactions,
            fraud_count        = EXCLUDED.fraud_count,
            fraud_rate_pct     = EXCLUDED.fraud_rate_pct,
            total_amount_inr   = EXCLUDED.total_amount_inr,
            fraud_amount_inr   = EXCLUDED.fraud_amount_inr,
            updated_at         = NOW()
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("Daily summary refreshed successfully")


def check_if_retrain_needed(**context):
    """Retrain if more than 10,000 new rows since last check."""
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions_enriched")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()

    if total > 0 and total % 10000 < 500:
        log.info(f"Retraining triggered at {total} rows")
        return "retrain_model"
    else:
        log.info(f"No retraining needed ({total} rows)")
        return "skip_retrain"


def retrain_model(**context):
    """Trigger model retraining inside Spark container."""
    import subprocess
    result = subprocess.run(
        ["docker", "exec", "spark", "python3", "/train_model.py"],
        capture_output=True, text=True
    )
    log.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Retraining failed: {result.stderr}")
    log.info("Model retrained successfully")


# ── DAG Definition ────────────────────────────────────────────────

default_args = {
    "owner":            "fraudlens-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="fraudlens_pipeline",
    default_args=default_args,
    description="Hourly fraud pipeline: quality check → analytics → optional retrain",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraudlens", "production", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")

    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
    )

    refresh_summary = PythonOperator(
        task_id="refresh_daily_summary",
        python_callable=refresh_daily_summary,
    )

    should_retrain = BranchPythonOperator(
        task_id="should_retrain",
        python_callable=check_if_retrain_needed,
    )

    retrain = PythonOperator(
        task_id="retrain_model",
        python_callable=retrain_model,
    )

    skip_retrain = EmptyOperator(task_id="skip_retrain")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # ── Pipeline Flow ──────────────────────────────────────────
    start >> quality_check >> refresh_summary >> should_retrain
    should_retrain >> retrain >> end
    should_retrain >> skip_retrain >> end
