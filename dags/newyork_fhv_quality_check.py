import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

BRONZE_PATH = "/opt/airflow/data/bronze"
INPUT_FILE = "newyork_fhv_base_aggregate_raw.json"

# Required columns in FHV Base Aggregate dataset
REQUIRED_COLUMNS = [
    "base_number",
    "base_name",
    "year",
    "month",
    "total_dispatched_trips",
    "unique_dispatched_vehicles",
]

MIN_ROW_COUNT = 10


def check_null_values(**context):
    """Check for null values in required columns."""
    input_path = os.path.join(BRONZE_PATH, INPUT_FILE)
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    null_report = {}
    for col in REQUIRED_COLUMNS:
        null_count = sum(1 for row in data if not row.get(col))
        null_report[col] = null_count
        if null_count > 0:
            print(f"[QC] WARNING: Column '{col}' has {null_count} null values")

    print(f"[QC] Null check report: {null_report}")
    context["ti"].xcom_push(key="null_report", value=null_report)
    return null_report


def check_duplicates(**context):
    """Check for duplicate records based on base_number + year + month."""
    input_path = os.path.join(BRONZE_PATH, INPUT_FILE)
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    keys = [(row.get("base_number"), row.get("year"), row.get("month")) for row in data]
    unique_keys = set(keys)
    duplicate_count = len(keys) - len(unique_keys)

    print(f"[QC] Total records   : {len(data)}")
    print(f"[QC] Unique keys     : {len(unique_keys)}")
    print(f"[QC] Duplicates found: {duplicate_count}")

    context["ti"].xcom_push(key="duplicate_count", value=duplicate_count)
    return duplicate_count


def check_row_count(**context):
    """Check that row count meets minimum threshold."""
    input_path = os.path.join(BRONZE_PATH, INPUT_FILE)
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    row_count = len(data)
    print(f"[QC] Row count: {row_count} (minimum required: {MIN_ROW_COUNT})")
    context["ti"].xcom_push(key="row_count", value=row_count)
    return row_count


def evaluate_quality(**context):
    """Evaluate all QC results and branch accordingly."""
    ti = context["ti"]
    null_report = ti.xcom_pull(task_ids="check_null_values", key="null_report")
    duplicate_count = ti.xcom_pull(task_ids="check_duplicates", key="duplicate_count")
    row_count = ti.xcom_pull(task_ids="check_row_count", key="row_count")

    issues = []

    # Check nulls in critical columns
    critical_cols = ["base_number", "year", "month", "total_dispatched_trips"]
    for col in critical_cols:
        if null_report.get(col, 0) > 0:
            issues.append(f"Null values in critical column: {col}")

    if duplicate_count > 0:
        issues.append(f"Found {duplicate_count} duplicate records")

    if row_count < MIN_ROW_COUNT:
        issues.append(f"Row count {row_count} below minimum {MIN_ROW_COUNT}")

    if issues:
        print(f"[QC] FAILED ❌ Issues found:")
        for issue in issues:
            print(f"  - {issue}")
        return "qc_failed"
    else:
        print("[QC] PASSED ✅ All checks passed")
        return "qc_passed"


with DAG(
    dag_id="newyork_fhv_quality_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Data Quality Check for FHV Base Aggregate Bronze Layer",
    tags=["newyork", "fhv", "quality", "dq"],
) as dag:

    check_nulls = PythonOperator(
        task_id="check_null_values",
        python_callable=check_null_values,
    )

    check_dupes = PythonOperator(
        task_id="check_duplicates",
        python_callable=check_duplicates,
    )

    check_rows = PythonOperator(
        task_id="check_row_count",
        python_callable=check_row_count,
    )

    evaluate = BranchPythonOperator(
        task_id="evaluate_quality",
        python_callable=evaluate_quality,
    )

    qc_passed = EmptyOperator(task_id="qc_passed")
    qc_failed = EmptyOperator(task_id="qc_failed")

    [check_nulls, check_dupes, check_rows] >> evaluate >> [qc_passed, qc_failed]