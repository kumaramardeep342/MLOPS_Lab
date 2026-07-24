"""
Apache Airflow DAG - DA5402W Assignment 1, Part C.

Orchestrates a sensor-data processing workflow sourced from Kafka:
  DataIngestion -> DataValidation -> [branch]
      -> (valid)   preprocessing_group(DataPreprocessing -> feature_engineering_group
                    (parallel: time features / statistical features) -> FeatureEngineering)
                    -> Analytics -> ReportGeneration
      -> (invalid) HandleInvalidData -> ReportGeneration

DataIngestion consumes the sensor readings published by producer.py to the
Kafka topic instead of generating synthetic data in-process.

Requires: pandas, kafka-python.
"""

import json
import os
from datetime import datetime, timedelta

import pandas as pd
from kafka import KafkaConsumer

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

#DATA_DIR = "/storage/data/datasets" # if scratch does not work then use this
DATA_DIR = "/storage/scratch" 
NUM_SENSORS = 10
INVALID_FRACTION_THRESHOLD = 0.20

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "sensor_1"
KAFKA_GROUP_ID = "grpA"
KAFKA_CONSUMER_TIMEOUT_MS = 10000  # stop pulling once idle this long
MAX_RECORDS_PER_RUN = 500
RAW_COLUMNS = ["sensor_id", "timestamp", "temperature", "humidity", "status"]

default_args = {
    "owner": "da5402w",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


def _path(name, run_id):
    safe_run_id = run_id.replace(":", "_").replace("+", "_")
    return os.path.join(DATA_DIR, f"{name}_{safe_run_id}.csv")


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def fetch_data_from_kafka(**context):
    """Pull the sensor readings published by producer.py off the Kafka topic."""
    os.makedirs(DATA_DIR, exist_ok=True)
    run_id = context["run_id"]

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS,
        api_version=(3, 5, 0),
    )

    rows = []
    try:
        for message in consumer:
            rows.append(message.value)
            if len(rows) >= MAX_RECORDS_PER_RUN:
                break
    finally:
        consumer.close()

    df = pd.DataFrame(rows, columns=RAW_COLUMNS) if rows else pd.DataFrame(columns=RAW_COLUMNS)

    raw_path = _path("raw", run_id)
    df.to_csv(raw_path, index=False)
    context["ti"].xcom_push(key="raw_path", value=raw_path)
    print(f"Fetched {len(rows)} records from Kafka topic '{KAFKA_TOPIC}' -> {raw_path}")


def validate_data(**context):
    ti = context["ti"]
    raw_path = ti.xcom_pull(key="raw_path", task_ids="data_ingestion")
    df = pd.read_csv(raw_path)

    n_total = len(df)
    n_invalid_temp = df["temperature"].isna().sum() + (
        (df["temperature"] < -20) | (df["temperature"] > 100)
    ).sum()
    invalid_fraction = n_invalid_temp / n_total if n_total else 1.0

    status = "valid" if invalid_fraction <= INVALID_FRACTION_THRESHOLD else "invalid"

    ti.xcom_push(key="validation_status", value=status)
    ti.xcom_push(key="invalid_fraction", value=invalid_fraction)
    print(f"Validation: {n_total} rows, invalid_fraction={invalid_fraction:.2%} -> {status}")


def branch_on_validation(**context):
    status = context["ti"].xcom_pull(key="validation_status", task_ids="data_validation")
    if status == "valid":
        return "preprocessing_group.data_preprocessing"
    return "handle_invalid_data"


def handle_invalid_data(**context):
    ti = context["ti"]
    invalid_fraction = ti.xcom_pull(key="invalid_fraction", task_ids="data_validation")
    print(f"Skipping processing: invalid_fraction={invalid_fraction:.2%} exceeds threshold")
    ti.xcom_push(key="status_message", value="Run skipped: too many invalid records")


def preprocess_data(**context):
    ti = context["ti"]
    raw_path = ti.xcom_pull(key="raw_path", task_ids="data_ingestion")
    df = pd.read_csv(raw_path)

    mean_temp = df["temperature"].mean()
    df["temperature"] = df["temperature"].fillna(mean_temp)
    df = df[(df["temperature"] >= -20) & (df["temperature"] <= 100)]
    df = df.drop_duplicates(subset=["sensor_id", "timestamp"])

    clean_path = _path("clean", context["run_id"])
    df.to_csv(clean_path, index=False)
    ti.xcom_push(key="clean_path", value=clean_path)
    print(f"Preprocessed {len(df)} rows -> {clean_path}")


def engineer_time_features(**context):
    ti = context["ti"]
    clean_path = ti.xcom_pull(key="clean_path", task_ids="preprocessing_group.data_preprocessing")
    df = pd.read_csv(clean_path, parse_dates=["timestamp"])

    df["hour_of_day"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

    out_path = _path("time_features", context["run_id"])
    df.to_csv(out_path, index=False)
    ti.xcom_push(key="time_features_path", value=out_path)
    print(f"Computed time features -> {out_path}")


def engineer_statistical_features(**context):
    ti = context["ti"]
    clean_path = ti.xcom_pull(key="clean_path", task_ids="preprocessing_group.data_preprocessing")
    df = pd.read_csv(clean_path)

    stats = df.groupby("sensor_id")["temperature"].agg(["mean", "std"]).reset_index()
    stats.columns = ["sensor_id", "sensor_mean_temp", "sensor_std_temp"]

    out_path = _path("stat_features", context["run_id"])
    stats.to_csv(out_path, index=False)
    ti.xcom_push(key="stat_features_path", value=out_path)
    print(f"Computed statistical features -> {out_path}")


def merge_features(**context):
    ti = context["ti"]
    group = "preprocessing_group.feature_engineering_group"
    time_path = ti.xcom_pull(key="time_features_path", task_ids=f"{group}.engineer_time_features")
    stat_path = ti.xcom_pull(key="stat_features_path", task_ids=f"{group}.engineer_statistical_features")

    time_df = pd.read_csv(time_path)
    stat_df = pd.read_csv(stat_path)
    merged = time_df.merge(stat_df, on="sensor_id", how="left")

    out_path = _path("engineered", context["run_id"])
    merged.to_csv(out_path, index=False)
    context["ti"].xcom_push(key="engineered_path", value=out_path)
    print(f"Merged engineered features -> {out_path}")


def run_analytics(**context):
    ti = context["ti"]
    engineered_path = ti.xcom_pull(
        key="engineered_path", task_ids="preprocessing_group.feature_engineering"
    )
    df = pd.read_csv(engineered_path)

    summary = {
        "avg_temperature_per_sensor": df.groupby("sensor_id")["temperature"].mean().to_dict(),
        "max_temperature_per_sensor": df.groupby("sensor_id")["temperature"].max().to_dict(),
        "active_sensor_count": int(df[df["status"] == "active"]["sensor_id"].nunique()),
        "status_distribution": df["status"].value_counts().to_dict(),
    }

    out_path = _path("analytics", context["run_id"]).replace(".csv", ".json")
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    ti.xcom_push(key="analytics_path", value=out_path)
    print(f"Analytics summary -> {out_path}")


def generate_report(**context):
    ti = context["ti"]
    validation_status = ti.xcom_pull(key="validation_status", task_ids="data_validation")
    report_lines = [f"DA5402W Workflow Report - run_id={context['run_id']}",
                     f"Validation status: {validation_status}"]

    if validation_status == "valid":
        analytics_path = ti.xcom_pull(key="analytics_path", task_ids="analytics")
        with open(analytics_path) as f:
            summary = json.load(f)
        report_lines.append("Analytics summary:")
        report_lines.append(json.dumps(summary, indent=2))
    else:
        msg = ti.xcom_pull(key="status_message", task_ids="handle_invalid_data")
        report_lines.append(msg or "No analytics produced (invalid data run).")

    out_path = _path("report", context["run_id"]).replace(".csv", ".txt")
    with open(out_path, "w") as f:
        f.write("\n".join(report_lines))
    print(f"Report written -> {out_path}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="da5402w_sensor_workflow_test",
    description="DA5402W Assignment 1 - Part C: workflow orchestration",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["da5402w", "assignment1"],
) as dag:

    data_ingestion = PythonOperator(
        task_id="data_ingestion",
        python_callable=fetch_data_from_kafka,
        priority_weight=5,
    )

    data_validation = PythonOperator(
        task_id="data_validation",
        python_callable=validate_data,
        priority_weight=10,
    )

    branch_validation = BranchPythonOperator(
        task_id="branch_validation",
        python_callable=branch_on_validation,
    )

    handle_invalid_data = PythonOperator(
        task_id="handle_invalid_data",
        python_callable=handle_invalid_data,
    )

    with TaskGroup(group_id="preprocessing_group") as preprocessing_group:

        data_preprocessing = PythonOperator(
            task_id="data_preprocessing",
            python_callable=preprocess_data,
            priority_weight=8,
        )

        with TaskGroup(group_id="feature_engineering_group") as feature_engineering_group:
            time_features = PythonOperator(
                task_id="engineer_time_features",
                python_callable=engineer_time_features,
            )
            stat_features = PythonOperator(
                task_id="engineer_statistical_features",
                python_callable=engineer_statistical_features,
            )

        feature_engineering = PythonOperator(
            task_id="feature_engineering",
            python_callable=merge_features,
        )

        data_preprocessing >> feature_engineering_group >> feature_engineering

    analytics = PythonOperator(
        task_id="analytics",
        python_callable=run_analytics,
        priority_weight=10,
    )

    report_generation = PythonOperator(
        task_id="report_generation",
        python_callable=generate_report,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        priority_weight=5,
    )

    data_ingestion >> data_validation >> branch_validation
    branch_validation >> preprocessing_group >> analytics >> report_generation
    branch_validation >> handle_invalid_data >> report_generation
