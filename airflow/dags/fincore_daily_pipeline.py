from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

import os
import boto3

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from botocore.client import Config


LOGGER = logging.getLogger(__name__)

DAG_ID = "fincore_daily_pipeline"

RAW_BUCKET = "fincore-raw"
PROCESSED_BUCKET = "fincore-processed"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_REGION = os.getenv("MINIO_REGION")

AWS_CONN_ID = "minio_s3"

TRADE_REJECT_RATE_THRESHOLD = 0.02
PNL_UNRESOLVABLE_THRESHOLD = 100


def failure_callback(context: dict[str, Any]) -> None:
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    LOGGER.error(
        "FinCore task failed: dag_id=%s task_id=%s run_id=%s exception=%s",
        context.get("dag").dag_id if context.get("dag") else None,
        task_instance.task_id if task_instance else None,
        context.get("run_id"),
        exception,
    )


def sla_miss_callback(
    dag: DAG,
    task_list: str,
    blocking_task_list: str,
    slas: list[Any],
    blocking_tis: list[Any],
) -> None:
    LOGGER.error(
        "FinCore SLA missed: dag_id=%s tasks=%s blocking_tasks=%s",
        dag.dag_id,
        task_list,
        blocking_task_list,
    )


def get_minio_client() -> Any:
    """
    Create a boto3 client using the MinIO credentials available
    inside the Airflow container.
    """
   

    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise AirflowException(
            "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are not available."
        )

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=MINIO_REGION,
        config=Config(
            signature_version="s3v4",
            s3={
                "addressing_style": "path",
            },
        ),
    )


def resolve_run_date(**context: Any) -> str:
    """
    Use dag_run.conf.run_date for manual runs when provided.
    Otherwise, use Airflow's logical date.
    """
    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf:
        configured_run_date = dag_run.conf.get("run_date")

        if configured_run_date:
            datetime.strptime(
                configured_run_date,
                "%Y-%m-%d",
            )
            return configured_run_date

    logical_date = context["logical_date"]
    return logical_date.strftime("%Y-%m-%d")


def validate_source_partition(
    *,
    bucket: str,
    prefix_template: str,
    source_name: str,
    **context: Any,
) -> dict[str, Any]:
    task_instance = context["ti"]

    run_date = task_instance.xcom_pull(
        task_ids="resolve_run_date",
    )

    prefix = prefix_template.format(run_date=run_date)

    client = get_minio_client()

    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    objects = [
        item
        for item in response.get("Contents", [])
        if not item["Key"].endswith("/")
    ]

    if not objects:
        raise AirflowException(
            f"No source objects found for {source_name}: "
            f"s3://{bucket}/{prefix}"
        )

    total_size_bytes = sum(
        int(item.get("Size", 0))
        for item in objects
    )

    result = {
        "source_name": source_name,
        "bucket": bucket,
        "prefix": prefix,
        "object_count": len(objects),
        "total_size_bytes": total_size_bytes,
        "objects": [
            {
                "key": item["Key"],
                "size_bytes": int(item.get("Size", 0)),
            }
            for item in objects
        ],
    }

    LOGGER.info(
        "Validated source partition: %s",
        json.dumps(result),
    )

    return result


def verify_all_extracts(**context: Any) -> None:
    task_instance = context["ti"]

    task_ids = [
        "extract_trades",
        "extract_market_data",
        "extract_portfolio",
    ]

    pulled_results = task_instance.xcom_pull(
        task_ids=task_ids,
    )

    # Airflow may return LazyXComSelectSequence.
    # Materialize it before validation or JSON serialization.
    results = list(pulled_results or [])

    if len(results) != len(task_ids):
        raise AirflowException(
            "One or more source extraction results are missing. "
            f"Expected {len(task_ids)}, received {len(results)}."
        )

    invalid_sources = []

    for task_id, result in zip(task_ids, results):
        if not isinstance(result, dict):
            invalid_sources.append(
                f"{task_id}: invalid XCom result {result!r}"
            )
            continue

        if int(result.get("object_count", 0)) < 1:
            invalid_sources.append(
                result.get("source_name", task_id)
            )

    if invalid_sources:
        raise AirflowException(
            "Source extraction validation failed for: "
            + ", ".join(invalid_sources)
        )

    LOGGER.info(
        "All source partitions validated successfully: %s",
        json.dumps(results, default=str),
    )

def read_metrics(**context: Any) -> dict[str, Any]:
    task_instance = context["ti"]

    run_date = task_instance.xcom_pull(
        task_ids="resolve_run_date",
    )

    prefix = f"metrics/dt={run_date}/"

    client = get_minio_client()

    response = client.list_objects_v2(
        Bucket=PROCESSED_BUCKET,
        Prefix=prefix,
    )

    metric_objects = [
        item["Key"]
        for item in response.get("Contents", [])
        if not item["Key"].endswith("/")
        and "part-" in item["Key"]
    ]

    if not metric_objects:
        raise AirflowException(
            "No Spark metrics output found at "
            f"s3://{PROCESSED_BUCKET}/{prefix}"
        )

    metric_key = sorted(metric_objects)[0]

    response = client.get_object(
        Bucket=PROCESSED_BUCKET,
        Key=metric_key,
    )

    payload = response["Body"].read().decode("utf-8").strip()

    try:
        metrics = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise AirflowException(
            f"Invalid metrics JSON in {metric_key}: {payload}"
        ) from exc

    LOGGER.info(
        "Loaded Spark metrics: %s",
        json.dumps(metrics, indent=2),
    )

    return metrics


def choose_quality_gate_branch(**context: Any) -> str:
    task_instance = context["ti"]

    metrics = task_instance.xcom_pull(
        task_ids="read_metrics",
    )

    if not metrics:
        raise AirflowException(
            "Metrics are unavailable for quality-gate evaluation."
        )

    quality_gate = metrics.get("quality_gate", {})

    reject_rate = float(
        quality_gate.get(
            "trade_reject_rate",
            metrics.get("reject_rate", 1.0),
        )
    )

    unresolvable_count = int(
        quality_gate.get(
            "pnl_unresolvable_count",
            metrics.get("pnl_unresolvable_count", 0),
        )
    )

    gate_passed = (
        reject_rate < TRADE_REJECT_RATE_THRESHOLD
        and unresolvable_count < PNL_UNRESOLVABLE_THRESHOLD
    )

    LOGGER.info(
        "Quality gate result: reject_rate=%s threshold=%s "
        "unresolvable_count=%s threshold=%s passed=%s",
        reject_rate,
        TRADE_REJECT_RATE_THRESHOLD,
        unresolvable_count,
        PNL_UNRESOLVABLE_THRESHOLD,
        gate_passed,
    )

    if gate_passed:
        return "quality_gate_passed"

    return "quality_gate_failed"


def quality_failure(**context: Any) -> None:
    task_instance = context["ti"]

    metrics = task_instance.xcom_pull(
        task_ids="read_metrics",
    )

    raise AirflowException(
        "FinCore quality gate failed. Warehouse loading has been "
        f"blocked. Metrics: {json.dumps(metrics)}"
    )


default_args = {
    "owner": "fincore-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback,
}


with DAG(
    dag_id=DAG_ID,
    description=(
        "Daily FinCore multi-source financial data pipeline."
    ),
    start_date=datetime(2026, 8, 1),
    schedule="30 0 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=6),
    sla_miss_callback=sla_miss_callback,
    tags=[
        "fincore",
        "financial-data",
        "pyspark",
        "minio",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    resolve_date = PythonOperator(
        task_id="resolve_run_date",
        python_callable=resolve_run_date,
    )

    wait_for_trades = S3KeySensor(
        task_id="wait_for_trades",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=RAW_BUCKET,
        bucket_key=(
            "trades/dt={{ "
            "ti.xcom_pull(task_ids='resolve_run_date') "
            "}}/*"
        ),
        wildcard_match=True,
        poke_interval=600,
        timeout=4 * 60 * 60,
        mode="reschedule",
    )

    wait_for_market_data = S3KeySensor(
        task_id="wait_for_market_data",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=RAW_BUCKET,
        bucket_key=(
            "market_data/dt={{ "
            "ti.xcom_pull(task_ids='resolve_run_date') "
            "}}/*"
        ),
        wildcard_match=True,
        poke_interval=600,
        timeout=4 * 60 * 60,
        mode="reschedule",
    )

    wait_for_portfolio = S3KeySensor(
        task_id="wait_for_portfolio",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=RAW_BUCKET,
        bucket_key=(
            "portfolio/dt={{ "
            "ti.xcom_pull(task_ids='resolve_run_date') "
            "}}/*"
        ),
        wildcard_match=True,
        poke_interval=600,
        timeout=4 * 60 * 60,
        mode="reschedule",
    )

    extract_trades = PythonOperator(
        task_id="extract_trades",
        python_callable=validate_source_partition,
        op_kwargs={
            "bucket": RAW_BUCKET,
            "prefix_template": "trades/dt={run_date}/",
            "source_name": "trades",
        },
    )

    extract_market_data = PythonOperator(
        task_id="extract_market_data",
        python_callable=validate_source_partition,
        op_kwargs={
            "bucket": RAW_BUCKET,
            "prefix_template": "market_data/dt={run_date}/",
            "source_name": "market_data",
        },
    )

    extract_portfolio = PythonOperator(
        task_id="extract_portfolio",
        python_callable=validate_source_partition,
        op_kwargs={
            "bucket": RAW_BUCKET,
            "prefix_template": "portfolio/dt={run_date}/",
            "source_name": "portfolio",
        },
    )

    join_extracts = PythonOperator(
        task_id="join_extracts",
        python_callable=verify_all_extracts,
    )

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
        set -euo pipefail

        RUN_DATE="{{ ti.xcom_pull(task_ids='resolve_run_date') }}"

        docker exec fincore-spark-master-1 \
          /bin/bash -lc "
            /opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages org.apache.hadoop:hadoop-aws:3.4.2 \
              /opt/fincore/spark/etl_job.py \
              --run-date ${RUN_DATE} \
              --s3-endpoint http://minio:9000 \
              --s3-access-key \\"${MINIO_ROOT_USER}\\" \
              --s3-secret-key \\"${MINIO_ROOT_PASSWORD}\\"
          "
        """,
        env={
            "MINIO_ROOT_USER": "{{ var.value.minio_root_user }}",
            "MINIO_ROOT_PASSWORD": (
                "{{ var.value.minio_root_password }}"
            ),
        },
        append_env=True,
        retries=3,
        retry_delay=timedelta(minutes=15),
        execution_timeout=timedelta(hours=2),
    )

    read_spark_metrics = PythonOperator(
        task_id="read_metrics",
        python_callable=read_metrics,
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=choose_quality_gate_branch,
    )

    quality_gate_passed = EmptyOperator(
        task_id="quality_gate_passed",
    )

    quality_gate_failed = PythonOperator(
        task_id="quality_gate_failed",
        python_callable=quality_failure,
    )

    start >> resolve_date

    resolve_date >> [
        wait_for_trades,
        wait_for_market_data,
        wait_for_portfolio,
    ]

    wait_for_trades >> extract_trades
    wait_for_market_data >> extract_market_data
    wait_for_portfolio >> extract_portfolio

    [
        extract_trades,
        extract_market_data,
        extract_portfolio,
    ] >> join_extracts

    join_extracts >> run_spark_etl
    run_spark_etl >> read_spark_metrics
    read_spark_metrics >> quality_gate

    quality_gate >> [
        quality_gate_passed,
        quality_gate_failed,
    ]