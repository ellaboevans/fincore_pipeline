from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

import os
import boto3
import io
import csv

from contextlib import closing
from urllib.parse import urlparse

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import psycopg2

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from botocore.client import Config



LOGGER = logging.getLogger(__name__)

DAG_ID = "fincore_daily_pipeline"

RAW_BUCKET = "fincore-raw"
PROCESSED_BUCKET = "fincore-processed"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_REGION = os.getenv("MINIO_REGION")

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000",
)

MINIO_REGION = os.getenv(
    "MINIO_REGION",
    "us-east-1",
)

WAREHOUSE_HOST = os.getenv(
    "WAREHOUSE_HOST",
    "warehouse-db",
)

WAREHOUSE_DB = os.getenv(
    "WAREHOUSE_DB",
    "fincore_warehouse",
)

WAREHOUSE_USER = os.getenv(
    "WAREHOUSE_USER",
    "fincore_user",
)

WAREHOUSE_PASSWORD = os.getenv(
    "WAREHOUSE_PASSWORD",
)

WAREHOUSE_PORT = int(os.getenv("WAREHOUSE_PORT", "5432"))

AWS_CONN_ID = "minio_s3"

TRADE_REJECT_RATE_THRESHOLD = 0.02
PNL_UNRESOLVABLE_THRESHOLD = 100

WAREHOUSE_LOADS = {
    "processed_trades": {
        "source_prefix": "processed/trades",
        "target_table": "raw.processed_trades",
        "partition_column": "trade_date",
        "columns": [
            "order_id",
            "portfolio_id",
            "symbol",
            "side",
            "quantity",
            "trade_price",
            "average_cost",
            "currency",
            "transact_time",
            "trade_date",
            "close_price",
            "market_currency",
            "fx_rate_to_usd",
            "realized_pnl_local",
            "realized_pnl_usd",
            "pnl_unresolvable",
        ],
    },
    "portfolio_pnl": {
        "source_prefix": "processed/portfolio_pnl",
        "target_table": "raw.portfolio_pnl",
        "partition_column": "pnl_date",
        "columns": [
            "portfolio_id",
            "portfolio_name",
            "pnl_date",
            "position_count",
            "total_market_value_usd",
            "total_unrealized_pnl_usd",
            "max_market_value_usd",
            "max_daily_loss_usd",
            "max_position_concentration_pct",
            "market_value_limit_breached",
            "daily_loss_limit_breached",
        ],
    },
    "rejected_records": {
        "source_prefix": "rejected",
        "target_table": "raw.rejected_records",
        "partition_column": "run_date",
        "columns": [
            "source_name",
            "run_date",
            "record_identifier",
            "raw_record",
            "rejection_rule",
            "rejection_reason",
            "rejected_at",
        ],
    },
    "unresolvable_pnl": {
        "source_prefix": "processed/unresolvable_pnl",
        "target_table": "raw.unresolvable_pnl",
        "partition_column": "trade_date",
        "columns": [
            "order_id",
            "portfolio_id",
            "symbol",
            "side",
            "quantity",
            "trade_price",
            "average_cost",
            "currency",
            "transact_time",
            "trade_date",
            "unresolvable_reason",
        ],
    },
}


def get_warehouse_connection():
    if not WAREHOUSE_PASSWORD:
        raise AirflowException(
            "WAREHOUSE_PASSWORD is not available in the airflow container."
        )
    
    return psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB,
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
        connect_timeout=15
    )
    
def get_pyarrow_s3_filesystem() -> pafs.S3FileSystem:
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    
    if not access_key or not secret_key:
        raise AirflowException(
            "MinIO credentials are not available."
        )
        
    endpoint = MINIO_ENDPOINT or "http://minio:9000"
    parsed_endpoint = urlparse(endpoint)
    
    endpoint_override = (
        parsed_endpoint.netloc
        if parsed_endpoint.scheme in ("http", "https")
        else endpoint
    )
    
    scheme = parsed_endpoint.scheme or "http"
    
    return pafs.S3FileSystem(
        endpoint_override=endpoint_override,
        access_key=access_key,
        secret_key=secret_key,
        region=MINIO_REGION,
        scheme=scheme
    )


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

def read_parquet_partition(
    *,
    source_prefix: str,
    run_date: str,
) -> pd.DataFrame:
    filesystem = get_pyarrow_s3_filesystem()

    partition_path = (
        f"{PROCESSED_BUCKET}/"
        f"{source_prefix}/"
        f"dt={run_date}"
    )

    LOGGER.info(
        "Reading Parquet partition from s3://%s",
        partition_path,
    )

    try:
        dataset = ds.dataset(
            partition_path,
            filesystem=filesystem,
            format="parquet",
        )

        table = dataset.to_table()

    except Exception as exc:
        raise AirflowException(
            f"Unable to read Parquet partition "
            f"s3://{partition_path}: {exc}"
        ) from exc

    dataframe = table.to_pandas()

    LOGGER.info(
        "Read %s rows from s3://%s",
        len(dataframe),
        partition_path,
    )

    return dataframe

def prepare_dataframe_for_copy(
    dataframe: pd.DataFrame,
    expected_columns: list[str],
) -> pd.DataFrame:
    missing_columns = [
        column
        for column in expected_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise AirflowException(
            "Parquet output is missing required columns: "
            + ", ".join(missing_columns)
        )

    prepared = dataframe.loc[:, expected_columns].copy()

    # PostgreSQL COPY accepts empty fields as NULL when configured.
    prepared = prepared.astype(object)
    prepared = prepared.where(pd.notna(prepared), None)

    return prepared

def copy_dataframe_to_postgres(
    *,
    cursor,
    dataframe: pd.DataFrame,
    target_table: str,
    columns: list[str],
) -> int:
    if dataframe.empty:
        LOGGER.warning(
            "No rows available for %s; COPY will be skipped.",
            target_table,
        )
        return 0

    buffer = io.StringIO()

    dataframe.to_csv(
        buffer,
        index=False,
        header=False,
        quoting=csv.QUOTE_MINIMAL,
        na_rep="\\N",
        date_format="%Y-%m-%d %H:%M:%S.%f%z",
    )

    buffer.seek(0)

    column_sql = ", ".join(
        f'"{column}"'
        for column in columns
    )

    copy_sql = f"""
        COPY {target_table} ({column_sql})
        FROM STDIN
        WITH (
            FORMAT CSV,
            NULL '\\N',
            QUOTE '"',
            ESCAPE '"'
        )
    """

    cursor.copy_expert(
        sql=copy_sql,
        file=buffer,
    )

    return len(dataframe)

def start_load_audit(
    *,
    cursor,
    dag_id: str,
    airflow_run_id: str,
    run_date: str,
    target_table: str,
    source_path: str,
) -> int:
    cursor.execute(
        """
        INSERT INTO audit.pipeline_load_log (
            dag_id,
            airflow_run_id,
            run_date,
            target_table,
            source_path,
            load_status,
            started_at
        )
        VALUES (%s, %s, %s, %s, %s, 'RUNNING', NOW())
        RETURNING load_id
        """,
        (
            dag_id,
            airflow_run_id,
            run_date,
            target_table,
            source_path,
        ),
    )

    return int(cursor.fetchone()[0])


def complete_load_audit(
    *,
    cursor,
    load_id: int,
    rows_deleted: int,
    rows_inserted: int,
) -> None:
    cursor.execute(
        """
        UPDATE audit.pipeline_load_log
        SET
            rows_deleted = %s,
            rows_inserted = %s,
            load_status = 'SUCCESS',
            completed_at = NOW()
        WHERE load_id = %s
        """,
        (
            rows_deleted,
            rows_inserted,
            load_id,
        ),
    )


def record_failed_load_audit(
    *,
    dag_id: str,
    airflow_run_id: str,
    run_date: str,
    target_table: str,
    source_path: str,
    error_message: str,
) -> None:
    try:
        with closing(get_warehouse_connection()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO audit.pipeline_load_log (
                        dag_id,
                        airflow_run_id,
                        run_date,
                        target_table,
                        source_path,
                        rows_deleted,
                        rows_inserted,
                        load_status,
                        started_at,
                        completed_at,
                        error_message
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        0,
                        0,
                        'FAILED',
                        NOW(),
                        NOW(),
                        %s
                    )
                    """,
                    (
                        dag_id,
                        airflow_run_id,
                        run_date,
                        target_table,
                        source_path,
                        error_message[:5000],
                    ),
                )

            connection.commit()

    except Exception:
        LOGGER.exception(
            "Unable to record failed load audit for %s",
            target_table,
        )

def load_partition_to_warehouse(
    *,
    load_name: str,
    **context: Any,
) -> dict[str, Any]:
    if load_name not in WAREHOUSE_LOADS:
        raise AirflowException(
            f"Unknown warehouse load configuration: {load_name}"
        )

    config = WAREHOUSE_LOADS[load_name]

    task_instance = context["ti"]
    dag_run = context["dag_run"]

    run_date = task_instance.xcom_pull(
        task_ids="resolve_run_date",
    )

    target_table = config["target_table"]
    partition_column = config["partition_column"]
    expected_columns = config["columns"]
    source_prefix = config["source_prefix"]

    source_path = (
        f"s3://{PROCESSED_BUCKET}/"
        f"{source_prefix}/"
        f"dt={run_date}/"
    )

    dataframe = read_parquet_partition(
        source_prefix=source_prefix,
        run_date=run_date,
    )

    dataframe = prepare_dataframe_for_copy(
        dataframe,
        expected_columns,
    )

    load_id: int | None = None

    try:
        with closing(get_warehouse_connection()) as connection:
            connection.autocommit = False

            with connection.cursor() as cursor:
                load_id = start_load_audit(
                    cursor=cursor,
                    dag_id=context["dag"].dag_id,
                    airflow_run_id=dag_run.run_id,
                    run_date=run_date,
                    target_table=target_table,
                    source_path=source_path,
                )

                cursor.execute(
                    f"""
                    DELETE FROM {target_table}
                    WHERE {partition_column} = %s
                    """,
                    (run_date,),
                )

                rows_deleted = cursor.rowcount

                rows_inserted = copy_dataframe_to_postgres(
                    cursor=cursor,
                    dataframe=dataframe,
                    target_table=target_table,
                    columns=expected_columns,
                )

                complete_load_audit(
                    cursor=cursor,
                    load_id=load_id,
                    rows_deleted=rows_deleted,
                    rows_inserted=rows_inserted,
                )

            connection.commit()

        result = {
            "load_name": load_name,
            "target_table": target_table,
            "run_date": run_date,
            "rows_deleted": rows_deleted,
            "rows_inserted": rows_inserted,
            "source_path": source_path,
        }

        LOGGER.info(
            "Warehouse load completed: %s",
            json.dumps(result),
        )

        return result

    except Exception as exc:
        LOGGER.exception(
            "Warehouse load failed for %s",
            target_table,
        )

        record_failed_load_audit(
            dag_id=context["dag"].dag_id,
            airflow_run_id=dag_run.run_id,
            run_date=run_date,
            target_table=target_table,
            source_path=source_path,
            error_message=str(exc),
        )

        raise AirflowException(
            f"Warehouse load failed for {target_table}: {exc}"
        ) from exc
        
def verify_warehouse_loads(**context: Any) -> None:
    task_instance = context["ti"]

    task_ids = [
        "warehouse_load.load_processed_trades",
        "warehouse_load.load_portfolio_pnl",
        "warehouse_load.load_rejected_records",
        "warehouse_load.load_unresolvable_pnl",
    ]

    pulled_results = task_instance.xcom_pull(
        task_ids=task_ids,
    )

    results = list(pulled_results or [])

    if len(results) != len(task_ids):
        raise AirflowException(
            "One or more warehouse load results are missing."
        )

    invalid_results = [
        task_id
        for task_id, result in zip(task_ids, results)
        if not isinstance(result, dict)
    ]

    if invalid_results:
        raise AirflowException(
            "Invalid warehouse load XCom results for: "
            + ", ".join(invalid_results)
        )

    LOGGER.info(
        "All warehouse loads completed successfully: %s",
        json.dumps(results, default=str),
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
    
    with TaskGroup(
        group_id="warehouse_load",
        tooltip="Load processed data into the warehouse"
    ) as warehouse_load_group:
        
        load_processed_trades = PythonOperator(
        task_id="load_processed_trades",
        python_callable=load_partition_to_warehouse,
        op_kwargs={
            "load_name": "processed_trades",
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        )

        load_portfolio_pnl = PythonOperator(
            task_id="load_portfolio_pnl",
            python_callable=load_partition_to_warehouse,
            op_kwargs={
                "load_name": "portfolio_pnl",
            },
            retries=2,
            retry_delay=timedelta(minutes=5),
        )

        load_rejected_records = PythonOperator(
            task_id="load_rejected_records",
            python_callable=load_partition_to_warehouse,
            op_kwargs={
                "load_name": "rejected_records",
            },
            retries=2,
            retry_delay=timedelta(minutes=5),
        )

        load_unresolvable_pnl = PythonOperator(
            task_id="load_unresolvable_pnl",
            python_callable=load_partition_to_warehouse,
            op_kwargs={
                "load_name": "unresolvable_pnl",
            },
            retries=2,
            retry_delay=timedelta(minutes=5),
        )
    
    warehouse_load_complete = PythonOperator(
    task_id="warehouse_load_complete",
    python_callable=verify_warehouse_loads,
    trigger_rule=TriggerRule.ALL_SUCCESS,
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
    
    quality_gate_passed >> warehouse_load_group
    warehouse_load_group >> warehouse_load_complete