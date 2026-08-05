# FinCore Financial Data Pipeline Runbook

## 1. Purpose

This runbook provides operational guidance for the FinCore daily financial data pipeline.

The pipeline ingests three daily source feeds:

- FIX trade messages
- Market-data CSV files
- Portfolio-state JSON files

It validates and transforms the data using PySpark, applies data-quality checks, loads approved outputs into PostgreSQL, builds analytical models using dbt, and exposes operational metrics through Grafana.

---

## 2. Local Architecture

| Local service  | Production equivalent              |
| -------------- | ---------------------------------- |
| MinIO          | Amazon S3                          |
| Apache Spark   | AWS Glue                           |
| Apache Airflow | Amazon MWAA                        |
| PostgreSQL     | Amazon Redshift                    |
| Grafana        | Amazon CloudWatch Dashboard        |
| Docker Compose | Cloud infrastructure/orchestration |

Pipeline flow:

```text
MinIO source partitions
        |
        v
Airflow S3 sensors
        |
        v
Parallel source validation
        |
        v
PySpark ETL
        |
        v
Metrics JSON
        |
        v
Airflow quality gate
       / \
      /   \
 Passed   Failed
   |        |
   v        v
Warehouse  Alert and stop
loads
   |
   v
dbt run
   |
   v
dbt test
   |
   v
Pipeline complete

```

---

## 3. Service Endpoints

| Service         | URL / Connection        |
| --------------- | ----------------------- |
| Airflow         | `http://localhost:8080` |
| MinIO API       | `http://localhost:9000` |
| MinIO Console   | `http://localhost:9001` |
| Spark Master UI | `http://localhost:8081` |
| Spark Worker UI | `http://localhost:8082` |
| PostgreSQL      | `localhost:5433`        |
| Grafana         | `http://localhost:3000` |

---

## 4. Prerequisites

### Install

- Docker and Docker Compose
- Python 3.11 or later
- PostgreSQL client tools
- Git
- At least 8 GB of available memory
- Sufficient Docker disk space

### Verify

```bash
docker --version
docker compose version
python3 --version
psql --version

```

---

## 5. Environment Configuration

Copy the example environment file:

```bash
cp .env.example .env

```

Populate the required values:

```env
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
MINIO_ENDPOINT=
MINIO_REGION=

WAREHOUSE_DB=
WAREHOUSE_USER=
WAREHOUSE_PASSWORD=
WAREHOUSE_HOST=
WAREHOUSE_PORT=

AIRFLOW_DB=
AIRFLOW_DB_USER=
AIRFLOW_DB_PASSWORD=
AIRFLOW_FERNET_KEY=
AIRFLOW_SECRET_KEY=

AIRFLOW_ADMIN_USERNAME=
AIRFLOW_ADMIN_PASSWORD=
AIRFLOW_ADMIN_EMAIL=

```

Load the environment into the current shell:

```bash
set -a
source .env
set +a

```

Do not commit `.env`. Confirm it is ignored:

```bash
git check-ignore .env

```

---

## 6. Start the Platform

Build the containers:

```bash
docker compose build

```

Start the stack:

```bash
docker compose up -d

```

Check container status:

```bash
docker compose ps

```

Expected services include:

- `minio`
- `minio-init`
- `warehouse-db`
- `airflow-db`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-triggerer`
- `spark-master`
- `spark-worker`
- `grafana`

---

## 7. Stop the Platform

Stop containers without deleting data:

```bash
docker compose down

```

Stop and remove volumes:

```bash
docker compose down -v

```

> **Warning**: Removing volumes deletes MinIO objects, PostgreSQL data, Airflow metadata, and Grafana dashboards.

---

## 8. Platform Health Checks

### 8.1 MinIO

Check the container:

```bash
docker compose ps minio

```

List buckets:

```bash
docker compose run --rm \
  --entrypoint /bin/sh \
  minio-init \
  -c '
    mc alias set local http://minio:9000 \
      "$MINIO_ROOT_USER" \
      "$MINIO_ROOT_PASSWORD"

    mc ls local
  '

```

_Expected buckets:_ `fincore-raw`, `fincore-processed`

### 8.2 Spark

Check services:

```bash
docker compose ps spark-master spark-worker

```

Check Spark version:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit --version

```

_Expected Spark version:_ `Spark 4.1.2`

### 8.3 Airflow

Check services:

```bash
docker compose ps \
  airflow-webserver \
  airflow-scheduler \
  airflow-triggerer

```

Check DAG import errors:

```bash
docker compose exec airflow-webserver \
  airflow dags list-import-errors

```

_Expected:_ `No data found`

Confirm the DAG is available:

```bash
docker compose exec airflow-webserver \
  airflow dags list | grep fincore

```

### 8.4 PostgreSQL

Test the warehouse:

```bash
docker compose exec warehouse-db \
  pg_isready \
  -U "$WAREHOUSE_USER" \
  -d "$WAREHOUSE_DB"

```

List schemas:

```bash
docker compose exec warehouse-db \
  psql \
  -U "$WAREHOUSE_USER" \
  -d "$WAREHOUSE_DB" \
  -c "\dn"

```

### 8.5 dbt

Check dbt inside Airflow:

```bash
docker compose exec airflow-scheduler \
  dbt --version

```

Test the project:

```bash
docker compose exec airflow-scheduler \
  bash -lc '
    cd /opt/airflow/dbt
    dbt debug --profiles-dir .
  '

```

### 8.6 Grafana

Check the container:

```bash
docker compose ps grafana

```

Open: `http://localhost:3000`

---

## 9. Generate Sample Data

Generate data for a trading date:

```bash
python3 data-generator/generate_sample_data.py \
  --run-date 2026-08-03

```

Expected output structure:

```text
data/generated/
├── trades/dt=2026-08-03/
├── market_data/dt=2026-08-03/
├── portfolio/dt=2026-08-03/
└── manifest_2026-08-03.json

```

Inspect the files:

```bash
head -n 5 \
  data/generated/trades/dt=2026-08-03/trades.fix

cat \
  data/generated/market_data/dt=2026-08-03/equities.csv

```

---

## 10. Upload Source Data to MinIO

```bash
docker compose run --rm \
  -v "$PWD/data/generated:/data-import:ro" \
  --entrypoint /bin/sh \
  minio-init \
  -c '
    mc alias set local http://minio:9000 \
      "$MINIO_ROOT_USER" \
      "$MINIO_ROOT_PASSWORD"

    mc cp --recursive \
      /data-import/trades/ \
      local/fincore-raw/trades/

    mc cp --recursive \
      /data-import/market_data/ \
      local/fincore-raw/market_data/

    mc cp --recursive \
      /data-import/portfolio/ \
      local/fincore-raw/portfolio/
  '

```

Verify:

```bash
docker compose run --rm \
  --entrypoint /bin/sh \
  minio-init \
  -c '
    mc alias set local http://minio:9000 \
      "$MINIO_ROOT_USER" \
      "$MINIO_ROOT_PASSWORD"

    mc tree local/fincore-raw
  '

```

_Expected partitions:_

- `trades/dt=2026-08-03/`
- `market_data/dt=2026-08-03/`
- `portfolio/dt=2026-08-03/`

---

## 11. Run Spark Manually

Use the Spark version-compatible Hadoop AWS dependency:

```bash
docker compose exec spark-master /bin/bash -lc '
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.hadoop:hadoop-aws:3.4.2 \
    /opt/fincore/spark/etl_job.py \
    --run-date 2026-08-03 \
    --s3-endpoint http://minio:9000 \
    --s3-access-key "$MINIO_ROOT_USER" \
    --s3-secret-key "$MINIO_ROOT_PASSWORD"
'

```

Expected output locations:

```text
fincore-processed/
├── processed/trades/dt=2026-08-03/
├── processed/portfolio_pnl/dt=2026-08-03/
├── processed/unresolvable_pnl/dt=2026-08-03/
├── rejected/dt=2026-08-03/
└── metrics/dt=2026-08-03/

```

Verify:

```bash
docker compose run --rm \
  --entrypoint /bin/sh \
  minio-init \
  -c '
    mc alias set local http://minio:9000 \
      "$MINIO_ROOT_USER" \
      "$MINIO_ROOT_PASSWORD"

    mc tree local/fincore-processed
  '

```

---

## 12. Airflow Execution

Trigger the DAG:

```bash
docker compose exec airflow-webserver \
  airflow dags trigger fincore_daily_pipeline \
  --conf '{"run_date": "2026-08-03"}'

```

Monitor the run at: `http://localhost:8080`

Expected DAG path:

```text
resolve_run_date
    |
    +-- wait_for_trades
    +-- wait_for_market_data
    +-- wait_for_portfolio
    |
parallel source validation
    |
join_extracts
    |
run_spark_etl
    |
read_metrics
    |
quality_gate
    |
quality_gate_passed
    |
warehouse_load
    |
warehouse_load_complete
    |
dbt_run
    |
dbt_test
    |
pipeline_complete

```

_Note: When the quality gate passes, `quality_gate_failed` is expected to be skipped._

---

## 13. Quality-Gate Rules

Warehouse loading is allowed only when:

- trade reject rate < 2% **and**
- unresolvable P&L count < 100

### Example successful run

- Processed trades: 991
- Rejected trades: 9
- Reject rate: 0.9%
- Unresolvable P&L: 15
- **Quality gate:** Passed

_The quality gate is evaluated by Airflow using the Spark metrics JSON._

---

## 14. Warehouse Loading

The warehouse load tasks run in parallel:

- `raw.processed_trades`
- `raw.portfolio_pnl`
- `raw.rejected_records`
- `raw.unresolvable_pnl`

### Each task:

1. Reads a date-partitioned Parquet dataset from MinIO.
2. Deletes existing rows for the run date.
3. Loads the new data through PostgreSQL COPY.
4. Records the result in `audit.pipeline_load_log`.

_This delete-and-reload design makes reruns idempotent._

---

## 15. Verify Warehouse Results

Check row counts:

```bash
docker compose exec warehouse-db \
  psql \
  -U "$WAREHOUSE_USER" \
  -d "$WAREHOUSE_DB" \
  -c "
    SELECT 'processed_trades' AS table_name, COUNT(*) AS row_count
    FROM raw.processed_trades
    WHERE trade_date = DATE '2026-08-03'

    UNION ALL

    SELECT 'portfolio_pnl', COUNT(*)
    FROM raw.portfolio_pnl
    WHERE pnl_date = DATE '2026-08-03'

    UNION ALL

    SELECT 'rejected_records', COUNT(*)
    FROM raw.rejected_records
    WHERE run_date = DATE '2026-08-03'

    UNION ALL

    SELECT 'unresolvable_pnl', COUNT(*)
    FROM raw.unresolvable_pnl
    WHERE trade_date = DATE '2026-08-03';
  "

```

Check audit records:

```bash
docker compose exec warehouse-db \
  psql \
  -U "$WAREHOUSE_USER" \
  -d "$WAREHOUSE_DB" \
  -c "
    SELECT
        target_table,
        rows_deleted,
        rows_inserted,
        load_status,
        started_at,
        completed_at
    FROM audit.pipeline_load_log
    ORDER BY load_id DESC
    LIMIT 10;
  "

```

---

## 16. Run dbt Manually

Run dbt models:

```bash
docker compose exec airflow-scheduler \
  bash -lc '
    cd /opt/airflow/dbt
    dbt run --profiles-dir .
  '

```

Run tests:

```bash
docker compose exec airflow-scheduler \
  bash -lc '
    cd /opt/airflow/dbt
    dbt test --profiles-dir .
  '

```

Run both:

```bash
docker compose exec airflow-scheduler \
  bash -lc '
    cd /opt/airflow/dbt
    dbt build --profiles-dir .
  '

```

_Expected schemas:_ `staging`, `marts`

Verify:

```bash
docker compose exec warehouse-db \
  psql \
  -U "$WAREHOUSE_USER" \
  -d "$WAREHOUSE_DB" \
  -c "
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema IN ('staging', 'marts')
    ORDER BY table_schema, table_name;
  "

```

---

## 17. Grafana Dashboard Validation

- Open: `http://localhost:3000`
- Dashboard: **FinCore Daily Pipeline Monitoring**

### Expected successful-run metrics

- Quality gate: **Passed**
- Processed trades: **991**
- Reject rate: **0.900%**
- Unresolvable P&L: **15**
- Realized P&L: **approximately -$199K**
- Risk breaches: **0**

> _A negative P&L is a business result and does not indicate pipeline failure._

---

## 18. Rerunning a Partition

Trigger the same run date again:

```bash
docker compose exec airflow-webserver \
  airflow dags trigger fincore_daily_pipeline \
  --conf '{"run_date": "2026-08-03"}'

```

### Expected behavior

- Spark overwrites the MinIO output partition.
- Warehouse tasks delete existing rows for the date.
- Warehouse tasks load the replacement rows.
- dbt rebuilds downstream models.
- Row counts remain stable.

Verify idempotency by checking:

```sql
SELECT COUNT(*)
FROM raw.processed_trades
WHERE trade_date = DATE '2026-08-03';

```

_The count should not double after rerunning._

---

## 19. Recovery Procedures

### 19.1 Restart a failed Airflow task

1. Open the DAG run in the Airflow UI.
2. Select the failed task.
3. Choose **Clear** (include downstream tasks when appropriate).
4. Confirm the task reruns.

### 19.2 Rerun Spark only

Use the manual Spark command outlined in section 11.

### 19.3 Reload warehouse data only

Clear the tasks inside `warehouse_load`, and clear downstream tasks (`warehouse_load_complete`, `dbt_run`, `dbt_test`, `pipeline_complete`).

### 19.4 Rebuild dbt models only

Clear `dbt_run`, `dbt_test`, and `pipeline_complete`, or execute dbt manually.

### 19.5 Restart a container

```bash
docker compose restart <service-name>

```

_Example:_

```bash
docker compose restart airflow-scheduler

```

---

## 20. Common Failures

### 20.1 Ivy cache points to `/nonexistent`

- **Error:** `FileNotFoundException: /nonexistent/.ivy2...`
- **Cause:** Spark container user has an invalid home directory.
- **Fix:** `--conf spark.jars.ivy=/tmp/.ivy2`

### 20.2 MinIO credentials are empty

- **Error:** `MinIO access key and secret key must be provided`
- **Cause:** `.env` values were not exported or passed into the container.
- **Fix:**

```bash
set -a
source .env
set +a

```

Ensure Spark services use `env_file: - .env`.

### 20.3 Hadoop 60s parsing error

- **Error:** `NumberFormatException: For input string: "60s"`
- **Cause:** `hadoop-aws:3.3.4` is incompatible with the Hadoop libraries bundled with Spark 4.1.2.
- **Fix:** Use `org.apache.hadoop:hadoop-aws:3.4.2`

### 20.4 Invalid FIX timestamp crashes Spark

- **Error:** `CANNOT_PARSE_TIMESTAMP`
- **Cause:** Spark ANSI mode rejects malformed values such as `60=INVALID`.
- **Fix:** Use tolerant parsing:

```python
F.try_to_timestamp(
    F.col("transact_time"),
    F.lit("yyyyMMdd-HH:mm:ss"),
)

```

Invalid values should return `NULL` and be routed to rejected records.

### 20.5 Airflow provider import failure

- **Error:** `ModuleNotFoundError: No module named 'airflow.providers.standard'`
- **Fix:** `from airflow.operators.bash import BashOperator`

### 20.6 Lazy XCom sequence cannot be serialized

- **Error:** `LazyXComSelectSequence is not JSON serializable`
- **Fix:**

```python
results = list(pulled_results or [])

```

For logging: `json.dumps(results, default=str)`

### 20.7 Audit table schema mismatch

- **Error:** `column "dag_id" does not exist`
- **Cause:** An older version of `audit.pipeline_load_log` already exists.
- **Fix:**

```sql
DROP TABLE IF EXISTS audit.pipeline_load_log;

```

Then rerun:

```bash
docker compose exec -T warehouse-db \
  psql \
  -U "$WAREHOUSE_USER" \
  -d "$WAREHOUSE_DB" \
  < scripts/create-warehouse-tables.sql

```

### 20.8 PyArrow MinIO endpoint error

- PyArrow must receive an endpoint without the protocol.
- **Correct:** `minio:9000`
- **Incorrect:** `http://minio:9000`
- Use `urlparse()` before passing `endpoint_override`.

### 20.9 Incorrect Parquet path

- **Correct PyArrow path:** `fincore-processed/processed/trades/dt=2026-08-03`
- Do not use `s3://fincore-processed/...` when passing a filesystem object separately to `pyarrow.dataset`.

### 20.10 Grafana cannot resolve PostgreSQL

- **Error:** `lookup warehouse-db: no such host`
- **Cause:** Grafana and PostgreSQL are not attached to the same Docker network.
- **Fix:** Attach both services to the same bridge network and recreate them.
- Verify:

```bash
docker compose exec grafana \
  getent hosts warehouse-db

```

### 20.11 dbt writes to public_marts

- **Cause:** dbt combines the target schema and custom schema.
- **Fix:** Add `dbt/macros/generate_schema_name.sql` with:

```jinja
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

```

---

## 21. Alert Conditions

Operational alerts should be raised for:

- Source sensor timeout
- Source partition missing
- Spark job failure
- Quality-gate failure
- Warehouse-load failure
- dbt run failure
- DAG runtime exceeding six hours
- PostgreSQL unavailable
- MinIO unavailable

### Configured placeholders

- `SLACK_WEBHOOK_URL`
- `PAGERDUTY_WEBHOOK_URL`

> _Never hard-code webhook secrets in the DAG._

---

## 22. Backup and Reset

### Back up PostgreSQL

```bash
docker compose exec warehouse-db \
  pg_dump \
  -U "$WAREHOUSE_USER" \
  "$WAREHOUSE_DB" \
  > fincore_warehouse_backup.sql

```

### Restore PostgreSQL

```bash
docker compose exec -T warehouse-db \
  psql \
  -U "$WAREHOUSE_USER" \
  -d "$WAREHOUSE_DB" \
  < fincore_warehouse_backup.sql

```

### Reset the full environment

```bash
docker compose down -v
docker compose build
docker compose up -d

```

Then recreate:

- Source files
- MinIO partitions
- Warehouse tables
- Monitoring views
- Airflow variables and connections
- Grafana dashboard

---

## 23. Operational Checklist

### Before running

- [ ] Docker services are healthy.
- [ ] `.env` is loaded.
- [ ] MinIO buckets exist.
- [ ] Source partitions exist for the run date.
- [ ] Airflow DAG has no import errors.
- [ ] Spark master and worker are running.
- [ ] PostgreSQL is accepting connections.
- [ ] `dbt debug` succeeds.
- [ ] Grafana can connect to PostgreSQL.

### After running

- [ ] Airflow DAG is green.
- [ ] Quality gate passed.
- [ ] Processed MinIO outputs exist.
- [ ] Rejected output exists.
- [ ] Metrics JSON exists.
- [ ] Warehouse row counts are correct.
- [ ] Load audit records show SUCCESS.
- [ ] dbt models exist in staging and marts.
- [ ] dbt tests completed.
- [ ] Grafana metrics match warehouse results.
- [ ] Evidence screenshots were captured.

---

## 24. Escalation

**Primary owner:** FinCore Data Engineering

### Escalate when:

- The same pipeline stage fails after all configured retries.
- Data-quality thresholds fail unexpectedly.
- The warehouse contains inconsistent rerun results.
- MinIO data is missing or corrupted.
- Financial results differ materially from validated source data.
- The six-hour SLA is at risk.

### Record during escalation:

- Run date
- Airflow run ID
- Failed task
- Error message
- Retry count
- Affected source or target
- MinIO partition path
- Warehouse row counts
- Recent deployment or configuration changes

---

## 25. Evidence

- Airflow successful DAG run
- Expanded warehouse load TaskGroup
- MinIO processed partitions
- PostgreSQL row-count verification
- dbt test results
- Grafana monitoring dashboard
