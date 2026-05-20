from __future__ import annotations

import io
import os
from datetime import datetime

import pandas as pd
from psycopg2.extras import execute_values

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import dag, task

from generators.sales_generator import generate_sales, load_config, upload_to_minio

MINIO_CONN_ID    = "minio_s3"
POSTGRES_CONN_ID = "postgres_target"

_ALERT_EMAIL = os.environ.get("AIRFLOW_ALERT_EMAIL", "")

_failure_notifier = SmtpNotifier(
    from_email=_ALERT_EMAIL,
    to=[_ALERT_EMAIL],
    subject="[AIRFLOW FAILED] {{ dag.dag_id }} › {{ ti.task_id }} | {{ run_id }}",
    html_content="""
    <h2 style="color:#c0392b;">Pipeline Failure Alert</h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse;">
      <tr><td><b>DAG</b></td><td>{{ dag.dag_id }}</td></tr>
      <tr><td><b>Task</b></td><td>{{ ti.task_id }}</td></tr>
      <tr><td><b>Run ID</b></td><td>{{ run_id }}</td></tr>
      <tr><td><b>Attempt</b></td><td>{{ try_number }} of {{ max_tries + 1 }}</td></tr>
      <tr><td><b>Exception</b></td><td>{{ exception_html }}</td></tr>
    </table>
    <p>
      <a href="{{ ti.log_url }}">View Logs</a> |
      <a href="{{ ti.mark_success_url }}">Mark Success</a>
    </p>
    """,
    smtp_conn_id="smtp_default",
)

_success_notifier = SmtpNotifier(
    from_email=_ALERT_EMAIL,
    to=[_ALERT_EMAIL],
    subject="[AIRFLOW SUCCESS] {{ dag.dag_id }} | {{ run_id }}",
    html_content="""
    <h2 style="color:#27ae60;">Pipeline Completed Successfully</h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse;">
      <tr><td><b>DAG</b></td><td>{{ dag.dag_id }}</td></tr>
      <tr><td><b>Run ID</b></td><td>{{ run_id }}</td></tr>
      <tr><td><b>Started</b></td><td>{{ dag_run.start_date }}</td></tr>
      <tr><td><b>Ended</b></td><td>{{ dag_run.end_date }}</td></tr>
    </table>
    """,
    smtp_conn_id="smtp_default",
)


@dag(
    dag_id="sales_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "ingestion"],
    on_success_callback=_success_notifier,
    default_args={
        "retries": 2,
        "on_failure_callback": _failure_notifier,
    },
)
def sales_pipeline():

    @task
    def generate_data() -> str:

        creds = S3Hook(aws_conn_id=MINIO_CONN_ID).get_credentials()
        key   = upload_to_minio(
            generate_sales(),
            access_key=creds.access_key,
            secret_key=creds.secret_key,
        )
        return key

    @task
    def check_table() -> None:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'sales'
            )
        """)
        exists = cursor.fetchone()[0]
        cursor.close()
        if not exists:
            raise RuntimeError(
                "Table 'sales' not found in target DB. "
                "Run init-db/02-create-sales-table.sql before ingesting."
            )

    @task
    def list_new_files(uploaded_key: str) -> list[str]:
        cfg  = load_config()
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        return hook.list_keys(bucket_name=cfg["minio"]["bucket_raw"]) or []

    @task
    def process_file(key: str) -> None:
        cfg     = load_config()
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # ── Extract ──────────────────────────────────────────────────────────
        raw = s3_hook.get_key(key=key, bucket_name=cfg["minio"]["bucket_raw"])
        df  = pd.read_csv(io.BytesIO(raw.get()["Body"].read()))

        # ── Transform ────────────────────────────────────────────────────────
        df = df.dropna(subset=["order_id", "order_ts", "customer_id",
                                "product", "region", "qty", "unit_price"])
        df = df.drop_duplicates(subset=["order_id"])
        df["order_ts"]    = pd.to_datetime(df["order_ts"], utc=True)
        df["qty"]         = df["qty"].astype(int)
        df["unit_price"]  = df["unit_price"].astype(float).round(2)
        df["total"]       = (df["qty"] * df["unit_price"]).round(2)
        df["source_file"] = key
        df = df[(df["qty"] > 0) & (df["unit_price"] > 0)]

        # ── Load ─────────────────────────────────────────────────────────────
        cols = ["order_id", "order_ts", "customer_id", "product",
                "region", "qty", "unit_price", "total", "source_file"]
        rows = df[cols].values.tolist()

        conn   = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_values(
            cursor,
            """
            INSERT INTO sales
                (order_id, order_ts, customer_id, product,
                 region, qty, unit_price, total, source_file)
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
            """,
            rows,
        )
        conn.commit()
        cursor.close()

        # ── Archive ──────────────────────────────────────────────────────────
        s3_hook.copy_object(
            source_bucket_key=key,
            dest_bucket_key=key,
            source_bucket_name=cfg["minio"]["bucket_raw"],
            dest_bucket_name=cfg["minio"]["bucket_processed"],
        )
        s3_hook.delete_objects(
            bucket=cfg["minio"]["bucket_raw"],
            keys=[key],
        )

    uploaded_key   = generate_data()
    table_ok       = check_table()
    keys           = list_new_files(uploaded_key)
    keys.set_upstream(table_ok)
    process_file.expand(key=keys)


sales_pipeline()
