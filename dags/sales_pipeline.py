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
    def check_tables() -> None:
        required = ["dim_date", "dim_customer", "dim_product", "dim_geography",
                    "dim_channel", "dim_payment", "fact_sales"]
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn    = pg_hook.get_conn()
        cursor  = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
        """, (required,))
        found   = {row[0] for row in cursor.fetchall()}
        missing = set(required) - found
        cursor.close()
        if missing:
            raise RuntimeError(
                f"Missing tables in target DB: {sorted(missing)}. "
                "Run init-db/02-create-sales-table.sql first."
            )

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
                                "product_name", "country_code", "qty", "unit_price"])
        df = df.drop_duplicates(subset=["order_id"])
        df["order_ts"]    = pd.to_datetime(df["order_ts"], utc=True)
        df["qty"]         = df["qty"].astype(int)
        df["unit_price"]  = df["unit_price"].astype(float).round(2)
        df["cost_price"]  = df["cost_price"].astype(float).round(2)
        df["discount_pct"]= df["discount_pct"].fillna(0).astype(float).round(2)
        df["total"]       = df["total"].astype(float).round(2)
        df["gross_margin"]= df["gross_margin"].astype(float).round(2)
        df["is_returned"] = df["is_returned"].fillna(False).astype(bool)
        df["source_file"] = key
        df = df[(df["qty"] > 0) & (df["unit_price"] > 0)]

        conn   = pg_hook.get_conn()
        cursor = conn.cursor()

        # ── Upsert dim_date ───────────────────────────────────────────────────
        dates = df["order_ts"].dt.date.unique()
        date_rows = []
        for d in dates:
            dt = datetime(d.year, d.month, d.day)
            date_rows.append((
                int(d.strftime("%Y%m%d")),
                d,
                d.year,
                (d.month - 1) // 3 + 1,
                d.month,
                dt.strftime("%B"),
                int(d.strftime("%W")),
                d.weekday(),
                dt.strftime("%A"),
                d.weekday() >= 5,
                d.weekday() < 5,
            ))
        execute_values(cursor, """
            INSERT INTO dim_date
                (date_id, full_date, year, quarter, month_num, month_name,
                 week_of_year, day_of_week, day_name, is_weekend, is_business_day)
            VALUES %s ON CONFLICT (date_id) DO NOTHING
        """, date_rows)

        # ── Upsert dim_customer ───────────────────────────────────────────────
        cust_cols = ["customer_id", "customer_full_name", "customer_email",
                     "customer_signup_date", "customer_tier"]
        cust_rows = df[cust_cols].drop_duplicates("customer_id").values.tolist()
        execute_values(cursor, """
            INSERT INTO dim_customer (customer_id, full_name, email, signup_date, tier)
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE
                SET tier = EXCLUDED.tier, updated_at = NOW()
        """, cust_rows)

        # ── Upsert dim_product ────────────────────────────────────────────────
        prod_cols = ["product_name", "product_category", "product_brand",
                     "product_sku", "product_cost_price", "product_list_price"]
        prod_rows = df[prod_cols].drop_duplicates("product_name").values.tolist()
        execute_values(cursor, """
            INSERT INTO dim_product
                (name, category, brand, sku, cost_price, list_price)
            VALUES %s
            ON CONFLICT (name) DO UPDATE
                SET cost_price = EXCLUDED.cost_price,
                    list_price = EXCLUDED.list_price,
                    updated_at = NOW()
        """, prod_rows)

        # ── Upsert dim_geography ──────────────────────────────────────────────
        geo_cols  = ["country", "country_code", "region", "sub_region", "latitude", "longitude"]
        geo_rows  = df[geo_cols].drop_duplicates("country_code").values.tolist()
        execute_values(cursor, """
            INSERT INTO dim_geography (country, country_code, region, sub_region, latitude, longitude)
            VALUES %s ON CONFLICT (country_code) DO NOTHING
        """, geo_rows)

        # ── Upsert dim_channel ────────────────────────────────────────────────
        ch_cols  = ["channel", "channel_type"]
        ch_rows  = df[ch_cols].drop_duplicates("channel").values.tolist()
        execute_values(cursor, """
            INSERT INTO dim_channel (channel, channel_type)
            VALUES %s ON CONFLICT (channel) DO NOTHING
        """, ch_rows)

        # ── Upsert dim_payment ────────────────────────────────────────────────
        pay_cols = ["payment_method", "payment_is_digital"]
        pay_rows = df[pay_cols].drop_duplicates("payment_method").values.tolist()
        execute_values(cursor, """
            INSERT INTO dim_payment (method, is_digital)
            VALUES %s ON CONFLICT (method) DO NOTHING
        """, pay_rows)

        conn.commit()

        # ── Resolve FK IDs ────────────────────────────────────────────────────
        cursor.execute("SELECT date_id, full_date FROM dim_date")
        date_map = {str(r[1]): r[0] for r in cursor.fetchall()}

        cursor.execute("SELECT customer_id FROM dim_customer")
        # customer_id is the natural PK — no mapping needed

        cursor.execute("SELECT product_id, name FROM dim_product")
        prod_map = {r[1]: r[0] for r in cursor.fetchall()}

        cursor.execute("SELECT geo_id, country_code FROM dim_geography")
        geo_map = {r[1]: r[0] for r in cursor.fetchall()}

        cursor.execute("SELECT channel_id, channel FROM dim_channel")
        ch_map = {r[1]: r[0] for r in cursor.fetchall()}

        cursor.execute("SELECT payment_id, method FROM dim_payment")
        pay_map = {r[1]: r[0] for r in cursor.fetchall()}

        # ── Build fact rows ───────────────────────────────────────────────────
        df["date_key"] = df["order_ts"].dt.date.astype(str).map(date_map)
        df["prod_fk"]  = df["product_name"].map(prod_map)
        df["geo_fk"]   = df["country_code"].map(geo_map)
        df["ch_fk"]    = df["channel"].map(ch_map)
        df["pay_fk"]   = df["payment_method"].map(pay_map)

        fact_cols = ["order_id", "order_ts", "date_key", "customer_id",
                     "prod_fk", "geo_fk", "ch_fk", "pay_fk",
                     "qty", "unit_price", "cost_price", "discount_pct",
                     "total", "gross_margin", "is_returned", "source_file"]
        rows = df[fact_cols].values.tolist()

        # ── Load fact_sales ───────────────────────────────────────────────────
        execute_values(cursor, """
            INSERT INTO fact_sales
                (order_id, order_ts, date_id, customer_id,
                 product_id, geo_id, channel_id, payment_id,
                 qty, unit_price, cost_price, discount_pct,
                 total, gross_margin, is_returned, source_file)
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
        """, rows)
        conn.commit()
        cursor.close()

        # ── Archive ───────────────────────────────────────────────────────────
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

    table_ok     = check_tables()
    uploaded_key = generate_data()
    uploaded_key.set_upstream(table_ok)
    keys         = list_new_files(uploaded_key)
    process_file.expand(key=keys)


sales_pipeline()
