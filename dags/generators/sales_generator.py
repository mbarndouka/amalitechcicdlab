from __future__ import annotations

import io
import os
import tomllib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config
from faker import Faker

fake = Faker()

_CONFIG_PATH = Path(
    os.getenv("GENERATOR_CONFIG", "/opt/airflow/config/generator.toml")
)


def load_config() -> dict:
    with open(_CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def generate_sales(n_rows: int | None = None) -> pd.DataFrame:
    cfg = load_config()["generator"]
    n_rows   = n_rows or cfg["default_rows"]
    start_dt = datetime.now(tz=timezone.utc) - timedelta(days=cfg["lookback_days"])

    rows = []
    for _ in range(n_rows):
        qty        = fake.random_int(min=1, max=cfg["max_qty"])
        unit_price = round(fake.random_number(digits=3) / 10 + 5.0, 2)
        rows.append({
            "order_id":    str(uuid.uuid4()),
            "order_ts":    fake.date_time_between(
                               start_date=start_dt,
                               end_date="now",
                               tzinfo=timezone.utc,
                           ).isoformat(),
            "customer_id": fake.random_int(min=1, max=cfg["max_customer_id"]),
            "product":     fake.random_element(cfg["products"]),
            "region":      fake.random_element(cfg["regions"]),
            "qty":         qty,
            "unit_price":  unit_price,
            "total":       round(qty * unit_price, 2),
        })
    return pd.DataFrame(rows)


def upload_to_minio(
    df: pd.DataFrame,
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
) -> str:
    cfg = load_config()["minio"]
    ts  = datetime.now(tz=timezone.utc)
    key = f"{ts.strftime('%Y/%m/%d')}/{ts.strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}.csv"

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    client = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    client.put_object(Bucket=cfg["bucket_raw"], Key=key, Body=csv_buffer.getvalue())
    return key


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=None)
    args = parser.parse_args()

    df  = generate_sales(args.rows)
    key = upload_to_minio(
        df,
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    )
    cfg = load_config()
    print(f"Uploaded {len(df)} rows → s3://{cfg['minio']['bucket_raw']}/{key}")
