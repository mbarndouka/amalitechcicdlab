# Amalitech Sales Pipeline

End-to-end sales data pipeline built on Apache Airflow 3, MinIO, PostgreSQL, and Metabase — fully containerised with Docker Compose and automated via GitHub Actions CI/CD.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│                                                                 │
│  ┌──────────────┐    ┌──────────────────────────────────────┐   │
│  │   Generator  │───▶│  MinIO  (S3-compatible object store) │   │
│  │  (Airflow    │    │  raw-sales/        processed-sales/  │   │
│  │   task)      │    └──────────────┬───────────────────────┘   │
│  └──────────────┘                   │                           │
│                                     ▼                           │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  Apache Airflow 3.2.1                    │   │
│  │  apiserver │ scheduler │ dag-processor │ worker │        │   │
│  │  triggerer │ (CeleryExecutor + Redis broker)            │   │
│  └──────────────────────────────┬───────────────────────────┘   │
│                                 │                               │
│                                 ▼                               │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │               PostgreSQL 16                              │   │
│  │  airflow (metadata DB)  │  target (star-schema DW)       │   │
│  └──────────────────────────┬─────────────────────────────┘    │
│                              │                                  │
│                              ▼                                  │
│                    ┌─────────────────┐                          │
│                    │    Metabase     │  (BI / dashboards)       │
│                    └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

### Data flow

1. **Ingest** — Airflow generates synthetic sales records and uploads CSV to MinIO `raw-sales/`
2. **Process** — Airflow reads each CSV, validates and transforms it, upserts dimension tables, loads `fact_sales`
3. **Archive** — processed files move from `raw-sales/` to `processed-sales/`
4. **Visualise** — Metabase connects to the `target` PostgreSQL database for dashboards

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker Engine | 24+ |
| Docker Compose | v2.1+ |
| Python | 3.10+ (secret generation only) |
| Git | any |

---

## Quick Start

### 1. Clone

```bash
git clone <repo-url>
cd amalitechcicdlab
```

### 2. Generate secrets

```bash
# Fernet key (Airflow encryption)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# JWT secret (Airflow API auth)
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 3. Create `.env`

Copy the template and fill in all empty values:

```bash
cp .env.example .env
```

Edit `.env` — every blank value is required. See [Environment Variables](#environment-variables) below.

### 4. Start the stack

```bash
docker compose up -d --wait
```

All services start in dependency order. The `--wait` flag blocks until every healthcheck passes (~3–5 minutes on first run).

### 5. Open the UIs

| Service | URL | Default credentials |
|---------|-----|---------------------|
| Airflow | http://localhost:8080 | `airflow` / `<AIRFLOW_ADMIN_PASSWORD>` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `<MINIO_ROOT_PASSWORD>` |
| Metabase | http://localhost:3000 | set on first launch |
| PostgreSQL | `localhost:5432` | `airflow` / `<POSTGRES_PASSWORD>` |

---

## Environment Variables

All variables live in `.env` (never commit this file). Use `.env.example` as the template.

| Variable | Description |
|----------|-------------|
| `AIRFLOW__CORE__FERNET_KEY` | Encrypts connections in the Airflow metadata DB |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Signs JWT tokens for the Airflow REST API |
| `_AIRFLOW_WWW_USER_PASSWORD` | Airflow web UI admin password |
| `POSTGRES_PASSWORD` | Password for the `airflow` PostgreSQL user |
| `MINIO_ROOT_PASSWORD` | MinIO root password |
| `AIRFLOW_SMTP_USER` | Gmail address for DAG alert emails |
| `AIRFLOW_SMTP_PASSWORD` | Gmail [App Password](https://myaccount.google.com/apppasswords) |
| `AIRFLOW_ALERT_EMAIL` | Recipient address for success/failure alerts |

Non-secret values (`AIRFLOW_UID`, `POSTGRES_DB`, `TARGET_DB`, etc.) have safe defaults in `.env.example`.

---

## DAG: `sales_pipeline`

**Schedule:** every 12 hours (`0 */12 * * *`) — runs at 00:00 and 12:00 UTC daily.

**File:** [`dags/sales_pipeline.py`](dags/sales_pipeline.py)

### Task graph

```
check_tables
     │
generate_data          ← uploads CSV to MinIO raw-sales/
     │
list_new_files         ← lists all objects in raw-sales/
     │
process_file.expand()  ← dynamic task per file: transform → load → archive
```

| Task | What it does |
|------|-------------|
| `check_tables` | Verifies all 7 star-schema tables exist in the `target` DB; fails fast if migrations haven't run |
| `generate_data` | Generates `N` synthetic sales rows (configurable), uploads as CSV to MinIO `raw-sales/` |
| `list_new_files` | Lists all unprocessed keys in `raw-sales/` |
| `process_file` | Reads CSV, cleans data, upserts all dimension tables, inserts into `fact_sales`, archives to `processed-sales/` |

### Email notifications

- **On failure:** task-level alert with DAG name, task name, run ID, attempt count, exception, and log link
- **On success:** DAG-level summary with start/end time

Configure SMTP via `AIRFLOW_SMTP_USER`, `AIRFLOW_SMTP_PASSWORD`, and `AIRFLOW_ALERT_EMAIL` in `.env`.

---

## Data Warehouse Schema

Target database: **`target`** on PostgreSQL.

### Fact table

| Column | Type | Description |
|--------|------|-------------|
| `sale_id` | BIGSERIAL | Surrogate PK |
| `order_id` | UUID | Natural key (deduplicated) |
| `order_ts` | TIMESTAMPTZ | Order timestamp |
| `date_id` | INT → `dim_date` | |
| `customer_id` | INT → `dim_customer` | |
| `product_id` | INT → `dim_product` | |
| `geo_id` | INT → `dim_geography` | |
| `channel_id` | INT → `dim_channel` | |
| `payment_id` | INT → `dim_payment` | |
| `qty`, `unit_price`, `cost_price` | NUMERIC | Line item figures |
| `discount_pct`, `total`, `gross_margin` | NUMERIC | Derived measures |
| `is_returned` | BOOLEAN | |
| `source_file` | TEXT | Originating MinIO key |

### Dimension tables

| Table | Grain | Key columns |
|-------|-------|-------------|
| `dim_date` | Calendar day | `date_id` (YYYYMMDD), year, quarter, month, week, day, is_weekend |
| `dim_customer` | Customer | `customer_id`, name, email, signup_date, tier |
| `dim_product` | Product name | name, category, brand, sku, cost_price, list_price |
| `dim_geography` | Country | country_code (2-char), region, sub_region, lat/lon |
| `dim_channel` | Sales channel | channel (`online`/`retail`/`partner`), channel_type |
| `dim_payment` | Payment method | method (`card`/`invoice`/`wallet`/`crypto`), is_digital |

Pre-seeded with 22 countries across Americas, Europe, Asia, Africa, and Oceania.

---

## Generator Configuration

**File:** [`config/generator.toml`](config/generator.toml)

| Setting | Default | Description |
|---------|---------|-------------|
| `generator.default_rows` | 1000 | Rows per DAG run |
| `generator.lookback_days` | 30 | Random order timestamp range |
| `generator.max_customer_id` | 500 | Customer ID ceiling |
| `generator.max_qty` | 20 | Max units per order |
| `minio.bucket_raw` | `raw-sales` | Landing bucket |
| `minio.bucket_processed` | `processed-sales` | Archive bucket |

Categories, products, countries, channel weights, and payment method weights are all configurable in the same file.

---

## CI/CD Pipeline

**File:** [`.github/workflows/main.yml`](.github/workflows/main.yml)

Triggered on every push and pull request to `main`.

```
push/PR ──▶ ci ──(push to main only)──▶ deploy ──▶ integration-test
```

### Job 1: CI — Build & Lint

Runs on every commit.

- Builds the custom Airflow image with Docker layer cache
- Validates `docker-compose.yaml` config
- Lints Dockerfile with **hadolint** (warning threshold)
- Lints Python DAGs with **ruff** (E/F/W rules)
- Lints YAML files with **yamllint**

### Job 2: CD — Deploy to Test Environment

Runs on push to `main` only (after CI passes).

- Starts the full Docker Compose stack on the GitHub Actions runner
- Waits for all healthchecks to pass (up to 10 min)
- Prints health state of all 9 services

### Job 3: Data Flow Validation

Runs after deploy (push to `main` only).

Validates the full pipeline end-to-end:

| Step | Assertion |
|------|-----------|
| A — MinIO | `raw-sales` bucket exists and MinIO is live |
| B — Airflow | Authenticates, unpauses DAG, triggers run, polls until `success` |
| C — PostgreSQL | `fact_sales` has rows; dimension tables populated |
| C — MinIO | Files archived to `processed-sales/` |
| D — Metabase | `GET /api/health` returns `{"status":"ok"}` |

### Required GitHub Secrets

Set in **repo → Settings → Secrets and variables → Actions**:

| Secret | How to generate |
|--------|----------------|
| `AIRFLOW_FERNET_KEY` | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_JWT_SECRET` | `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `AIRFLOW_ADMIN_PASSWORD` | Any strong password |
| `POSTGRES_PASSWORD` | Any strong password |
| `MINIO_PASSWORD` | Any strong password |
| `SMTP_USER` | Gmail address |
| `SMTP_PASSWORD` | Gmail App Password |
| `ALERT_EMAIL` | Alert recipient email |

> **Note:** Secrets are unavailable for pull requests from forks. CI will fail on fork PRs by design.

---

## Project Structure

```
.
├── .github/workflows/main.yml   # CI/CD pipeline
├── config/
│   ├── airflow.cfg              # Airflow SMTP and core config overrides
│   ├── generator.toml           # Sales data generator settings
│   └── email/                   # Email notification templates
├── dags/
│   ├── sales_pipeline.py        # Main DAG
│   └── generators/
│       └── sales_generator.py   # Synthetic data generator
├── init-db/
│   ├── 01-create-target-db.sh   # Creates the `target` database
│   └── 02-create-sales-table.sql # Star schema DDL + geography seed data
├── docker-compose.yaml
├── Dockerfile                   # Custom Airflow image
├── .env.example                 # Environment variable template
└── pyproject.toml
```

---

## Stopping the Stack

```bash
# Stop all containers
docker compose down

# Stop and remove all volumes (full reset)
docker compose down -v
```
