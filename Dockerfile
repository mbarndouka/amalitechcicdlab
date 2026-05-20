ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.2.1
FROM ${AIRFLOW_BASE_IMAGE}

COPY pyproject.toml /opt/airflow/pyproject.toml

RUN pip install --no-cache-dir /opt/airflow
