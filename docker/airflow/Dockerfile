FROM apache/airflow:3.3.2

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.13

COPY docker/airflow/requirements.txt /tmp/airflow-requirements.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/airflow-requirements.txt
