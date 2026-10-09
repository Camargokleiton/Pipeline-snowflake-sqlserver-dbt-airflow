import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")

SNOWFLAKE_DATABASE = "ERP_DATABASE"
SNOWFLAKE_SCHEMAS = ("BRONZE", "SILVER", "GOLD")
SNOWFLAKE_STAGE = "BRONZE_PARQUET_STAGE"


def ensure_pipeline_schemas():
    from data_font.fake_ingest_sqlserver_database import seed_database_sqlserver as seed_module

    seed_module.ensure_schema()

    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake")
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        for schema_name in SNOWFLAKE_SCHEMAS:
            cursor.execute(
                f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{schema_name}"
            )
        cursor.execute(
            f"CREATE STAGE IF NOT EXISTS "
            f"{SNOWFLAKE_DATABASE}.BRONZE.{SNOWFLAKE_STAGE}"
        )
    finally:
        cursor.close()
        connection.close()


def generate_fake_data_task():
    from data_font.fake_ingest_sqlserver_database import seed_database_sqlserver as seed_module

    counts = {
        "categories": seed_module.get_env_int("FAKE_CATEGORIES", 5),
        "customers": seed_module.get_env_int("FAKE_CUSTOMERS", 50),
        "products": seed_module.get_env_int("FAKE_PRODUCTS", 100),
        "orders": seed_module.get_env_int("FAKE_ORDERS", 200),
    }

    if counts["orders"] and (not counts["customers"] or not counts["products"]):
        raise ValueError("Para gerar pedidos, FAKE_CUSTOMERS e FAKE_PRODUCTS devem ser maiores que zero.")

    seed_module.insert_fake_data(counts)


with DAG(
    dag_id="dag_seed_sqlserver",
    description="Gera dados ficticios no SQL Server a cada 5 minutos e aguarda a carga completa.",
    start_date=datetime(2026, 9, 28),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["sqlserver", "seed", "fake-data"],
) as dag:
    ensure_schemas = PythonOperator(
        task_id="ensure_sqlserver_and_snowflake_schemas",
        python_callable=ensure_pipeline_schemas,
    )

    generate_fake_data = PythonOperator(
        task_id="generate_fake_data",
        python_callable=generate_fake_data_task,
    )

    trigger_bronze_ingestion = TriggerDagRunOperator(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id="dag_sqlserver_to_snowflake_bronze",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    ensure_schemas >> generate_fake_data >> trigger_bronze_ingestion