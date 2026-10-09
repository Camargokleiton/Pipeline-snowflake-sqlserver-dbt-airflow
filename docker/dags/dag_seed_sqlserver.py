import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow")


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

    seed_module.ensure_schema()
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

    generate_fake_data >> trigger_bronze_ingestion