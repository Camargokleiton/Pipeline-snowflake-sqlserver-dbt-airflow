import os
from datetime import date, datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

TABLES_CONFIG = {
    "customers": "id_customer",
    "categories": "id_category",
    "products": "id_product",
    "orders": "id_order",
    "order_items": "id_item",
    "payments": "id_payment",
}

TEMP_DIR = "/tmp/parquet_ingestion"
SNOWFLAKE_DATABASE = "ERP_DATABASE"
BRONZE_SCHEMA = "BRONZE"
SNOWFLAKE_CONN_ID = "snowflake"
TIMESTAMP_COLUMNS = {"created_at", "updated_at", "order_date", "payment_date"}


def _snowflake_type_from_pandas_series(series: pd.Series) -> str:
    dtype = str(series.dtype).lower()
    if (
        str(series.name).lower() in TIMESTAMP_COLUMNS
        or pd.api.types.is_datetime64_any_dtype(series.dtype)
        or (
            series.dtype == object
            and any(
                isinstance(value, (date, datetime))
                for value in series.dropna().head(20)
            )
        )
    ):
        return "TIMESTAMP_NTZ"
    if "int" in dtype or "uint" in dtype:
        return "NUMBER"
    if "float" in dtype or "double" in dtype or "decimal" in dtype:
        return "FLOAT"
    if "bool" in dtype:
        return "BOOLEAN"
    if "datetime" in dtype or "date" in dtype:
        return "TIMESTAMP"
    return "VARCHAR"


def _serialize_timestamps_for_parquet(
    dataframe: pd.DataFrame,
    snowflake_types: dict[str, str],
) -> None:
    for column, snowflake_type in snowflake_types.items():
        if snowflake_type == "TIMESTAMP_NTZ":
            values = pd.to_datetime(dataframe[column], errors="raise")
            if values.dt.tz is not None:
                values = values.dt.tz_convert("UTC").dt.tz_localize(None)
            dataframe[column] = values.dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def create_snowflake_bronze_target():
    """Cria database, schema e stage no Snowflake antes da ingestão."""
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}")
        cursor.execute(
            f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.BRONZE_PARQUET_STAGE"
        )

        mssql_hook = MsSqlHook(mssql_conn_id="mssql_default")
        for table_name in TABLES_CONFIG:
            df_schema = mssql_hook.get_pandas_df(f"SELECT TOP 0 * FROM dbo.{table_name}")
            if df_schema.empty:
                continue

            columns_sql = ", ".join(
                f'"{str(col).lower()}" {_snowflake_type_from_pandas_series(df_schema[col])}'
                for col in df_schema.columns
            )
            raw_table_name = f"RAW_{table_name.upper()}"
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.{raw_table_name} '
                f'({columns_sql}, "_extracted_at" TIMESTAMP_NTZ)'
            )

        print("Snowflake bronze target created successfully.")
    finally:
        cursor.close()
        conn.close()


def extract_to_parquet_and_load_snowflake(table_name: str, primary_key: str):
    """Lê do SQL Server, salva em parquet e carrega em bronze no Snowflake."""
    print(f"Starting extraction to Parquet for table: {table_name}")

    mssql_hook = MsSqlHook(mssql_conn_id="mssql_default")
    df = mssql_hook.get_pandas_df(f"SELECT * FROM dbo.{table_name}")

    if df.empty:
        print(f"Table {table_name} is empty. Skipping processing.")
        return
    df.columns = [str(col).lower() for col in df.columns]
    df = df.drop_duplicates(subset=[primary_key.lower()])
    snowflake_types = {
        column: _snowflake_type_from_pandas_series(df[column])
        for column in df.columns
    }
    _serialize_timestamps_for_parquet(df, snowflake_types)

    os.makedirs(TEMP_DIR, exist_ok=True)
    parquet_path = os.path.join(TEMP_DIR, f"{table_name}.parquet")
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)

    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    raw_table_name = f"RAW_{table_name.upper()}"
    bronze_table = f"{SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.{raw_table_name}"
    stage_path = f"@{SNOWFLAKE_DATABASE}.{BRONZE_SCHEMA}.BRONZE_PARQUET_STAGE/{table_name}/"

    try:
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {bronze_table} ("
            + ", ".join(
                f'"{str(col).lower()}" {snowflake_types[col]}'
                for col in df.columns
            )
            + ")"
        )
        cursor.execute(
            f'ALTER TABLE {bronze_table} ADD COLUMN IF NOT EXISTS "_extracted_at" TIMESTAMP_NTZ'
        )

        cursor.execute(f"PUT file://{parquet_path} {stage_path} OVERWRITE = TRUE;")
        cursor.execute(f"TRUNCATE TABLE {bronze_table};")

        cursor.execute(
            f"""
            COPY INTO {bronze_table}
            FROM {stage_path}
            FILES = ('{table_name}.parquet')
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'ABORT_STATEMENT';
            """
        )
        copy_results = cursor.fetchall()
        loaded_rows = sum(int(result[3]) for result in copy_results)
        if loaded_rows != len(df):
            raise RuntimeError(
                f"Expected to load {len(df)} rows into {bronze_table}, "
                f"but Snowflake loaded {loaded_rows}."
            )
        cursor.execute(
            f'UPDATE {bronze_table} SET "_extracted_at" = CURRENT_TIMESTAMP() '
            'WHERE "_extracted_at" IS NULL'
        )

        print(f"Successfully loaded data into {bronze_table}!")
    finally:
        if os.path.exists(parquet_path):
            os.remove(parquet_path)
        cursor.close()
        conn.close()


with DAG(
    dag_id="dag_sqlserver_to_snowflake_bronze",
    default_args=default_args,
    description="Pipeline ELT via Parquet: Extrai do SQL Server, gera Parquet e carrega no Snowflake Bronze",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "sqlserver", "snowflake", "bronze", "parquet"],
) as dag:
    create_target = PythonOperator(
        task_id="create_snowflake_bronze_target",
        python_callable=create_snowflake_bronze_target,
    )

    ingest_tasks = []
    for table_name, pk in TABLES_CONFIG.items():
        task = PythonOperator(
            task_id=f"ingest_{table_name}_to_bronze",
            python_callable=extract_to_parquet_and_load_snowflake,
            op_kwargs={"table_name": table_name, "primary_key": pk},
        )
        ingest_tasks.append(task)

    trigger_dbt_snapshots = TriggerDagRunOperator(
        task_id="trigger_dbt_snapshots",
        trigger_dag_id="dag_dbt_snapshots",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    create_target >> ingest_tasks
    ingest_tasks >> trigger_dbt_snapshots