import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

TABLES_CONFIG = {
    'customers': 'id_customer',
    'categories': 'id_category',
    'products': 'id_product',
    'orders': 'id_order',
    'order_items': 'id_item',
    'payments': 'id_payment'
}

TEMP_DIR = '/tmp/parquet_ingestion'


def extract_to_parquet_and_load_snowflake(table_name: str, primary_key: str):
    """
    Extrai do SQL Server, gera arquivo Parquet local, envia para o Snowflake Stage
    e executa o COPY INTO de forma idempotente na camada BRONZE_RAW.
    """
    print(f"Starting extraction to Parquet for table: {table_name}")

    # 1. Extração do SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    sql_query = f"SELECT * FROM dbo.{table_name}"
    df = mssql_hook.get_pandas_df(sql_query)

    if df.empty:
        print(f"⚠️ Table {table_name} is empty. Skipping processing.")
        return

    # Normalização e metadados
    df.columns = [col.lower() for col in df.columns]
    df['_extracted_at'] = pd.Timestamp.now()
    df = df.drop_duplicates(subset=[primary_key.lower()])

    # 2. Gerar arquivo Parquet compacto localmente
    os.makedirs(TEMP_DIR, exist_ok=True)
    parquet_path = os.path.join(TEMP_DIR, f"{table_name}.parquet")
    
    # Salva com compressão Snappy (padrão Parquet super leve)
    df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
    
    file_size_kb = os.path.getsize(parquet_path) / 1024
    print(f"📦 Parquet file generated: {parquet_path} ({file_size_kb:.2f} KB)")

    # 3. Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    raw_table_name = f"RAW_{table_name.upper()}"
    stage_file_name = f"{table_name}.parquet"

    try:
        # Step A: Upload do Parquet para o Internal Stage do Snowflake
        print(f"📤 Uploading Parquet to Snowflake Stage: @BRONZE_RAW.BRONZE_PARQUET_STAGE")
        cursor.execute(f"""
            PUT file://{parquet_path} @ERP_ECOMMERCE_DW.BRONZE_RAW.BRONZE_PARQUET_STAGE 
            OVERWRITE = TRUE;
        """)

        # Step B: Limpeza da tabela destino (Garantia de carga limpa e sem duplicação)
        cursor.execute(f"TRUNCATE TABLE ERP_ECOMMERCE_DW.BRONZE_RAW.{raw_table_name};")

        # Step C: COPY INTO direto a partir do Parquet staged
        print(f"⚡ Running COPY INTO for table BRONZE_RAW.{raw_table_name}")
        
        # Mapeamento dinâmico das colunas do Parquet para as colunas da Tabela
        columns_list = ", ".join(df.columns)
        parquet_select_list = ", ".join([f"$1:{col}" for col in df.columns])

        copy_query = f"""
            COPY INTO ERP_ECOMMERCE_DW.BRONZE_RAW.{raw_table_name} ({columns_list})
            FROM (
                SELECT {parquet_select_list}
                FROM @ERP_ECOMMERCE_DW.BRONZE_RAW.BRONZE_PARQUET_STAGE/{stage_file_name}
            )
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = 'CONTINUE';
        """
        
        cursor.execute(copy_query)
        print(f"✅ Successfully loaded Parquet into BRONZE_RAW.{raw_table_name}!")

    finally:
        # Remover o arquivo temporário do container para não ocupar disco
        if os.path.exists(parquet_path):
            os.remove(parquet_path)
            
        cursor.close()
        conn.close()


with DAG(
    dag_id='dag_sqlserver_to_snowflake_bronze',
    default_args=default_args,
    description='Pipeline ELT via Parquet: Extrai do SQL Server, gera Parquet e carrega no Snowflake Bronze',
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'sqlserver', 'snowflake', 'bronze', 'parquet']
) as dag:

    tasks = []
    for table_name, pk in TABLES_CONFIG.items():
        task = PythonOperator(
            task_id=f'ingest_{table_name}_to_bronze',
            python_callable=extract_to_parquet_and_load_snowflake,
            op_kwargs={'table_name': table_name, 'primary_key': pk}
        )
        tasks.append(task)

    tasks