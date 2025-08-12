import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "sergei"
DAG_ID = "raw_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
## DAG для загрузки данных о землетрясениях из S3 в PostgreSQL

### Назначение
Пайплайн переносит данные о землетрясениях из MinIO (S3-хранилища) в целевую таблицу PostgreSQL, обеспечивая:
- Контроль зависимостей через сенсоры
- Трансформацию данных при загрузке
- Отслеживание качества данных

### Источники и приемники
| Тип               | Путь/Назначение                     |
|--------------------|-------------------------------------|
| **Источник**       | `s3://prod/raw/earthquake/<date>/`  |
| **Приемник**       | `ods.fct_earthquake` в PostgreSQL   |
| **Формат данных**  | Parquet → PostgreSQL-таблица        |
"""

SHORT_DESCRIPTION = "Перенос данных о землетрясениях из S3 (MinIO) в PostgreSQL DWH"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=30),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Start load for dates: {start_date}/{end_date}")

    con = duckdb.connect()

    try:
        # S3
        con.sql("INSTALL httpfs;")
        con.sql("LOAD httpfs;")
        con.execute("SET s3_url_style = 'path'")
        con.execute("SET s3_endpoint = 'minio:9000'")
        con.execute("SET s3_access_key_id = ?", [ACCESS_KEY])
        con.execute("SET s3_secret_access_key = ?", [SECRET_KEY])
        con.sql("SET s3_use_ssl = FALSE;")
        con.sql("SET TIMEZONE='UTC';")

        # PostgreSQL
        con.sql("INSTALL postgres;")
        con.sql("LOAD postgres;")
        con.execute(
            "ATTACH ? AS pg_db (TYPE postgres)",
            [f"dbname=test user=user password={PASSWORD} host=postgres port=5432"]
        )

        parquet_path = f"s3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet"

        # Проверка, есть ли данные
        cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
        if cnt == 0:
            logging.warning(f"No data for {start_date}, skipping insert")
            return

        insert_sql = f"""
            INSERT INTO pg_db.{SCHEMA}.{TARGET_TABLE} (...)
            SELECT ...
            FROM read_parquet('{parquet_path}');
        """
        con.sql(insert_sql)

    except Exception as e:
        logging.error(f"Error loading data for {start_date}: {e}")
        raise
    finally:
        con.close()

    logging.info(f"Данные за {start_date} успешно загружены в PostgreSQL.")



with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  
        poke_interval=60, 
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end

