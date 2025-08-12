import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "sergei"
DAG_ID = "raw_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_count_day_earthquake"

PG_CONNECT = "postgres_user"

LONG_DESCRIPTION = """
## DAG для построения витрины средней магнитуды землетрясений по дням

### Назначение
Этот DAG агрегирует данные о землетрясениях из слоя ODS (Operational Data Store) в слой DM (Data Mart),
вычисляя среднюю магнитуду событий за каждый день.  
Полученная витрина используется для аналитики сейсмической активности, трендов и мониторинга.

### Логика работы
1. Ожидание завершения DAG загрузки ODS-слоя (`raw_s3_to_pg`) через сенсор.
2. Создание временной таблицы (staging) со средней магнитудой (`avg(mag)`) за конкретную дату.
3. Очистка целевой витрины от данных за эту дату для избежания дубликатов.
4. Загрузка агрегированных данных в витрину (`dm.fct_count_day_earthquake`).
5. Удаление временной таблицы.
"""

SHORT_DESCRIPTION = "Агрегация данных о землетрясениях из ODS в витрину DM со средней магнитудой событий за день"


args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=30),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["dm", "pg"],
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
        external_dag_id="raw_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            time::date AS date,
            avg(mag::float)
        FROM
            ods.fct_earthquake
        WHERE
            time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1
        """,
    )

    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN
        (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """,
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    (
            start >>
            sensor_on_raw_layer >>
            drop_stg_table_before >>
            create_stg_table >>
            drop_from_target_table >>
            insert_into_target_table >>
            drop_stg_table_after >>
            end
    )