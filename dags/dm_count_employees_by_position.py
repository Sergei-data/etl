
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from pyspark.sql import SparkSession
from clickhouse_driver import Client

OWNER = "sergei"
PG_CONNECT = "clickhouse_connection"  
BUCKET = "test"
LAYER = "raw"
SOURCE = "earthquake"

ACCESS_KEY = "aQs5dExHDTcAr3teiz5U"
SECRET_KEY = "ctgrmAMKE1itQ95TWO5OZclnaO6qqNyh4cdP1EX1"
S3_ENDPOINT = "http://localhost:9000"  

CLICKHOUSE_HOST = "localhost"


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 8, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=30),
}

def spark_read_earthquake_data(execution_date, **context):
    spark = (
        SparkSession.builder.appName("ReadEarthquakeData")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    date_str = execution_date.format("YYYY-MM-DD")
    s3_path = f"s3a://{BUCKET}/{LAYER}/{SOURCE}/{date_str}/{date_str}_00-00-00.parquet"
    df = spark.read.parquet(s3_path)
    df.createOrReplaceTempView("earthquake_raw")

    # сохраняем датафрейм в XCom, чтобы передать дальше (нужно сериализовать, но для упрощения — пишем в отдельный файл)
    output_path = f"/tmp/earthquake_{date_str}.parquet"
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

    return output_path

def aggregate_count(execution_date, **context):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("CountEarthquake").getOrCreate()
    date_str = execution_date.format("YYYY-MM-DD")
    input_path = f"/tmp/earthquake_{date_str}.parquet"
    df = spark.read.parquet(input_path)

    count_df = df.groupBy("time").count()
    count_df.createOrReplaceTempView("count_view")

    # Запишем в ClickHouse
    client = Client(host=CLICKHOUSE_HOST)
    # Создаем таблицу если нужно
    client.execute("""
        CREATE TABLE IF NOT EXISTS dm_count_day_earthquake (
            time DateTime,
            count UInt64
        ) ENGINE = MergeTree()
        ORDER BY time
    """)

    # Преобразуем датафрейм в список кортежей для вставки
    data = [(row.time, row['count']) for row in count_df.collect()]
    client.execute("INSERT INTO dm_count_day_earthquake (time, count) VALUES", data)

    spark.stop()

def aggregate_avg_mag(execution_date, **context):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("AvgMagnitudeEarthquake").getOrCreate()
    date_str = execution_date.format("YYYY-MM-DD")
    input_path = f"/tmp/earthquake_{date_str}.parquet"
    df = spark.read.parquet(input_path)

    avg_df = df.groupBy("time").avg("mag")
    avg_df = avg_df.withColumnRenamed("avg(mag)", "avg_magnitude")
    avg_df.createOrReplaceTempView("avg_view")

    client = Client(host=CLICKHOUSE_HOST)
    client.execute("""
        CREATE TABLE IF NOT EXISTS dm_avg_day_earthquake (
            time DateTime,
            avg_magnitude Float64
        ) ENGINE = MergeTree()
        ORDER BY time
    """)

    data = [(row.time, row['avg_magnitude']) for row in avg_df.collect()]
    client.execute("INSERT INTO dm_avg_day_earthquake (time, avg_magnitude) VALUES", data)

    spark.stop()


with DAG(
    dag_id="dm_count_day_earthquake",
    schedule_interval="0 5 * * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dm", "clickhouse", "spark"],
) as dag1:
    read_task = PythonOperator(
        task_id="read_earthquake_data",
        python_callable=spark_read_earthquake_data,
        provide_context=True,
    )
    count_task = PythonOperator(
        task_id="aggregate_count",
        python_callable=aggregate_count,
        provide_context=True,
    )

    read_task >> count_task


with DAG(
    dag_id="dm_avg_day_earthquake",
    schedule_interval="0 6 * * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dm", "clickhouse", "spark"],
) as dag2:
    read_task = PythonOperator(
        task_id="read_earthquake_data",
        python_callable=spark_read_earthquake_data,
        provide_context=True,
    )
    avg_task = PythonOperator(
        task_id="aggregate_avg_magnitude",
        python_callable=aggregate_avg_mag,
        provide_context=True,
    )

    read_task >> avg_task
