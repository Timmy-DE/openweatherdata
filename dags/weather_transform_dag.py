import logging
import pendulum
import duckdb
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable


OWNER = "Timmy-DE"
DAG_ID = "weather_transform_dag"
LAYER_RAW = "weather-raw"
LAYER_STAGING = "weather-staging"
SOURCE = "openweatherdata"
LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""
SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

@task
def get_dates(**context) -> dict:
    return {
        "start_date": context["data_interval_start"].format("YYYY-MM-DD"),
        "end_date": context["data_interval_end"].format("YYYY-MM-DD"),
    }
    


@task 
def transform_data(dates: dict) -> None:
    start_date = dates["start_date"]
    access_key = Variable.get("access_key")
    secret_key = Variable.get("secret_key")

    con = duckdb.connect()
    con.sql(f"""
    SET TIMEZONE='UTC';
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_url_style = 'path';
    SET s3_endpoint = 'minio:9000';
    SET s3_access_key_id = '{access_key}';
    SET s3_secret_access_key = '{secret_key}';
    SET s3_use_ssl = FALSE;
    COPY (
        SELECT * FROM (
            SELECT
                unnest(hourly['time'])::TIMESTAMP          AS measured_at,
                unnest(hourly['temperature_2m'])::FLOAT    AS temperature,
                '{start_date}'::DATE                       AS partition_date
            FROM read_parquet(
                's3://weather-raw/{LAYER_RAW}/{SOURCE}/{start_date}/{start_date}.gz.parquet'
            )
        ) WHERE temperature IS NOT NULL
    ) TO 's3://weather-staging/{LAYER_STAGING}/{SOURCE}/{start_date}/{start_date}.gz.parquet';
    """)
    con.close()
    logging.info(f"✅ Transform done for {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 6 * * *",
    default_args=args,
    tags=["s3", "staging"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dates = get_dates()

    wait_for_raw = S3KeySensor(
        task_id="wait_for_raw",
        bucket_name="weather-raw",
        bucket_key=f"weather-raw/{SOURCE}/{{{{ ds }}}}/{{{{ ds }}}}.gz.parquet",
        aws_conn_id="minio_conn",
        timeout=3600,
        poke_interval=60,
    )

    transform_task = transform_data(dates)

    start >> dates >> wait_for_raw >> transform_task >> end