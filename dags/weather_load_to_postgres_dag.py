import logging
import pendulum
import pandas as pd
import boto3
import io

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from sqlalchemy import create_engine, text

OWNER = "Timmy-DE"
DAG_ID = "weather_load_to_postgres_dag"
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
def create_table_if_not_exists() -> None:
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres_dwh:5432/postgres"
    )
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS weather_data (
                measured_at     TIMESTAMP NOT NULL,
                temperature     FLOAT,
                partition_date  DATE NOT NULL,
                UNIQUE (measured_at)
            )
        """))
    logging.info("✅ Table created or already exists")


@task
def load_to_postgres(dates: dict) -> None:
    start_date = dates["start_date"]
    access_key = Variable.get("access_key")
    secret_key = Variable.get("secret_key")

    s3 = boto3.client("s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    obj = s3.get_object(
        Bucket="weather-staging",
        Key=f"{LAYER_STAGING}/{SOURCE}/{start_date}/{start_date}.gz.parquet"
    )
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    logging.info(f"📦 Loaded {len(df)} rows from staging")

    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres_dwh:5432/postgres"
    )
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO weather_data (measured_at, temperature, partition_date)
                VALUES (:measured_at, :temperature, :partition_date)
                ON CONFLICT (measured_at) DO UPDATE
                    SET temperature     = EXCLUDED.temperature,
                        partition_date  = EXCLUDED.partition_date
            """), {
                "measured_at": row["measured_at"],
                "temperature": row["temperature"],
                "partition_date": row["partition_date"],
            })
    logging.info(f"✅ Loaded {len(df)} rows to PostgreSQL for {start_date}")

@task
def create_mart_table() -> None:
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres_dwh:5432/postgres"
    )
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS weather_daily_summary (
                partition_date  DATE NOT NULL,
                avg_temp        FLOAT,
                max_temp        FLOAT,
                min_temp        FLOAT,
                hours_count     INT,
                UNIQUE (partition_date)
            )
        """))
    logging.info("✅ Mart table created or already exists")


@task
def create_mart(dates: dict) -> None:
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres_dwh:5432/postgres"
    )
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO weather_daily_summary
                (partition_date, avg_temp, max_temp, min_temp, hours_count)
            SELECT
                partition_date,
                round(avg(temperature)::numeric, 1),
                max(temperature),
                min(temperature),
                count(*)
            FROM weather_data
            WHERE partition_date = :partition_date
            GROUP BY partition_date
            ON CONFLICT (partition_date) DO UPDATE
                SET avg_temp    = EXCLUDED.avg_temp,
                    max_temp    = EXCLUDED.max_temp,
                    min_temp    = EXCLUDED.min_temp,
                    hours_count = EXCLUDED.hours_count
        """), {"partition_date": dates["start_date"]})
    logging.info(f"✅ Mart updated for {dates['start_date']}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 7 * * *",
    default_args=args,
    tags=["postgres", "mart"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dates = get_dates()

    wait_for_staging = S3KeySensor(
        task_id="wait_for_staging",
        bucket_name="weather-staging",
        bucket_key=f"{LAYER_STAGING}/{SOURCE}/{{{{ ds }}}}/{{{{ ds }}}}.gz.parquet",
        aws_conn_id="minio_conn",
        timeout=3600,
        poke_interval=60,
    )

    create_table = create_table_if_not_exists()
    create_mart_table = create_mart_table()
    load = load_to_postgres(dates)
    create_mart = create_mart(dates)

start >> dates >> wait_for_staging >> create_table >> create_mart_table >> load >> create_mart >> end