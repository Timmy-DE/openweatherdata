import json
import logging

import duckdb
import httpx
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

OWNER = "Timmy-DE"
DAG_ID = "weather_raw_to_minio_dag"
LAYER = "weather-raw"
SOURCE = "openweatherdata"
LONG_DESCRIPTION = """
# LONG DESCRIPTION––
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
def get_api_data(dates: dict) -> str:
    start_date = dates["start_date"]
    end_date = dates["end_date"]
    response = httpx.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": 55.75,
            "longitude": 37.62,
            "hourly": "temperature_2m",
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=60,
    )
    response.raise_for_status()
    logging.info(f"✅ Data fetched for {start_date}/{end_date}")
    return json.dumps(response.json())


@task
def transfer_to_minio(data: str, dates: dict) -> None:
    start_date = dates["start_date"]
    access_key = Variable.get("access_key")
    secret_key = Variable.get("secret_key")

    tmp_path = f"/tmp/{start_date}.json"
    with open(tmp_path, "w") as f:
        f.write(data)

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
            SELECT * FROM read_json_auto('{tmp_path}')
        ) TO 's3://weather-raw/{LAYER}/{SOURCE}/{start_date}/{start_date}.gz.parquet';
    """)
    con.close()
    logging.info(f"✅ Data transferred to MinIO for {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dates = get_dates()
    data = get_api_data(dates)
    transfer = transfer_to_minio(data, dates)

    start >> dates >> data >> transfer >> end
