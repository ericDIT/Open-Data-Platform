from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task

import datetime

from include.extraction.weather_api import fetch_weather_data
from include.loading.loading_data_to_db import load_raw_data_to_pgsql_docker

POSTGRES_CONN_ID = "postgres_weather_conn"
STAGING_SCHEMA = "staging"
RAW_WEATHER_TABLE = "raw_weather_data"
DAILY_FACTS_TABLE = "weather_facts_daily"

@dag(
    dag_id="dag_weather",
    start_date=datetime.datetime(2025, 8, 2),
    schedule_interval=None,
    catchup=False,
    tags=["weather","elt"]
)
def weather_elt_pipeline():
    
    @task
    def extract_weather_data():
        print("fetching weather data...")
        csv_path = fetch_weather_data()
        if csv_path:
            print(f"Extraction completed to: {csv_path}")
            return csv_path
        else:
            print("Error occured trying to extract weather data")
            raise ValueError("Weather data extraction failed")

    @task
    def load_raw_weather_data_task(csv_file_path: str):
        print("loading raw weather data to database...")
        load_raw_data_to_pgsql_docker(csv_file_path,POSTGRES_CONN_ID,STAGING_SCHEMA,RAW_WEATHER_TABLE)
        print(f"Data loaded to Postgresql table: '{STAGING_SCHEMA}.{RAW_WEATHER_TABLE}'")

    transform_daily_weather_facts_task = PostgresOperator(
        task_id="transform_daily_weather_facts_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="include/transformation/sql/transform_weather.sql",
        database="airflow"
    )

    path_to_csv_xcom = extract_weather_data()
    raw_data_loaded = load_raw_weather_data_task(csv_file_path=path_to_csv_xcom)
    raw_data_loaded >> transform_daily_weather_facts_task

weather_elt_pipeline()