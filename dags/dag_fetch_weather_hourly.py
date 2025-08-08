from airflow.decorators import dag, task

import datetime
from datetime import timedelta

from include.extraction.weather_api import fetch_weather_data
from include.loading.loading_weather_data_to_db import load_raw_weather_data_to_pgsql

POSTGRES_CONN_ID = "postgres_weather_conn"
STAGING_SCHEMA = "staging"
RAW_WEATHER_TABLE = "raw_weather_data"
HOURLY_FACTS_TABLE = "weather_facts_hourly"

@dag(
    dag_id="dag_weather_hourly",
    start_date=datetime.datetime(2025,8,1),
    schedule_interval='0 */1 * * *',
    catchup=False,
    tags=["weather","elt","hourly"]
)
def hourly_weather():

    @task(retries=3, retry_delay=timedelta(minutes=5))
    def extract_weather_data():
        print("fetching weather data...")
        csv_path = fetch_weather_data()
        if csv_path:
            print(f"Extraction completed to: {csv_path}")
            return csv_path
        else:
            print("Error occured trying to extract weather data")
            raise ValueError("Weather data extraction failed")
        
    @task(retries=3)
    def load_raw_weather_data_task(csv_file_path: str):
        print("loading raw weather data to database...")
        load_raw_weather_data_to_pgsql(csv_file_path,POSTGRES_CONN_ID,STAGING_SCHEMA,RAW_WEATHER_TABLE)
        print(f"Data loaded to Postgresql table: '{STAGING_SCHEMA}.{RAW_WEATHER_TABLE}'")

    path_to_csv_xcom = extract_weather_data()
    raw_data_loaded = load_raw_weather_data_task(csv_file_path=path_to_csv_xcom)

    raw_data_loaded

hourly_weather()