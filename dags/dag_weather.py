from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from include.extraction import weather_api
import os

def data_fetch():
    print("fetching weather data...")
    data = weather_api.fetch_weather_data()

with DAG(
    dag_id="dag_weather",
    start_date=datetime.datetime(2025, 8, 2),
    schedule_interval=None,
    catchup=False,
    tags=["weather","openweather"]
) as dag:

    task_for_weather_extraction = PythonOperator(
        task_id='weather_fetch',
        python_callable=data_fetch
    )

    task_for_weather_extraction