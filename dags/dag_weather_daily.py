from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.decorators import dag

import datetime

POSTGRES_CONN_ID = "postgres_weather_conn"

def last_hourly_execution_date(daily_exectuion_date):
    return daily_exectuion_date.replace(hour=23)

@dag(
    dag_id="dag_weather_daily",
    start_date=datetime.datetime(2025, 8, 1),
    schedule_interval="30 23 * * *",
    catchup=False,
    tags=["weather","elt","daily"]
)
def transforming_daily_weather():

    wait_for_last_hourly_fetch = ExternalTaskSensor(
    task_id="wait_for_last_hourly_fetch",
    external_dag_id="dag_weather_hourly",
    external_task_id="load_raw_weather_data_task",
    execution_date_fn=last_hourly_execution_date,
    mode="reschedule",
    poke_interval=60,
    timeout=55*60,
    allowed_states=["success"],
    failed_states=["failed", "skipped"]
    )

    transform_daily_weather_facts_task = PostgresOperator(
        task_id="transform_daily_weather_facts_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="include/transformation/sql/transform_weather_daily.sql",
        database="airflow"
    )
    
    wait_for_last_hourly_fetch>>transform_daily_weather_facts_task

daily_weather_elt_pipeline()