from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag

import datetime
from datetime import timedelta

POSTGRES_CONN_ID = "postgres_weather_conn"

@dag(
    dag_id="dag_weather_every_3h",
    start_date=datetime.datetime(2025, 8, 1),
    schedule_interval="0 */3 * * *",
    catchup=False,
    tags=["weather","elt","3h"]
)
def transforming_weather_every_3h():

    transform_weather_facts_task = PostgresOperator(
        task_id="transform_weather_facts_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="include/transformation/sql/transform_weather_3h.sql",
        database="airflow",
        retries=3,
        retry_delay=timedelta(minutes=10)
    )
    
    transform_weather_facts_task

transforming_weather_every_3h()