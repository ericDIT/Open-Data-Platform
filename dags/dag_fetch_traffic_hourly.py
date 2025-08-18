from airflow.decorators import dag, task

import datetime
from datetime import timedelta

from include.extraction.traffic_api import fetch_traffic_data
from include.loading.loading_traffic_data_to_db import load_raw_traffic_data_to_pgsql

POSTGRES_CONN_ID = "postgres_traffic_conn"
STAGING_SCHEMA = "staging"
RAW_TRAFFIC_MAIN_TABLE = "raw_traffic_data"
RAW_TRAFFIC_COORD_TABLE = "raw_coord_data"

#Temporary Solution for lat/lon:
lat_lon = "52.52,13.41"


@dag(
    dag_id="dag_traffic_hourly",
    start_date=datetime.datetime(2025,8,1),
    schedule_interval='0 */1 * * *',
    catchup=False,
    tags=["traffic","elt","hourly"]
)
def hourly_traffic():

    @task(retries=3, retry_delay=timedelta(minutes=5))
    def extract_traffic_data():
        print("fetching traffic data...")
        csv_path_main_data, csv_path_coord_data = fetch_traffic_data(lat_lon)
        if csv_path_main_data and csv_path_coord_data:
            print(f"Extraction completed to: {csv_path_main_data} and {csv_path_coord_data}")
            return {
                'main_path': csv_path_main_data,
                'coord_path': csv_path_coord_data
            }
        else:
            print("Error occured trying to extract traffic data")
            raise ValueError("Traffic data extraction failed")
        
    @task(retries=3)
    def load_raw_traffic_data_task(paths: dict):
        main = paths['main_path']
        coord = paths['coord_path']
        print("loading raw traffic data to database...")
        load_raw_traffic_data_to_pgsql(main,coord,POSTGRES_CONN_ID,STAGING_SCHEMA,RAW_TRAFFIC_MAIN_TABLE,RAW_TRAFFIC_COORD_TABLE)
        print(f"Data loaded to Postgresql table: '{STAGING_SCHEMA}.{RAW_TRAFFIC_MAIN_TABLE}' and table '{STAGING_SCHEMA}.{RAW_TRAFFIC_COORD_TABLE}'")

    x_com_paths = extract_traffic_data()
    raw_data_loaded = load_raw_traffic_data_task(paths=x_com_paths)

    x_com_paths >> raw_data_loaded

hourly_traffic()