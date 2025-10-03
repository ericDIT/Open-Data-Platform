
import psycopg2
from psycopg2 import sql

import os
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

def load_raw_traffic_data_to_pgsql(
        main_path: str, 
        coord_path: str, 
        postgres_conn_id: str, 
        schema: str, 
        main_traffic_table: str, 
        coord_traffic_table: str
):

    conn=None
    cur=None

    if not os.path.exists(main_path):
        log.error(f"Could not find the csv at: {main_path}")
        raise FileNotFoundError(f"Could not find the csv at: {main_path}")
    
    if not os.path.exists(coord_path):
        log.error(f"Could not find the csv at: {coord_path}")
        raise FileNotFoundError(f"Could not find the csv at: {coord_path}")

    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        log.info(f"Connected to Postgresql with connection ID: {postgres_conn_id}")

        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema)))
        conn.commit()
        log.info(f"Ensured that schema: {schema} exists.")

        full_table_name_main = sql.SQL("{}.{}").format(sql.Identifier(schema),sql.Identifier(main_traffic_table))
        full_table_name_coord = sql.SQL("{}.{}").format(sql.Identifier(schema),sql.Identifier(coord_traffic_table))

        create_table_sql_main = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id UUID PRIMARY KEY,
                city VARCHAR(255),
                frc VARCHAR(10),
                currentSpeed NUMERIC,
                freeFlowSpeed NUMERIC,
                currentTravelTime NUMERIC,
                freeFlowTravelTime NUMERIC,
                confidence INTEGER,
                roadClosure BOOLEAN,
                extraction_timestamp_utc TIMESTAMP WITH TIME ZONE
            );
        """).format(full_table_name_main)

        create_table_sql_coord = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                latitude DECIMAL(10,8),
                longitude DECIMAL(11,8),
                extraction_timestamp_utc TIMESTAMP WITH TIME ZONE,
                traffic_data_id UUID REFERENCES {}(id)
            );
        """).format(full_table_name_coord,full_table_name_main)

        log.info(f"Ensuring table '{main_traffic_table}' and '{coord_traffic_table}' in schema '{schema}' exists correctly")
        cur.execute(create_table_sql_main)
        cur.execute(create_table_sql_coord)
        conn.commit()
        log.info("Tables successfully created / already exist")

        log.info(f"Loading data from {main_path} into {full_table_name_main}")
        with open(main_path, 'r', newline='', encoding='utf-8') as f:
            copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV,HEADER, NULL '')").format(full_table_name_main)
            cur.copy_expert(copy_sql, f)

        log.info(f"Loading data from {coord_path} into {full_table_name_coord}")
        with open(coord_path, 'r', newline='', encoding='utf-8') as f:
            copy_sql = sql.SQL("COPY {}(latitude, longitude, extraction_timestamp_utc, traffic_data_id) FROM STDIN WITH (FORMAT CSV, HEADER, NULL '')").format(full_table_name_coord)
            cur.copy_expert(copy_sql, f)

        conn.commit()
        log.info("ALL Data successfully loaded and committed")

        try:
            if os.path.exists(main_path):
                os.remove(main_path)
            if os.path.exists(coord_path):
                os.remove(coord_path)
            log.info(f"CSV files: {main_path} and {coord_path} successfully removed from directory")
        except Exception as delete_error:
            log.warning(f"Csv could not be delete after loading data to db: {delete_error}")
    except (FileNotFoundError, psycopg2.Error, Exception) as e:
        if conn:
            conn.rollback()
        log.error(f"An error occurred while loading data: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()        