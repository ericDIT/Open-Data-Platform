
import psycopg2
from psycopg2 import sql

import os
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

def load_raw_data_to_pgsql_docker(csv_file_path: str, postgres_conn_id: str, schema: str, table_name: str):

    conn=None
    cur=None

    if not os.path.exists(csv_file_path):
        log.error(f"Could not find the csv at: {csv_file_path}")
        raise FileNotFoundError(f"Could not find the csv at: {csv_file_path}")

    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        log.info(f"Connected to PostgreSQL with connection ID: {postgres_conn_id}")

        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema)))
        conn.commit()
        log.info(f"Ensured that schema: {schema} exists.")

        full_table_name = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table_name))

        create_sql_table = sql.SQL("""
                                   CREATE TABLE IF NOT EXISTS {} (
                                        city_name VARCHAR(255),
                                        city_id BIGINT,
                                        lon NUMERIC,
                                        lat NUMERIC,
                                        temperature NUMERIC,
                                        feels_like NUMERIC,
                                        temp_min NUMERIC,
                                        temp_max NUMERIC,
                                        pressure INTEGER,
                                        humidity INTEGER,
                                        sea_level INTEGER,
                                        grnd_level INTEGER,
                                        weather_id INTEGER,
                                        weather_main VARCHAR(255),
                                        weather_description VARCHAR(255),
                                        weather_icon VARCHAR(10),
                                        wind_speed NUMERIC,
                                        wind_deg INTEGER,
                                        wind_gust NUMERIC,
                                        clouds_all INTEGER,
                                        visibility INTEGER,
                                        country VARCHAR(5),
                                        sunrise_utc TIMESTAMP WITH TIME ZONE,
                                        sunset_utc TIMESTAMP WITH TIME ZONE,
                                        data_received_timestamp_utc TIMESTAMP WITH TIME ZONE,
                                        extraction_timestamp_utc TIMESTAMP WITH TIME ZONE
                                    );
                                   """).format(full_table_name)
        cur.execute(create_sql_table)
        conn.commit()
        log.info(f"Ensuring table '{table_name}' in schema '{schema}' exists correctly")

        with open(csv_file_path, 'r', newline='', encoding='utf-8') as f_header:
            next(f_header)

            columns = [
                'city_name','city_id','lon','lat',
                'temperature','feels_like','temp_min','temp_max',
                'pressure','humidity','sea_level','grnd_level',
                'weather_id','weather_main','weather_description','weather_icon',
                'wind_speed','wind_deg','wind_gust','clouds_all',
                'visibility','country','sunrise_utc','sunset_utc',
                'data_received_timestamp_utc','extraction_timestamp_utc'
            ]

            copy_sql = sql.SQL("COPY {} ({}) FROM  STDIN WITH CSV NULL ''").format(
                full_table_name,
                sql.SQL(', ').join(map(sql.Identifier, columns))
            )
            cur.copy_expert(copy_sql, f_header)

        conn.commit()
        log.info(f"Data successfully loaded from csv: {csv_file_path} into {schema}.{table_name}")
    except FileNotFoundError:
        raise
    except psycopg2.Error as e:
        conn.rollback()
        log.error(f"PostgreSQL error while trying to load data: {e}")
        raise
    except Exception as e:
        log.error(f"Unexxpected error occured loading the data: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


