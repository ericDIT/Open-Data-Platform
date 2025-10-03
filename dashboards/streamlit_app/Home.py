import streamlit as st
import pandas as pd
import psycopg2

DB_HOST = "db"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"
DB_PORT = "5432"

def load_data_from_db(query_table):
    conn = psycopg2.connect(host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS)
    query = f"SELECT * FROM {query_table}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.set_page_config(
    page_title="Weather and Traffic Dashboard",
    layout="wide",
)

st.write("# Welcome! Select a dashboard from the menu.")