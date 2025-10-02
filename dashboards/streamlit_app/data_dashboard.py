import streamlit as st
import psycopg2
import pandas as pd
import os

DB_HOST = os.getenv()


conn = psycopg2.connect(
    dbname="staging",
    user="airflow",
    password="airflow",
    host="localhost",   # oder "db" wenn dein Code im Docker-Container läuft
    port=5432
)