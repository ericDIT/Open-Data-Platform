import streamlit as st
from Home import load_data_from_db

st.title("🚦 Traffic-data dashboard")

df = load_data_from_db("staging.raw_traffic_data")
st.write("Daten aus der Datenbank")
st.dataframe(df)

if "currentspeed" in df.columns:
    st.line_chart(df["currentspeed"])