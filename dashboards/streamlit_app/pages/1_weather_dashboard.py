import streamlit as st
from Home import load_data_from_db

st.title("🌤️ Weather-data dashboard")

df = load_data_from_db("staging.raw_weather_data")
st.write("Daten aus der Datenbank")
st.dataframe(df)

if "temperature" in df.columns:
    st.line_chart(df["temperature"])