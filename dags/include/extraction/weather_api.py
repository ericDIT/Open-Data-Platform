import requests
import os
import pandas as pd
from datetime import datetime, timezone
import logging

log = logging.getLogger(__name__)

def fetch_weather_data():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    city = os.getenv("CITY")

    if not api_key or not city:
        log.error("Error occured with getting City or API-Key from .env")
        raise ValueError("API-Key and City not set, please check your .env file")

    #in C instead of K units
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
    except requests.exceptions.RequestException as error_message:
        log.error(f"Request failed: {error_message}")
        return None
    
    try:
        df_data = {
            'city_name': raw_data.get('name'),
            'city_id': raw_data.get('id'),
            'lon': raw_data['coord'].get('lon'),
            'lat': raw_data['coord'].get('lat'),
            'temperature': raw_data['main'].get('temp'),
            'feels_like': raw_data['main'].get('feels_like'),
            'temp_min': raw_data['main'].get('temp_min'),
            'temp_max': raw_data['main'].get('temp_max'),
            'pressure': raw_data['main'].get('pressure'),
            'humidity': raw_data['main'].get('humidity'),
            'sea_level': raw_data['main'].get('sea_level'),
            'grnd_level': raw_data['main'].get('grnd_level'),
            'weather_id': raw_data['weather'][0].get('id') if raw_data.get('weather') else None,
            'weather_main': raw_data['weather'][0].get('main') if raw_data.get('weather') else None,
            'weather_description': raw_data['weather'][0].get('description') if raw_data.get('weather') else None,
            'weather_icon': raw_data['weather'][0].get('icon') if raw_data.get('weather') else None,
            'wind_speed': raw_data['wind'].get('speed'),
            'wind_deg': raw_data['wind'].get('deg'),
            'wind_gust': raw_data['wind'].get('gust') if raw_data.get('wind') else None,
            'clouds_all': raw_data['clouds'].get('all'),
            'visibility': raw_data.get('visibility'),
            'country': raw_data['sys'].get('country'),
            'sunrise_utc': pd.to_datetime(raw_data['sys'].get('sunrise'), unit='s', utc=True) if raw_data.get('sys') and raw_data['sys'].get('sunrise') else None,
            'sunset_utc': pd.to_datetime(raw_data['sys'].get('sunset'), unit='s', utc=True) if raw_data.get('sys') and raw_data['sys'].get('sunset') else None,
            'data_received_timestamp_utc': pd.to_datetime(raw_data['dt'], unit='s', utc=True) if raw_data.get('dt') else None
        }

        df = pd.DataFrame([df_data])
        df['extraction_timestamp_utc'] = pd.to_datetime(datetime.now(timezone.utc))

        #Here I'm saving the dataframe to a csv file
        target_container_data_path = "/opt/airflow/data/weather_data"
        os.makedirs(target_container_data_path, exist_ok=True)
        timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_UTC')
        csv_file_name = f"{city}_weather_{timestamp_str}.csv"
        csv_file_path = os.path.join(target_container_data_path, csv_file_name)
        df.to_csv(csv_file_path, index=False)
        log.info(f'Data saved to {csv_file_path}')
        return csv_file_path

    except KeyError as k:
        log.error(f"KeyError trying to convert the raw weather data to csv with key: {k} and raw data: {raw_data}")
        return None
    except Exception as e:
        log.error(f"Unexpected Error trying to convert the raw weather data to csv: {e}")
        return None