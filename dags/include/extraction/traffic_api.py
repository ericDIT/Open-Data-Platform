import logging
import os
import requests
import pandas as pd
from datetime import datetime, timezone

log = logging.getLogger(__name__)

def fetch_traffic_data(city: str, lat_lon_point: str):

    api_key=os.getenv("TOMTOM_API_KEY")

    if not api_key:
        log.error("Error occured with trying to load api-key, please check .env-file")
        raise ValueError("Error occured with trying to load api-key, please check .env-file")
    
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={api_key}&point={lat_lon_point}&unit=kmph&thickness=10&openLr=false"

    try:
        response = requests.get(url,timeout=10)
        response.raise_for_status()
        raw_data = response.json()
    except requests.exceptions.RequestException as error_message:
        log.error(f"Request failed: {error_message}")
        return None

    try:

        if not raw_data or 'flowSegmentData' not in raw_data:
            log.warning("No flow segment data found in response")
            return None

        segment_data = raw_data['flowSegmentData']
        all_data = []

        static_data = {
            'frc': raw_data.get('frc'),
            'currentSpeed': raw_data.get('currentSpeed'),
            'freeFlowSpeed': raw_data.get('freeFlowSpeed'),
            'currentTravelTime': raw_data.get('currentTravelTime'),
            'freeFlowTravelTime': raw_data.get('freeFlowTravelTime'),
            'confidence': raw_data.get('confidence'),
            'roadClosure': raw_data.get('roadClosure'),
        }

        if 'coordinates' not in segment_data or 'coordinate' not in segment_data['coordinates']:
            log.warning("No coordinates found in segment data")
            return None

        coordinates = segment_data['coordinates']['coordinate']

        for coord in coordinates:
            row = static_data.copy()
            row['latitude'] = coord.get('latitude')
            row['longitude'] = coord.get('longitude')
            all_data.append(row)
    
        df = pd.DataFrame(all_data)
        df['extraction_timestamp_utc'] = pd.to_datetime(datetime.now(timezone.utc))

        #Here I'm saving the dataframe to a csv file
        target_container_data_path = "/opt/airflow/data/traffic_data"
        os.makedirs(target_container_data_path, exist_ok=True)
        timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_UTC')
        csv_file_name = f"{city}_traffic_{timestamp_str}.csv"
        csv_file_path = os.path.join(target_container_data_path, csv_file_name)
        df.to_csv(csv_file_path, index=False)
        log.info(f'Data saved to {csv_file_path}')
        return csv_file_path
    
    except KeyError as k:
        log.error(f"KeyError trying to convert the raw traffic data to csv with key: {k} and raw data: {raw_data}")
        return None
    except Exception as e:
        log.error(f"Unexpected Error trying to convert the raw traffic data to csv: {e}")
        return None