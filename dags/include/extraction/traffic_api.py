import logging
import os
import requests
import pandas as pd
from datetime import datetime, timezone
import uuid

log = logging.getLogger(__name__)

def fetch_traffic_data(lat_lon_point: str):

    city=os.getenv("CITY")
    api_key=os.getenv("TOMTOM_API_KEY")

    # Solution is currently only supporting this zoom level will add different levels based on future goals
    zoom_level="10"

    if not api_key:
        log.error("Error occured with trying to load api-key, please check .env-file")
        raise ValueError("Error occured with trying to load api-key, please check .env-file")
    
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom_level}/json?key={api_key}&point={lat_lon_point}&unit=kmph&thickness=10&openLr=false"

    try:
        response = requests.get(url,timeout=10)
        response.raise_for_status()
        raw_data = response.json()
    except requests.exceptions.RequestException as error_message:
        log.error(f"Request failed: {error_message}")
        return None

    try:

        if 'flowSegmentData' not in raw_data:
            log.error(f"flowSegmentData not found: {raw_data}")
            return None

        flow_segment = raw_data['flowSegmentData']

        unique_id = str(uuid.uuid4())

        main_traffic_data = {
            'id': unique_id,
            'city': city,
            'frc': flow_segment.get('frc'),
            'currentSpeed': flow_segment.get('currentSpeed'),
            'freeFlowSpeed': flow_segment.get('freeFlowSpeed'),
            'currentTravelTime': flow_segment.get('currentTravelTime'),
            'freeFlowTravelTime': flow_segment.get('freeFlowTravelTime'),
            'confidence': flow_segment.get('confidence'),
            'roadClosure': flow_segment.get('roadClosure')
        }
    
        coordinates_data = flow_segment.get('coordinates', {}).get('coordinate', [])
        if not coordinates_data:
            log.warning("No coordinates found in segment data")
            return None

        timestamp = pd.to_datetime(datetime.now(timezone.utc))

        df_for_main_data = pd.DataFrame([main_traffic_data])
        df_for_coordinates = pd.DataFrame(coordinates_data)
        df_for_main_data['extraction_timestamp_utc'] = timestamp
        df_for_coordinates['extraction_timestamp_utc'] = timestamp
        df_for_coordinates['traffic_data_id'] = unique_id


        #Here I'm saving the 2 dataframes to two seperate csv files
        target_container_data_path = "/opt/airflow/data/traffic_data"
        os.makedirs(target_container_data_path, exist_ok=True)
        timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_UTC')
        main_csv_file_name = f"{city}_main_traffic_{timestamp_str}.csv"
        coord_csv_file_name = f"{city}_coord_traffic_{timestamp_str}.csv"
        main_csv_file_path = os.path.join(target_container_data_path, main_csv_file_name)
        coord_csv_file_path = os.path.join(target_container_data_path, coord_csv_file_name)
        df_for_main_data.to_csv(main_csv_file_path, index=False)
        df_for_coordinates.to_csv(coord_csv_file_path, index=False)
        log.info(f'Main data saved to {main_csv_file_path} and coordinate data saved to {coord_csv_file_path}')
        return main_csv_file_path, coord_csv_file_path
    
    except KeyError as k:
        log.error(f"KeyError trying to convert the raw traffic data to csv with key: {k} and raw data: {raw_data}")
        return None
    except Exception as e:
        log.error(f"Unexpected Error trying to convert the raw traffic data to csv: {e}")
        return None