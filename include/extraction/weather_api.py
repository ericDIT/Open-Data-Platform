import requests
import os

def fetch_weather_data():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    city = os.getenv("CITY")

    if not api_key or city:
        raise ValueError("API-Key and City not set, please check your .env file")

    #in C instead of K units
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() #check if error occurs
        data = response.json()
        print("Weather data for {city}:")
        print(data)
        return data
    except requests.exceptions.RequestException as error_message:
        print(f"Request failed: {error_message}")
        return None
