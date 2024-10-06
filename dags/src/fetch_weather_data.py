import logging
import requests
from src.logger_config import setup_logger


# Initialize the logger for fetch_weather_data.py
logger = setup_logger(
    name=__name__,
    log_file='./logs/fetch_weather_data.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_weather_data(lat: str, lon: str, api_key: str, base_url: str) -> dict:
    """
    Fetch weather data for a given latitude and longitude, including current, hourly, and daily forecasts.

    Args:
        lat (str): The latitude of the location.
        lon (str): The longitude of the location.
        api_key (str): The OpenWeather API key.
        base_url (str): The base URL for the OpenWeather API.

    Returns:
        dict: A dictionary containing the complete weather data for the specified location, including current, hourly, and daily forecasts.
              Returns None if the request fails.
    """
    try:
        # Fetch all available weather data (current, hourly, daily)
        url = f"{base_url}?lat={lat}&lon={lon}&appid={api_key}"
        
        # Make the GET request to the OpenWeather API
        response = requests.get(url)
        
        # Raise an error if the request was not successful (4xx, 5xx HTTP status codes)
        response.raise_for_status()
        
        # Log success and return the weather data as JSON
        logger.info(f"Successfully fetched complete weather data for lat: {lat}, lon: {lon}")
        return response.json()
    
    except requests.exceptions.RequestException as e:
        # Log an error if the request failed
        logger.error(f"Error fetching complete data for lat: {lat}, lon: {lon} - {e}")
        return None
