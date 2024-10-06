import logging
import pandas as pd
from src.logger_config import setup_logger
from src.convert_to_df import convert_to_dataframe
from src.fetch_weather_data import fetch_weather_data


# Initialize the logger for process_weather_data.py
logger = setup_logger(
    name=__name__,
    log_file='./logs/process_weather_data.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


def fetch_convert_combine(locations: list, api_key: str, base_url: str) -> pd.DataFrame:
    """
    Fetches current weather data for multiple locations, converts them into DataFrames,
    and combines the DataFrames across all locations.

    Args:
        locations (list): A list of location dictionaries containing latitudes and longitudes.
        api_key (str): The API key for OpenWeather API.
        base_url (str): The base URL for OpenWeather API.

    Returns:
        pd.DataFrame: A combined DataFrame of current weather data for all locations.
    """
    all_data = []

    # Fetch and convert weather data for each location
    for location in locations:
        lat = location['lat']
        lon = location['lon']
        
        logger.info(f"Fetching weather data for lat: {lat}, lon: {lon}")
        
        # Fetch weather data using the API key and base URL
        weather_data = fetch_weather_data(lat, lon, api_key, base_url)
        
        # Convert the fetched data to DataFrame
        if weather_data:
            df = convert_to_dataframe(weather_data, lat, lon)
            
            if not df.empty:
                # Reset index before appending to avoid issues with reindexing
                df = df.reset_index(drop=True)
                all_data.append(df)
            else:
                logger.error(f"Failed to convert weather data to DataFrame for lat: {lat}, lon: {lon}")
        else:
            logger.error(f"Failed to fetch weather data for lat: {lat}, lon: {lon}")

    # Combine DataFrames across locations
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True, axis=0)
        logger.info(f"Combined current weather data for all locations.")
        return combined_df
    else:
        logger.warning("No data to combine.")
        # Return empty DataFrame in case there is nothing to combine
        return pd.DataFrame()
