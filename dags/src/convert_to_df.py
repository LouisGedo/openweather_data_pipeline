import logging
import pandas as pd
from src.logger_config import setup_logger


# Initialize the logger for convert_to_df.py
logger = setup_logger(
    name=__name__,
    log_file='./logs/convert_to_df.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


def convert_to_dataframe(weather_data: dict, lat: str, lon: str) -> pd.DataFrame:
    """
    Converts the current weather data into a pandas DataFrame using json_normalize.

    Args:
        weather_data (dict): The weather data received from the OpenWeather API.
        lat (str): Latitude of the location.
        lon (str): Longitude of the location.

    Returns:
        pd.DataFrame: A DataFrame containing the current weather data, including the flattened 'weather' field.
    """
    try:
        # Step 1: Flatten the entire JSON structure from the top layer
        df = pd.json_normalize(weather_data, sep='_')

        # Step 2: Flatten the 'weather' column separately
        weather_df = pd.json_normalize(df['weather'].explode())

        # Step 3: Add the 'weather' prefix to the columns in weather_df
        weather_df = weather_df.add_prefix('weather_')

        # Step 4: Concatenate the flattened weather columns back into the main DataFrame
        df = pd.concat([df.drop(columns=['weather']), weather_df], axis=1)

        # Log success
        logger.info(f"Successfully converted weather data to DataFrame for lat: {lat}, lon: {lon}")
        return df

    except Exception as e:
        logger.error(f"Error converting weather data for lat: {lat}, lon: {lon} to DataFrame: {e}")
        # Return empty DataFrame in case of error
        return pd.DataFrame()