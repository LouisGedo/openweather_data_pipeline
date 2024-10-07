import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict
from src.config import get_secret
from airflow.decorators import task, dag
from src.logger_config import setup_logger
from src.load_locations import load_locations
from src.upload_data_to_blob import upload_weather_data
from src.process_weather_data import fetch_convert_combine

# Initialize logger for the DAG
logger = setup_logger(
    name=__name__,
    log_file="./logs/dag.log",  # Log file for the DAG
    level=logging.INFO
)

# Define the basic parameters of the DAG
@dag(
    start_date=datetime(2024, 10, 1),
    schedule="@hourly",  # Schedule to run every hour
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["weather_pipeline"],
    params={
        "api_secret_name": "OpenWeatherApiKey",
        "blob_url_secret_name": "AzureStorageUrl",
        "locations_file": "locations.json",
        "container_name": "openweather"
    }
)

def weather_data_pipeline():
    """
    ### Weather Data Pipeline

    This DAG retrieves the API key and Azure Blob Storage URL from Azure Key Vault, 
    loads location data from a JSON file using `load_locations.py`, processes the weather data,
    and uploads the combined data to Azure Blob Storage.

    #### Steps:
    1. Retrieve API key and Blob Storage URL from Azure Key Vault.
    2. Load locations from a JSON file.
    3. Fetch, process, and combine weather data for all locations.
    4. Upload the combined weather data to Azure Blob Storage.
    """

    @task
    def retrieve_secrets(api_secret_name: str, blob_url_secret_name: str) -> dict:
        """
        Task to retrieve secrets from Azure Key Vault.

        Args:
            api_secret_name (str): The name of the API key secret in Azure Key Vault.
            blob_url_secret_name (str): The name of the Blob Storage URL secret in Azure Key Vault.

        Returns:
            dict: A dictionary containing the API key and Blob URL if successful, or raises an error.
        """
        logger.info("Starting secret retrieval from Azure Key Vault.")

        # Retrieve secrets from Azure Key Vault
        API_KEY = get_secret(api_secret_name)
        AZURE_STORAGE_URI = get_secret(blob_url_secret_name)

        # Check if secrets were successfully retrieved
        if not API_KEY or not AZURE_STORAGE_URI:
            logger.error(f"Failed to retrieve API_KEY or AZURE_STORAGE_URI from Azure Key Vault.")
            raise ValueError("Failed to retrieve API_KEY or AZURE_STORAGE_URI from Azure Key Vault.")
        
        logger.info("Successfully retrieved secrets from Azure Key Vault.")
        return {"API_KEY": API_KEY, "AZURE_STORAGE_URI": AZURE_STORAGE_URI}

    @task
    def load_location_data(locations_file: str) -> List[Dict[str, str]]:
        """
        Task to load location data from a JSON file.

        Args:
            locations_file (str): The path to the JSON file containing location data.

        Returns:
            List[Dict[str, str]]: A list of locations (latitude and longitude).
        """
        logger.info(f"Loading locations from {locations_file}.")
        locations = load_locations(locations_file)
        if locations:
            logger.info(f"Successfully loaded {len(locations)} locations.")
        else:
            logger.error(f"Failed to load locations from {locations_file}.")
            raise ValueError(f"Failed to load locations from {locations_file}.")
        return locations

    @task
    def process_weather_data(locations: List[Dict[str, str]], secrets: dict) -> pd.DataFrame:
        """
        Task to fetch and process weather data for all locations.

        Args:
            locations (list): List of location dictionaries containing latitudes and longitudes.
            secrets (dict): A dictionary containing API key and Blob Storage URL.
        
        Returns:
            pd.DataFrame: A DataFrame containing the combined weather data for all locations.
        """
        API_KEY = secrets["API_KEY"]
        BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

        logger.info(f"Processing weather data for {len(locations)} locations.")
        
        # Fetch and process weather data using `fetch_convert_combine`
        weather_data_df = fetch_convert_combine(locations, API_KEY, BASE_URL)

        # Log the process
        if not weather_data_df.empty:
            logger.info(f"Successfully processed weather data for {len(locations)} locations.")
        else:
            logger.warning("No weather data could be processed.")
            raise ValueError("No weather data was fetched.")
        
        return weather_data_df

    @task
    def upload_weather_data_to_blob(weather_data_df: pd.DataFrame, secrets: dict, container_name: str):
        """
        Task to upload the combined weather data to Azure Blob Storage.

        Args:
            weather_data_df (pd.DataFrame): The combined weather data DataFrame.
            secrets (dict): A dictionary containing API key and Blob Storage URL.
            container_name (str): The Azure Blob Storage container name.
        """
        AZURE_STORAGE_URI = secrets["AZURE_STORAGE_URI"]

        logger.info("Uploading weather data to Azure Blob Storage.")
        
        # Upload the combined weather data to Azure Blob Storage
        upload_weather_data(weather_data_df, AZURE_STORAGE_URI, container_name)
        logger.info("Successfully uploaded weather data to Azure Blob Storage.")

    # Get parameters from Airflow variables or defaults
    api_secret_name = "{{ params.api_secret_name }}"
    blob_url_secret_name = "{{ params.blob_url_secret_name }}"
    locations_file = "{{ params.locations_file }}"
    container_name = "{{ params.container_name }}"

    # Execute the secrets retrieval task
    secrets = retrieve_secrets(
        api_secret_name=api_secret_name,
        blob_url_secret_name=blob_url_secret_name
    )

    # Load location data after secrets are retrieved
    locations = load_location_data(locations_file=locations_file)

    # Process the weather data after locations are loaded
    weather_data = process_weather_data(locations=locations, secrets=secrets)

    # Upload the processed weather data to Azure Blob Storage
    upload_to_blob = upload_weather_data_to_blob(weather_data, secrets, container_name=container_name)

    # Set task dependencies
    secrets >> locations >> weather_data >> upload_to_blob

# Instantiate the DAG
weather_data_pipeline()
