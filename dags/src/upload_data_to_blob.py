import logging
import pandas as pd
from io import BytesIO
from datetime import datetime
from src.config import credentials
from azure.storage.blob import BlobServiceClient
from src.logger_config import setup_logger


# Initialize the logger for save_data.py
logger = setup_logger(
    name=__name__,
    log_file='./logs/save_data.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


def upload_to_blob(file_content: bytes, filename: str, account_url: str, credentials, container_name: str) -> None:
    """
    Uploads a file to Azure Blob Storage.

    Args:
        file_content (bytes): The content of the file to upload (in bytes).
        filename (str): The name of the file to save in the blob container.
        account_url (str): The URL of the Azure Blob Storage account.
        credentials: The credentials for accessing the Blob Storage (SAS token, Account Key, etc.).
        container_name (str): The name of the container where the file will be uploaded.
    """
    try:
        # Initialize the BlobServiceClient using the account URL and credentials
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credentials)
        
        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Get the blob client for the specific file
        blob_client = container_client.get_blob_client(filename)
        
        # Upload the file content to Blob Storage
        blob_client.upload_blob(file_content, overwrite=True)
        logger.info(f'{filename} uploaded to blob storage successfully.')

    except Exception as e:
        logger.error(f"Error uploading {filename} to blob storage: {e}")


def upload_weather_data(dataframe: pd.DataFrame, account_url: str, container_name: str) -> None:
    """
    Save the weather data as a Parquet file and upload it to Azure Blob Storage.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing weather data for all locations.
        account_url (str): The URL of the Azure Blob Storage account.
        container_name (str): The Azure Blob Storage container name.
    """
    try:
        # Check if the DataFrame is empty before proceeding
        if dataframe.empty:
            logger.warning("No current weather data to save.")
            return

        # Generate a timestamp for the current time
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create a filename with timestamp
        filename = f"weather_data_{timestamp}.parquet"

        # Convert DataFrame to Parquet in memory (as bytes)
        buffer = BytesIO()
        dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)  # Reset buffer position to the beginning

        logger.info("Uploading weather data to Azure Blob Storage...")

        # Upload to Azure Blob Storage
        upload_to_blob(buffer.getvalue(), filename, account_url, credentials, container_name)
    
    except Exception as e:
        logger.error(f"Error saving current weather data to blob storage: {e}")
