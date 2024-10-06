import os
import json
import logging
from typing import List, Dict
from src.logger_config import setup_logger


# Initialize the logger for load_locations.py
logger = setup_logger(
    name=__name__,
    log_file='./logs/load_locations.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


def load_locations(json_file_path: str) -> List[Dict[str, str]]:
    """
    Load locations from the specified JSON file.

    Args:
        json_file_path (str): The path to the JSON file containing location data.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing latitude and longitude values.
    """
    try:
        # Directly use the /usr/local/airflow/include/ directory as base
        include_dir = "/usr/local/airflow/include"
        absolute_path = os.path.join(include_dir, json_file_path)

        # Check if the file exists before attempting to open it
        if not os.path.exists(absolute_path):
            raise FileNotFoundError(f"File not found: {absolute_path}")
        
        # Load the JSON file
        with open(absolute_path, "r") as f:
            locations = json.load(f)
        
        # Log the success of loading the locations
        logger.info(f"Successfully loaded {len(locations)} locations from {json_file_path}.")
        return locations
    
    except Exception as e:
        # Log the error if the file loading fails
        logger.error(f"Error loading locations from {json_file_path}: {e}")
        return []
