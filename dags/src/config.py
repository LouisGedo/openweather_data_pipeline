import os
import logging
from src.logger_config import setup_logger
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential


# Initialize the logger for config.py
logger = setup_logger(
    name=__name__,
    log_file='./logs/config.log',
    level=logging.INFO,
    log_format='%(asctime)s - %(levelname)s - %(message)s'
)


# Retrieve Azure credentials from environment variables
client_id = os.getenv('AZURE_CLIENT_ID')
tenant_id = os.getenv('AZURE_TENANT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
vault_url = os.getenv("AZURE_VAULT_URI")

# Initialize the Azure Key Vault client
credentials = ClientSecretCredential(
    client_id=client_id, 
    client_secret=client_secret,
    tenant_id=tenant_id
)

secret_client = SecretClient(vault_url=vault_url, credential=credentials)

# Function to retrieve secrets from Key Vault
def get_secret(secret_name: str) -> str:
    """
    Retrieve a secret from Azure Key Vault.

    Args:
        secret_name (str): The name of the secret to retrieve.
    
    Returns:
        str: The value of the secret.
    """
    try:
        secret = secret_client.get_secret(secret_name)
        logger.info(f"Successfully retrieved secret '{secret_name}'")
        return secret.value
    except Exception as e:
        logger.error(f"Error retrieving secret '{secret_name}': {e}")
        return None
