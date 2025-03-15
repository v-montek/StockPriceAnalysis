import os
from azure.keyvault.secrets import SecretClient
from azure.identity import AzureCliCredential
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL")

credential = AzureCliCredential()
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

# Retrieve secrets
def get_secret(secret_name):
    return secret_client.get_secret(secret_name).value

DB_CONFIG = {
    "server": get_secret("SQLServer"),
    "database": get_secret("SQLServerDB"),
    "username": get_secret("SQLServerUsername"),
    "password": get_secret("SQLServerPWD"),
}

API_KEY = get_secret("PolygonAPIKey")