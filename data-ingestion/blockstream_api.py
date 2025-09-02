import requests
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CLIENT_ID = os.getenv('BLOCKSTREAM_CLIENT_ID')
CLIENT_SECRET = os.getenv('BLOCKSTREAM_CLIENT_SECRET')
TOKEN_URL = os.getenv('BLOCKSTREAM_TOKEN_URL')
ACCESS_TOKEN = None

def validate_credentials():
    if not CLIENT_ID:
        logger.error("BLOCKSTREAM_CLIENT_ID environment variable is required")
        raise ValueError("BLOCKSTREAM_CLIENT_ID environment variable is required")
    if not CLIENT_SECRET:
        logger.error("BLOCKSTREAM_CLIENT_SECRET environment variable is required")
        raise ValueError("BLOCKSTREAM_CLIENT_SECRET environment variable is required")
    if not TOKEN_URL:
        logger.error("BLOCKSTREAM_TOKEN_URL environment variable is required")
        raise ValueError("BLOCKSTREAM_TOKEN_URL environment variable is required")
    
    logger.info("All required environment variables are present")

def authenticate():
    global ACCESS_TOKEN
    
    validate_credentials()
    
    logger.info("Starting authentication process")
    
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    
    try:
        logger.debug(f"Making authentication request to {TOKEN_URL}")
        response = requests.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        
        ACCESS_TOKEN = response.json().get('access_token')
        
        if ACCESS_TOKEN:
            logger.info("Authentication successful")
        else:
            logger.error("No access token received in response")
            raise ValueError("No access token received")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Authentication failed: {e}")
        raise

def get_headers():
    if not ACCESS_TOKEN:
        logger.error("Access token is not available. Authentication required first.")
        raise ValueError("Access token is not available. Please authenticate first.")
    
    return {
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

def fetch_data(endpoint):
    logger.info(f"Fetching data from endpoint: {endpoint}")
    
    try:
        headers = get_headers()
        logger.debug(f"Making request to {endpoint}")
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully fetched data from {endpoint}")
        logger.debug(f"Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Non-dict response'}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from {endpoint}: {e}")
        raise

def main():
    try:
        logger.info("Starting Blockstream API test")
        authenticate()

        logger.info("Test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    main()


