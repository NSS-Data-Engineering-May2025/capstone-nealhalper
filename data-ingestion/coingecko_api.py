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

API_KEY = os.getenv('COINGECKO_API_KEY')
BASE_URL = "https://api.coingecko.com/api/v3"

def validate_credentials():
    if not API_KEY:
        logger.error("COINGECKO_API_KEY environment variable is required")
        raise ValueError("COINGECKO_API_KEY environment variable is required")
    
    logger.info("CoinGecko API key is present")

def get_params():
    if not API_KEY:
        logger.error("API key is not available")
        raise ValueError("API key is not available. Please set COINGECKO_API_KEY environment variable.")
    
    return {
        'x_cg_demo_api_key': API_KEY
    }

def fetch_data(endpoint, additional_params=None):
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    
    logger.info(f"Fetching data from endpoint: {url}")
    
    try:
        params = get_params()
        
        if additional_params:
            params.update(additional_params)
            
        logger.debug(f"Making request to {url} with params: {list(params.keys())}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully fetched data from {url}")
        logger.debug(f"Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Non-dict response'}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from {url}: {e}")
        raise

def ping_api():
    logger.info("Testing CoinGecko API connection")
    return fetch_data("ping")

def get_simple_price(ids, vs_currencies="usd", include_market_cap=False, include_24hr_vol=False):
    params = {
        'ids': ids if isinstance(ids, str) else ','.join(ids),
        'vs_currencies': vs_currencies if isinstance(vs_currencies, str) else ','.join(vs_currencies)
    }
    
    if include_market_cap:
        params['include_market_cap'] = 'true'
    if include_24hr_vol:
        params['include_24hr_vol'] = 'true'
        
    return fetch_data("simple/price", params)

def get_coins_list(include_platform=False):
    params = {}
    if include_platform:
        params['include_platform'] = 'true'
        
    return fetch_data("coins/list", params)

def get_coin_data(coin_id, localization=False, tickers=False, market_data=True, community_data=False, developer_data=False, sparkline=False):
    params = {
        'localization': str(localization).lower(),
        'tickers': str(tickers).lower(),
        'market_data': str(market_data).lower(),
        'community_data': str(community_data).lower(),
        'developer_data': str(developer_data).lower(),
        'sparkline': str(sparkline).lower()
    }
    
    return fetch_data(f"coins/{coin_id}", params)

def main():
    try:
        logger.info("Starting CoinGecko API test")

        validate_credentials()

        ping_result = ping_api()
        logger.info(f"Ping result: {ping_result}")

        logger.info("Testing simple price endpoint")
        price_data = get_simple_price("bitcoin,ethereum", "usd", include_market_cap=True)
        logger.info(f"Price data: {price_data}")
        
        logger.info("Test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    main()
