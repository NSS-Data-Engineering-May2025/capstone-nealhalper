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

API_KEY = os.getenv('FRED_API_KEY')
BASE_URL = os.getenv('FRED_BASE_URL')

def validate_credentials():
    if not API_KEY:
        logger.error("FRED_API_KEY environment variable is required")
        raise ValueError("FRED_API_KEY environment variable is required")
    
    logger.info("FRED API key is present")

def get_params():
    if not API_KEY:
        logger.error("API key is not available")
        raise ValueError("API key is not available. Please set FRED_API_KEY environment variable.")
    
    return {
        'api_key': API_KEY,
        'file_type': 'json'  
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

def get_category(category_id):
    logger.info(f"Getting category information for ID: {category_id}")
    params = {'category_id': category_id}
    return fetch_data("category", params)

def get_category_children(category_id):
    logger.info(f"Getting child categories for ID: {category_id}")
    params = {'category_id': category_id}
    return fetch_data("category/children", params)

def get_category_series(category_id, limit=1000, offset=0):
    logger.info(f"Getting series for category ID: {category_id}")
    params = {
        'category_id': category_id,
        'limit': limit,
        'offset': offset
    }
    return fetch_data("category/series", params)

def get_series_info(series_id):
    logger.info(f"Getting series information for: {series_id}")
    params = {'series_id': series_id}
    return fetch_data("series", params)

def get_series_observations(series_id, limit=100000, offset=0, sort_order="asc", 
                           observation_start=None, observation_end=None):
    logger.info(f"Getting observations for series: {series_id}")
    params = {
        'series_id': series_id,
        'limit': limit,
        'offset': offset,
        'sort_order': sort_order
    }
    
    if observation_start:
        params['observation_start'] = observation_start
    if observation_end:
        params['observation_end'] = observation_end
        
    return fetch_data("series/observations", params)

def search_series(search_text, limit=1000, offset=0, order_by="popularity", sort_order="desc"):
    logger.info(f"Searching for series with text: {search_text}")
    params = {
        'search_text': search_text,
        'limit': limit,
        'offset': offset,
        'order_by': order_by,
        'sort_order': sort_order
    }
    return fetch_data("series/search", params)

def get_sources(limit=1000, offset=0):
    logger.info("Getting all data sources")
    params = {
        'limit': limit,
        'offset': offset
    }
    return fetch_data("sources", params)

def main():
    try:
        logger.info("Starting FRED API test")
        
        validate_credentials()
        
        logger.info("Testing category endpoint")
        category_data = get_category(125)
        logger.info(f"Category data: {category_data}")

        logger.info("Testing series search")
        search_results = search_series("unemployment rate", limit=5)
        logger.info(f"Search results count: {len(search_results.get('seriess', []))}")
        
        if search_results.get('seriess'):
            series_id = search_results['seriess'][0]['id']
            logger.info(f"Getting info for series: {series_id}")
            series_info = get_series_info(series_id)
            logger.info(f"Series title: {series_info.get('seriess', [{}])[0].get('title', 'N/A')}")
        
        logger.info("Test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    main()
