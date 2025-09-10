import requests
import os
import logging
import json
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv('FRED_API_KEY')
BASE_URL = os.getenv('FRED_BASE_URL')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'fred-data')

def get_minio_client():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        logger.error("MinIO credentials not found. Please set MINIO_ACCESS_KEY and MINIO_SECRET_KEY")
        raise ValueError("MinIO credentials required")
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  
    )
    
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logger.info(f"Created MinIO bucket: {MINIO_BUCKET}")
        else:
            logger.info(f"Using existing MinIO bucket: {MINIO_BUCKET}")
    except S3Error as e:
        logger.error(f"Failed to create/access MinIO bucket: {e}")
        raise
    
    return client

def upload_to_minio(data, object_name):
    try:
        client = get_minio_client()

        json_data = json.dumps(data, indent=2, default=str)

        data_stream = io.BytesIO(json_data.encode('utf-8'))
        data_length = len(json_data.encode('utf-8'))

        client.put_object(
            MINIO_BUCKET,
            object_name,
            data=data_stream,
            length=data_length,
            content_type='application/json'
        )
        
        logger.info(f"Successfully uploaded {object_name} to MinIO bucket {MINIO_BUCKET} ({data_length} bytes)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload {object_name} to MinIO: {e}")
        return False
    finally:
        if 'data_stream' in locals():
            data_stream.close()

def upload_batch_to_minio(batch_data, category, start_date, end_date):
    try:
        client = get_minio_client()

        start_str = start_date.replace('-', '')
        end_str = end_date.replace('-', '')
        object_name = f"fred/{category}/historical_{start_str}_to_{end_str}.json"

        batch_structure = {
            'batch_info': {
                'collection_timestamp': datetime.now().isoformat(),
                'category': category,
                'start_date': start_date,
                'end_date': end_date,
                'series_count': len(batch_data),
                'api_source': 'fred'
            },
            'series_data': batch_data
        }

        json_data = json.dumps(batch_structure, indent=2, default=str)
        data_stream = io.BytesIO(json_data.encode('utf-8'))
        data_length = len(json_data.encode('utf-8'))

        client.put_object(
            MINIO_BUCKET,
            object_name,
            data=data_stream,
            length=data_length,
            content_type='application/json'
        )
        
        logger.info(f"Successfully uploaded batch {object_name} to MinIO bucket {MINIO_BUCKET} ({data_length} bytes, {len(batch_data)} series)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload batch {object_name} to MinIO: {e}")
        return False
    finally:
        if 'data_stream' in locals():
            data_stream.close()

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

def get_interest_rates_data():
    logger.info("Fetching interest rates data")
    
    interest_rate_series = {
        'federal_funds_rate': 'FEDFUNDS',
        'treasury_10y': 'GS10',
        'treasury_2y': 'GS2',
        'treasury_3m': 'GS3M',
        'prime_rate': 'DPRIME'
    }
    
    return interest_rate_series

def get_sofr_data():
    logger.info("Fetching SOFR data series")
    
    sofr_series = {
        'sofr_rate': 'SOFR',
        'sofr_30d_avg': 'SOFR30DAYAVG',
        'sofr_90d_avg': 'SOFR90DAYAVG',
        'sofr_180d_avg': 'SOFR180DAYAVG',
        'sofr_index': 'SOFRINDEX'
    }
    
    return sofr_series

def get_sp500_data():
    logger.info("Fetching S&P 500 data series")
    
    sp500_series = {
        'sp500_index': 'SP500'
    }
    
    return sp500_series

def get_jobless_claims_data():
    logger.info("Fetching jobless claims data series")
    
    jobless_series = {
        'initial_claims': 'ICSA',
        'continued_claims': 'CCSA',
        'initial_claims_4w_avg': 'ICSA4W',
        'unemployment_rate': 'UNRATE'
    }
    
    return jobless_series

def get_historical_data_past_year(series_dict, data_category):
    logger.info(f"Collecting {data_category} data for the past year")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    logger.info(f"Date range: {start_date_str} to {end_date_str}")
    
    historical_data = {}
    
    for series_name, series_id in series_dict.items():
        try:
            logger.info(f"Fetching {series_name} ({series_id})")
  
            observations = get_series_observations(
                series_id=series_id,
                observation_start=start_date_str,
                observation_end=end_date_str,
                sort_order="asc"
            )
            
            if 'observations' in observations:
                data_points = observations['observations']
                logger.info(f"{series_name}: Retrieved {len(data_points)} data points")

                historical_data[series_name] = {
                    'series_id': series_id,
                    'data_points': data_points,
                    'count': len(data_points),
                    'start_date': start_date_str,
                    'end_date': end_date_str,
                    'raw_response': observations  
                }

                if data_points:
                    first_point = data_points[0]
                    last_point = data_points[-1]
                    logger.info(f"{series_name}: {first_point['date']} = {first_point['value']} -> {last_point['date']} = {last_point['value']}")
                
            else:
                logger.warning(f"No observations found for {series_name} ({series_id})")
                historical_data[series_name] = None
                
        except Exception as e:
            logger.error(f"Failed to fetch {series_name} ({series_id}): {e}")
            historical_data[series_name] = None

    category_filename = data_category.lower().replace(' ', '_')
    upload_success = upload_batch_to_minio(historical_data, category_filename, start_date_str, end_date_str)
    
    if upload_success:
        logger.info(f"Successfully uploaded {data_category} data to MinIO")
    else:
        logger.error(f"Failed to upload {data_category} data to MinIO")
    
    return historical_data

def collect_and_store_all_economic_data():
    logger.info("Starting comprehensive economic data collection and storage")
    
    all_data = {}

    logger.info("\n--- Collecting Interest Rates ---")
    interest_rates = get_interest_rates_data()
    all_data['interest_rates'] = get_historical_data_past_year(interest_rates, "Interest Rates")

    logger.info("\n--- Collecting SOFR Data ---")
    sofr_data = get_sofr_data()
    all_data['sofr'] = get_historical_data_past_year(sofr_data, "SOFR")

    logger.info("\n--- Collecting S&P 500 Data ---")
    sp500_data = get_sp500_data()
    all_data['sp500'] = get_historical_data_past_year(sp500_data, "S&P 500")

    logger.info("\n--- Collecting Jobless Claims ---")
    jobless_data = get_jobless_claims_data()
    all_data['jobless_claims'] = get_historical_data_past_year(jobless_data, "Jobless Claims")
    
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        summary_object = f"fred/summary/complete_economic_data_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.json"
        
        summary_data = {
            'collection_info': {
                'collection_timestamp': datetime.now().isoformat(),
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'categories_collected': list(all_data.keys()),
                'api_source': 'fred'
            },
            'data': all_data
        }
        
        upload_success = upload_to_minio(summary_data, summary_object)
        if upload_success:
            logger.info("Successfully uploaded complete economic data summary to MinIO")
        
    except Exception as e:
        logger.error(f"Failed to upload summary data: {e}")
    
    logger.info("Economic data collection and storage completed")
    return all_data

def main():
    try:
        logger.info("Starting FRED API comprehensive economic data collection with MinIO storage")
        
        validate_credentials()
 
        try:
            client = get_minio_client()
            logger.info("MinIO connection successful")
        except Exception as e:
            logger.error(f"MinIO connection failed: {e}")
            return
 
        logger.info("\n--- Starting Data Collection and Storage ---")
        all_economic_data = collect_and_store_all_economic_data()

        logger.info("\n--- Summary Report ---")
        total_series = 0
        for category, data in all_economic_data.items():
            logger.info(f"\n{category.upper()}:")
            if isinstance(data, dict):
                category_count = 0
                for series_name, series_data in data.items():
                    if series_data and 'count' in series_data:
                        logger.info(f"  {series_name}: {series_data['count']} data points")
                        category_count += 1
                        total_series += 1
                    else:
                        logger.info(f"  {series_name}: No data available")
                logger.info(f"  Category total: {category_count} series")
        
        logger.info(f"\nTotal series collected and stored: {total_series}")
        logger.info("Data collection and MinIO storage completed successfully")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise

if __name__ == "__main__":
    main()
