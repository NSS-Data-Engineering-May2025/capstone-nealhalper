import requests
import os
import logging
import json
import time
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

API_KEY = os.getenv('COINGECKO_API_KEY')
BASE_URL = "https://api.coingecko.com/api/v3"

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'coingecko-data')

API_RATE_LIMIT_SECONDS = float(os.getenv('COINGECKO_RATE_LIMIT', '1.2'))  
BATCH_DELAY_SECONDS = float(os.getenv('COINGECKO_BATCH_DELAY', '10'))   
BATCH_SIZE = int(os.getenv('COINGECKO_BATCH_SIZE', '50'))              

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

def upload_batch_to_minio(batch_data, first_date, last_date):
    try:
        client = get_minio_client()
        
        first_date_str = first_date.strftime("%Y%m%d")
        last_date_str = last_date.strftime("%Y%m%d")
        object_name = f"bitcoin_historical_{first_date_str}_to_{last_date_str}.json"
        
        batch_structure = {
            'batch_info': {
                'collection_timestamp': datetime.now().isoformat(),
                'first_date': first_date.strftime("%d-%m-%Y"),
                'last_date': last_date.strftime("%d-%m-%Y"),
                'record_count': len(batch_data),
                'api_source': 'coingecko'
            },
            'records': batch_data
        }
        
        json_data = json.dumps(batch_structure, indent=2)
        data_stream = io.BytesIO(json_data.encode('utf-8'))
        data_length = len(json_data.encode('utf-8'))

        client.put_object(
            MINIO_BUCKET,
            object_name,
            data=data_stream,
            length=data_length,
            content_type='application/json'
        )
        
        logger.info(f"Successfully uploaded batch {object_name} to MinIO bucket {MINIO_BUCKET} ({data_length} bytes, {len(batch_data)} records)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload batch {object_name} to MinIO: {e}")
        return False
    finally:
        if 'data_stream' in locals():
            data_stream.close()

def upload_to_minio(data, object_name):
    try:
        client = get_minio_client()

        json_data = json.dumps(data, indent=2)
        
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
        
        logger.debug(f"Rate limiting: Waiting {API_RATE_LIMIT_SECONDS} seconds before request")
        time.sleep(API_RATE_LIMIT_SECONDS)
        
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

def get_bitcoin_coin_id():
    logger.info("Using Bitcoin's official coin ID")
    return 'bitcoin'

def get_coin_history(coin_id, date, localization=False):
    logger.info(f"Fetching historical snapshot for {coin_id} on {date}")
    
    params = {
        'date': date,
        'localization': str(localization).lower()
    }
    
    return fetch_data(f"coins/{coin_id}/history", params)

def collect_and_store_bitcoin_year_data():
    logger.info("Starting collection of Bitcoin historical data for the past year")
    logger.info(f"Rate limiting: {API_RATE_LIMIT_SECONDS}s between requests, {BATCH_DELAY_SECONDS}s every {BATCH_SIZE} requests")
    logger.info("Data will be stored in batches of 50 records per file")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    logger.info(f"Collecting data from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}")
    
    bitcoin_id = get_bitcoin_coin_id()
    successful_uploads = 0
    failed_uploads = 0
    request_count = 0
    
    current_batch = []
    batch_first_date = None
    batch_count = 0
    records_per_batch = 50
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d-%m-%Y")
        request_count += 1
        
        try:
            logger.info(f"Fetching raw data for {date_str} (Request {request_count})")
            
            raw_history = get_coin_history(bitcoin_id, date_str)
            
            if raw_history:
                record = {
                    'date': date_str,
                    'coin_id': bitcoin_id,
                    'collection_timestamp': datetime.now().isoformat(),
                    'raw_data': raw_history  
                }
                
                current_batch.append(record)
                
                if batch_first_date is None:
                    batch_first_date = current_date
                
                logger.info(f"Added {date_str} to batch ({len(current_batch)}/{records_per_batch})")
                
                if len(current_batch) >= records_per_batch:
                    batch_last_date = current_date
                    if upload_batch_to_minio(current_batch, batch_first_date, batch_last_date):
                        successful_uploads += len(current_batch)
                        batch_count += 1
                        logger.info(f"Successfully uploaded batch {batch_count} with {len(current_batch)} records")
                    else:
                        failed_uploads += len(current_batch)
                        logger.error(f"Failed to upload batch {batch_count}")

                    current_batch = []
                    batch_first_date = None
            else:
                failed_uploads += 1
                logger.warning(f"No data received for {date_str}")

            if request_count % BATCH_SIZE == 0:
                logger.info(f"API batch delay: Processed {request_count} requests, pausing for {BATCH_DELAY_SECONDS} seconds")
                time.sleep(BATCH_DELAY_SECONDS)
            
        except Exception as e:
            failed_uploads += 1
            logger.error(f"Failed to collect/store data for {date_str}: {e}")
        
        current_date += timedelta(days=1)
 
    if current_batch and batch_first_date:
        batch_last_date = current_date - timedelta(days=1) 
        if upload_batch_to_minio(current_batch, batch_first_date, batch_last_date):
            successful_uploads += len(current_batch)
            batch_count += 1
            logger.info(f"Successfully uploaded final batch {batch_count} with {len(current_batch)} records")
        else:
            failed_uploads += len(current_batch)
            logger.error(f"Failed to upload final batch {batch_count}")
    
    logger.info(f"Data collection completed. Success: {successful_uploads}, Failed: {failed_uploads}")
    logger.info(f"Total API requests made: {request_count}")
    logger.info(f"Total batches created: {batch_count}")
    return successful_uploads, failed_uploads

def get_bitcoin_historical_data_range(start_date=None, end_date=None, interval_days=1):
    if start_date is None:
        start_dt = datetime.now() - timedelta(days=365)
        start_date = start_dt.strftime("%d-%m-%Y")
        logger.info(f"Using calculated start date from one year ago: {start_date}")
    else:
        start_dt = datetime.strptime(start_date, "%d-%m-%Y")
    
    if end_date is None:
        end_dt = datetime.now()
        end_date = end_dt.strftime("%d-%m-%Y")
        logger.info(f"Using current date as end date: {end_date}")
    else:
        end_dt = datetime.strptime(end_date, "%d-%m-%Y")
    
    logger.info(f"Collecting Bitcoin historical data from {start_date} to {end_date} with {interval_days}-day intervals")
    
    bitcoin_id = get_bitcoin_coin_id()
    historical_data = []
    
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%d-%m-%Y")
        
        try:
            logger.info(f"Fetching data for {date_str}")
            history = get_coin_history(bitcoin_id, date_str)
            
            if 'market_data' in history:
                price = history['market_data']['current_price'].get('usd', 'N/A')
                market_cap = history['market_data']['market_cap'].get('usd', 'N/A')
                total_volume = history['market_data']['total_volume'].get('usd', 'N/A')
                
                data_point = {
                    'date': date_str,
                    'price_usd': price,
                    'market_cap_usd': market_cap,
                    'total_volume_usd': total_volume,
                    'raw_data': history
                }
                historical_data.append(data_point)
                logger.info(f"Bitcoin on {date_str}: Price ${price}, Market Cap ${market_cap}, Volume ${total_volume}")
            
        except Exception as e:
            logger.error(f"Failed to fetch data for {date_str}: {e}")
            continue
        
        current_dt += timedelta(days=interval_days)
    
    logger.info(f"Successfully collected {len(historical_data)} data points")
    return historical_data

def get_bitcoin_historical_data(date=None):
    if date is None:
        one_year_ago = datetime.now() - timedelta(days=365)
        date = one_year_ago.strftime("%d-%m-%Y")
        logger.info(f"Using calculated date from one year ago: {date}")
    
    logger.info(f"Fetching historical snapshot for Bitcoin on {date}")
    
    bitcoin_id = get_bitcoin_coin_id()
    
    try:
        history = get_coin_history(bitcoin_id, date)
        
        if 'market_data' in history:
            price = history['market_data']['current_price'].get('usd', 'N/A')
            market_cap = history['market_data']['market_cap'].get('usd', 'N/A')
            total_volume = history['market_data']['total_volume'].get('usd', 'N/A')
            logger.info(f"Bitcoin on {date}: Price ${price}, Market Cap ${market_cap}, Volume ${total_volume}")
        
        return history
        
    except Exception as e:
        logger.error(f"Failed to fetch historical data for Bitcoin: {e}")
        return None

def main():
    try:
        logger.info("Starting CoinGecko API test for Bitcoin historical data")

        validate_credentials()

        ping_result = ping_api()
        logger.info(f"Ping result: {ping_result}")

        bitcoin_id = get_bitcoin_coin_id()
        logger.info(f"Bitcoin coin ID: {bitcoin_id}")

        logger.info("\n--- Collecting and storing Bitcoin data for all dates in past year ---")
        success_count, fail_count = collect_and_store_bitcoin_year_data()
        
        total_attempts = success_count + fail_count
        logger.info(f"\n=== Data Collection Summary ===")
        logger.info(f"Total dates attempted: {total_attempts}")
        logger.info(f"Successful uploads: {success_count}")
        logger.info(f"Failed uploads: {fail_count}")
        if total_attempts > 0:
            success_rate = (success_count / total_attempts) * 100
            logger.info(f"Success rate: {success_rate:.2f}%")

        logger.info("\n--- Testing single date for verification ---")
        test_date = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
        historical_data = get_bitcoin_historical_data(date=test_date)
        
        if historical_data and 'market_data' in historical_data:
            logger.info(f"Verification: Successfully retrieved data for {test_date}")
        else:
            logger.warning(f"Verification: Failed to retrieve data for {test_date}")

        logger.info("Bitcoin historical data collection and storage completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    main()
