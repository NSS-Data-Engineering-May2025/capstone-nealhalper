import requests
import os
import logging
import json
import io
from datetime import datetime
from minio import Minio
from minio.error import S3Error
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
BASE_URL = "https://blockstream.info/api"

MINIO_ENDPOINT = os.getenv('MINIO_EXTERNAL_URL')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET_NAME')

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

def initialize_minio():
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False 
        )
        
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logger.info(f"Created MinIO bucket: {MINIO_BUCKET}")
        else:
            logger.info(f"MinIO bucket exists: {MINIO_BUCKET}")
            
        return client
        
    except S3Error as e:
        logger.error(f"MinIO S3 error: {e}")
        raise
    except Exception as e:
        logger.error(f"MinIO initialization error: {e}")
        raise

def save_to_minio(data, object_name, minio_client=None):
    if minio_client is None:
        minio_client = initialize_minio()
    
    try:
        json_data = json.dumps(data, indent=2, default=str)
        json_bytes = json_data.encode('utf-8')
        
        minio_client.put_object(
            MINIO_BUCKET,
            object_name,
            data=io.BytesIO(json_bytes), 
            length=len(json_bytes),
            content_type='application/json'
        )
        
        logger.info(f"Successfully saved data to MinIO: {object_name}")
        return True
        
    except S3Error as e:
        logger.error(f"Failed to save to MinIO: {e}")
        return False
    except Exception as e:
        logger.error(f"Error saving to MinIO: {e}")
        return False

def authenticate():
    validate_credentials()
    
    logger.info("Starting authentication process")
    
    payload = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    
    try:
        response = requests.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        
        if access_token:
            logger.info("Authentication successful")
            return access_token
        else:
            raise ValueError("No access token received")
    except requests.exceptions.RequestException as e:
        logger.error(f"Authentication failed: {e}")
        raise

def get_headers(access_token):
    if not access_token:
        raise ValueError("Access token is required")
    return {'Authorization': f'Bearer {access_token}'}

def fetch_data(endpoint, access_token):
    logger.info(f"Fetching data from endpoint: {endpoint}")
    
    try:
        headers = get_headers(access_token)
        logger.debug(f"Making request to {endpoint}")
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        
        content_type = response.headers.get('content-type', '').lower()
        
        if 'application/json' in content_type:
            data = response.json()
            logger.info(f"Successfully fetched JSON data from {endpoint}")
        else:
            data = response.text.strip()
            logger.info(f"Successfully fetched text data from {endpoint}")
            
            if endpoint.endswith('/height'):
                try:
                    data = int(data)
                except ValueError:
                    pass  
        
        logger.debug(f"Response data type: {type(data)}")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from {endpoint}: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {endpoint}: {e}")
        return response.text.strip()

def get_mempool_info(access_token):
    logger.info("Fetching mempool information")
    endpoint = f"{BASE_URL}/mempool"
    return fetch_data(endpoint, access_token)

def get_fee_estimates(access_token):
    logger.info("Fetching fee estimates")
    endpoint = f"{BASE_URL}/fee-estimates"
    return fetch_data(endpoint, access_token)

def get_blocks_tip_height(access_token):
    logger.info("Fetching current block height")
    endpoint = f"{BASE_URL}/blocks/tip/height"
    return fetch_data(endpoint, access_token)

def get_blocks_tip_hash(access_token):
    logger.info("Fetching current block hash")
    endpoint = f"{BASE_URL}/blocks/tip/hash"
    return fetch_data(endpoint, access_token)

def get_block_data(block_hash, access_token):
    logger.info(f"Fetching block data for hash: {block_hash}")
    endpoint = f"{BASE_URL}/block/{block_hash}"
    return fetch_data(endpoint, access_token)

def get_recent_blocks(access_token, start_height=None):
    if start_height:
        logger.info(f"Fetching blocks starting from height: {start_height}")
        endpoint = f"{BASE_URL}/blocks/{start_height}"
    else:
        logger.info("Fetching latest blocks")
        endpoint = f"{BASE_URL}/blocks"
    return fetch_data(endpoint, access_token)

def get_block_by_height(height, access_token):
    logger.info(f"Fetching block hash for height: {height}")
    endpoint = f"{BASE_URL}/block-height/{height}"
    return fetch_data(endpoint, access_token)

def get_mempool_txids(access_token):
    logger.info("Fetching mempool transaction IDs")
    endpoint = f"{BASE_URL}/mempool/txids"
    return fetch_data(endpoint, access_token)

def get_mempool_recent(access_token):
    logger.info("Fetching recent mempool transactions")
    endpoint = f"{BASE_URL}/mempool/recent"
    return fetch_data(endpoint, access_token)

def get_block_header(block_hash, access_token):
    logger.info(f"Fetching block header for hash: {block_hash}")
    endpoint = f"{BASE_URL}/block/{block_hash}/header"
    return fetch_data(endpoint, access_token)

def get_block_status(block_hash, access_token):
    logger.info(f"Fetching block status for hash: {block_hash}")
    endpoint = f"{BASE_URL}/block/{block_hash}/status"
    return fetch_data(endpoint, access_token)

def get_block_txs(block_hash, access_token, start_index=0):
    logger.info(f"Fetching transactions for block: {block_hash}, starting at index: {start_index}")
    endpoint = f"{BASE_URL}/block/{block_hash}/txs/{start_index}" if start_index > 0 else f"{BASE_URL}/block/{block_hash}/txs"
    return fetch_data(endpoint, access_token)

def get_block_txids(block_hash, access_token):
    logger.info(f"Fetching transaction IDs for block: {block_hash}")
    endpoint = f"{BASE_URL}/block/{block_hash}/txids"
    return fetch_data(endpoint, access_token)

def get_transaction(txid, access_token):
    logger.info(f"Fetching transaction data for txid: {txid}")
    endpoint = f"{BASE_URL}/tx/{txid}"
    return fetch_data(endpoint, access_token)

def get_transaction_status(txid, access_token):
    logger.info(f"Fetching transaction status for txid: {txid}")
    endpoint = f"{BASE_URL}/tx/{txid}/status"
    return fetch_data(endpoint, access_token)

def get_network_stats(access_token):
    logger.info("Fetching comprehensive network statistics")
    
    stats = {}
    
    try:
        stats['mempool'] = get_mempool_info(access_token)
        
        stats['fee_estimates'] = get_fee_estimates(access_token)

        stats['current_height'] = get_blocks_tip_height(access_token)
        
        stats['current_hash'] = get_blocks_tip_hash(access_token)
   
        stats['recent_blocks'] = get_recent_blocks(access_token)

        stats['recent_mempool_txs'] = get_mempool_recent(access_token)
        
        logger.info("Successfully compiled network statistics")
        return stats
        
    except Exception as e:
        logger.error(f"Failed to compile network statistics: {e}")
        raise

    
def collect_and_store_blockstream_data(access_token, minio_client=None):
    if minio_client is None:
        minio_client = initialize_minio()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        logger.info("Starting comprehensive Blockstream data collection")
        
        logger.info("Collecting mempool data...")
        mempool_data = get_mempool_info(access_token)
        save_to_minio(mempool_data, f"mempool/mempool_{timestamp}.json", minio_client)
        
        logger.info("Collecting fee estimates...")
        fee_data = get_fee_estimates(access_token)
        save_to_minio(fee_data, f"fees/fee_estimates_{timestamp}.json", minio_client)
        
        logger.info("Collecting current block height...")
        height_data = get_blocks_tip_height(access_token)
        save_to_minio({"height": height_data, "timestamp": timestamp}, 
                     f"blocks/current_height_{timestamp}.json", minio_client)
        
        logger.info("Collecting current block hash...")
        hash_data = get_blocks_tip_hash(access_token)
        save_to_minio({"hash": hash_data, "timestamp": timestamp}, 
                     f"blocks/current_hash_{timestamp}.json", minio_client)
        
        logger.info("Collecting recent blocks...")
        blocks_data = get_recent_blocks(access_token)
        save_to_minio(blocks_data, f"blocks/recent_blocks_{timestamp}.json", minio_client)
        
        logger.info("Collecting recent mempool transactions...")
        recent_mempool = get_mempool_recent(access_token)
        save_to_minio(recent_mempool, f"mempool/recent_transactions_{timestamp}.json", minio_client)
        
        logger.info("Collecting mempool transaction IDs...")
        try:
            mempool_txids = get_mempool_txids(access_token)
            if isinstance(mempool_txids, list) and len(mempool_txids) < 10000:
                save_to_minio(mempool_txids, f"mempool/txids_{timestamp}.json", minio_client)
            else:
                logger.info(f"Skipping mempool txids - too large ({len(mempool_txids) if isinstance(mempool_txids, list) else 'unknown'} items)")
        except Exception as e:
            logger.warning(f"Failed to collect mempool txids: {e}")
        
        if blocks_data and isinstance(blocks_data, list) and len(blocks_data) > 0:
            latest_block = blocks_data[0]
            latest_block_id = latest_block.get('id')
            if latest_block_id:
                logger.info(f"Collecting detailed data for latest block: {latest_block_id}")
                try:
                    block_header = get_block_header(latest_block_id, access_token)
                    save_to_minio({"header": block_header, "block_id": latest_block_id}, 
                                 f"blocks/block_header_{latest_block_id}_{timestamp}.json", minio_client)
                except Exception as e:
                    logger.warning(f"Failed to get block header: {e}")
                
                try:
                    block_status = get_block_status(latest_block_id, access_token)
                    save_to_minio(block_status, 
                                 f"blocks/block_status_{latest_block_id}_{timestamp}.json", minio_client)
                except Exception as e:
                    logger.warning(f"Failed to get block status: {e}")
                
                try:
                    block_txs = get_block_txs(latest_block_id, access_token)
                    save_to_minio(block_txs, 
                                 f"blocks/block_transactions_{latest_block_id}_{timestamp}.json", minio_client)
                except Exception as e:
                    logger.warning(f"Failed to get block transactions: {e}")
        
        summary = {
            "collection_timestamp": timestamp,
            "data_types_collected": [
                "mempool",
                "fee_estimates", 
                "current_height",
                "current_hash",
                "recent_blocks",
                "recent_mempool_transactions",
                "mempool_txids",
                "block_header",
                "block_status",
                "block_transactions"
            ],
            "collection_status": "success"
        }
        save_to_minio(summary, f"metadata/collection_summary_{timestamp}.json", minio_client)
        
        logger.info(f"Successfully collected and stored all Blockstream data at {timestamp}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to collect and store data: {e}")
        error_summary = {
            "collection_timestamp": timestamp,
            "collection_status": "failed",
            "error": str(e)
        }
        save_to_minio(error_summary, f"metadata/collection_error_{timestamp}.json", minio_client)
        return False

def main():
    try:
        logger.info("Starting Blockstream data collection for MinIO storage")
   
        logger.info("Initializing MinIO connection...")
        minio_client = initialize_minio()
        
        logger.info("Authenticating with Blockstream API...")
        access_token = authenticate()
        
        success = collect_and_store_blockstream_data(access_token, minio_client)
        
        if success:
            logger.info("Data collection and storage completed successfully")
        else:
            logger.error("Data collection failed")
            
    except Exception as e:
        logger.error(f"Main process failed: {e}")
        raise

if __name__ == "__main__":
    main()


