import requests
import os
import logging
import json
import io
import time
import signal
import sys
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global shutdown flag
shutdown_requested = False

def signal_handler(signum, frame):
    """Graceful shutdown handler for SIGINT (Ctrl+C) and SIGTERM"""
    global shutdown_requested
    logger.info("Shutdown signal received. Finishing current operation and exiting gracefully...")
    logger.info("This may take a moment to complete the current API request and save data.")
    shutdown_requested = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

CLIENT_ID = os.getenv('BLOCKSTREAM_CLIENT_ID')
CLIENT_SECRET = os.getenv('BLOCKSTREAM_CLIENT_SECRET')
TOKEN_URL = os.getenv('BLOCKSTREAM_TOKEN_URL')
BASE_URL = "https://blockstream.info/api"

MINIO_ENDPOINT = os.getenv('MINIO_EXTERNAL_URL')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET_NAME')

# Optimized rate limiting configuration
RATE_LIMIT_DELAY = 5.2  # 692 requests/hour (8 req/hour buffer)
BATCH_SIZE = 15  # Slightly larger batches for efficiency
RETRY_DELAY_BASE = 4  # Slightly faster retries

# Enhanced rate limiting with burst protection
BURST_PROTECTION_DELAY = 10  # Extra delay every N requests
BURST_PROTECTION_INTERVAL = 50  # Apply burst protection every 50 requests

FEE_SAMPLE_INTERVAL = 144  # Sample fee data every ~24 hours
HISTORICAL_DAYS = 365  # Full year collection

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

def check_existing_data(minio_client, object_path):
    try:
        minio_client.stat_object(MINIO_BUCKET, object_path)
        return True
    except S3Error:
        return False

def get_existing_block_heights(minio_client):
    try:
        existing_heights = set()
        objects = minio_client.list_objects(MINIO_BUCKET, prefix="blocks/", recursive=True)
        
        for obj in objects:
            if obj.object_name.startswith("blocks/height_") and obj.object_name.endswith(".json"):
                try:
                    height_str = obj.object_name.replace("blocks/height_", "").replace(".json", "")
                    height = int(height_str)
                    existing_heights.add(height)
                except ValueError:
                    continue
        
        logger.info(f"Found {len(existing_heights)} existing blocks in MinIO")
        return existing_heights
        
    except Exception as e:
        logger.warning(f"Could not list existing blocks: {e}")
        return set()

def get_existing_fee_data_points(minio_client):
    try:
        existing_fee_heights = set()
        objects = minio_client.list_objects(MINIO_BUCKET, prefix="fees/historical/", recursive=True)
        
        for obj in objects:
            if obj.object_name.startswith("fees/historical/height_") and obj.object_name.endswith("_fees.json"):
                try:
                    filename = obj.object_name.split("/")[-1]  
                    height_str = filename.replace("height_", "").replace("_fees.json", "")
                    height = int(height_str)
                    existing_fee_heights.add(height)
                except ValueError:
                    continue
        
        logger.info(f"Found {len(existing_fee_heights)} existing fee data points in MinIO")
        return existing_fee_heights
        
    except Exception as e:
        logger.warning(f"Could not list existing fee data: {e}")
        return set()

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
        
        logger.debug(f"Successfully saved data to MinIO: {object_name}")
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

def fetch_data_with_retry(endpoint, access_token, max_retries=5, request_count=0):
    for attempt in range(max_retries):
        try:
            # Apply base rate limiting
            delay = RATE_LIMIT_DELAY * (2 ** attempt) if attempt > 0 else RATE_LIMIT_DELAY
            
            # Add burst protection every N requests to avoid overwhelming API
            if request_count > 0 and request_count % BURST_PROTECTION_INTERVAL == 0:
                delay += BURST_PROTECTION_DELAY
                logger.info(f"Applying burst protection delay after {request_count} requests")
            
            time.sleep(delay)
            
            headers = get_headers(access_token)
            response = requests.get(endpoint, headers=headers, timeout=30)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds before retry {attempt + 1}/{max_retries}")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            
            content_type = response.headers.get('content-type', '').lower()
            
            if 'application/json' in content_type:
                data = response.json()
            else:
                data = response.text.strip()
                if endpoint.endswith('/height'):
                    try:
                        data = int(data)
                    except ValueError:
                        pass
            
            return data
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                retry_delay = RETRY_DELAY_BASE ** (attempt + 1)
                logger.warning(f"Attempt {attempt + 1} failed for {endpoint}: {e}. Retrying in {retry_delay}s")
                time.sleep(retry_delay)
            else:
                logger.error(f"All {max_retries} attempts failed for {endpoint}: {e}")
                raise

def get_current_block_height(access_token):
    endpoint = f"{BASE_URL}/blocks/tip/height"
    return fetch_data_with_retry(endpoint, access_token)

def get_block_hash_by_height(height, access_token):
    endpoint = f"{BASE_URL}/block-height/{height}"
    return fetch_data_with_retry(endpoint, access_token)

def get_block_data(block_hash, access_token):
    endpoint = f"{BASE_URL}/block/{block_hash}"
    return fetch_data_with_retry(endpoint, access_token)

def get_fee_estimates(access_token):
    endpoint = f"{BASE_URL}/fee-estimates"
    return fetch_data_with_retry(endpoint, access_token)

def get_mempool_data(access_token):
    endpoint = f"{BASE_URL}/mempool"
    return fetch_data_with_retry(endpoint, access_token)

def calculate_historical_ranges(access_token, existing_block_heights, existing_fee_heights):
    """Calculate ranges for both block and fee data collection - FULL YEAR"""
    current_height = get_current_block_height(access_token)
    
    # Bitcoin averages ~144 blocks per day (10 min intervals)
    # Last 365 days = 365 days * 144 blocks/day = ~52,560 blocks
    blocks_per_period = HISTORICAL_DAYS * 144
    start_height = max(1, current_height - blocks_per_period)
    
    # Calculate missing blocks (full year)
    all_block_heights = set(range(start_height, current_height + 1))
    blocks_to_collect = sorted(all_block_heights - existing_block_heights)
    
    # Calculate missing fee data points (sample every FEE_SAMPLE_INTERVAL blocks)
    all_fee_sample_heights = set(range(start_height, current_height + 1, FEE_SAMPLE_INTERVAL))
    fee_heights_to_collect = sorted(all_fee_sample_heights - existing_fee_heights)
    
    logger.info(f"Current height: {current_height}")
    logger.info(f"Historical range (last {HISTORICAL_DAYS} days): {start_height} to {current_height}")
    logger.info(f"Total blocks in range: {current_height - start_height + 1}")
    logger.info(f"Existing blocks: {len(existing_block_heights)}")
    logger.info(f"Blocks to collect: {len(blocks_to_collect)}")
    logger.info(f"Existing fee data points: {len(existing_fee_heights)}")
    logger.info(f"Fee data points to collect: {len(fee_heights_to_collect)}")
    
    # Calculate estimated time for full year
    total_requests = len(blocks_to_collect) * 2 + len(fee_heights_to_collect) * 3 + 2  # +2 for current data
    estimated_hours = (total_requests * RATE_LIMIT_DELAY) / 3600
    estimated_days = estimated_hours / 24
    logger.info(f"Estimated collection time: {estimated_hours:.1f} hours ({estimated_days:.1f} days) for {total_requests} total requests")
    
    return start_height, current_height, blocks_to_collect, fee_heights_to_collect

def save_progress_checkpoint(collected_data, checkpoint_type, minio_client):
    """Save progress checkpoint for graceful resume"""
    try:
        checkpoint = {
            'checkpoint_type': checkpoint_type,
            'timestamp': datetime.now().isoformat(),
            'collected_data': collected_data,
            'shutdown_requested': shutdown_requested
        }
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_path = f"checkpoints/{checkpoint_type}_checkpoint_{timestamp_str}.json"
        
        if save_to_minio(checkpoint, checkpoint_path, minio_client):
            logger.info(f"Progress checkpoint saved: {checkpoint_path}")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")
        return False

def collect_historical_block_data(access_token, minio_client, blocks_to_collect):
    global shutdown_requested
    logger.info(f"Starting historical block collection for {len(blocks_to_collect)} blocks")
    logger.info(f"Optimized rate limiting: 692 requests/hour (5.2s delay)")
    logger.info("Press Ctrl+C to gracefully stop after current operation completes")
    
    collected_count = 0
    failed_count = 0
    request_count = 0
    
    for i, height in enumerate(blocks_to_collect):
        # Check for shutdown request at the start of each iteration
        if shutdown_requested:
            logger.info(f"Graceful shutdown requested. Stopping block collection after {collected_count} blocks.")
            save_progress_checkpoint({
                'last_collected_height': height - 1 if i > 0 else None,
                'collected_count': collected_count,
                'failed_count': failed_count,
                'remaining_blocks': len(blocks_to_collect) - i
            }, 'block_collection', minio_client)
            break
            
        try:
            # Track total requests for burst protection
            request_count += 2  # Each block requires 2 API calls
            
            block_hash = get_block_hash_by_height(height, access_token)
            
            # Check again before second API call
            if shutdown_requested:
                logger.info("Shutdown requested during block hash retrieval. Stopping safely.")
                break
                
            block_data = get_block_data(block_hash, access_token)
            
            raw_block_record = {
                'height': height,
                'block_hash': block_hash,
                'collection_timestamp': datetime.now().isoformat(),
                'raw_block_data': block_data,
                'request_count': request_count
            }
            
            block_object_path = f"blocks/height_{height}.json"
            save_to_minio(raw_block_record, block_object_path, minio_client)
            
            collected_count += 1
            
            if (i + 1) % 50 == 0:
                progress_pct = ((i + 1) / len(blocks_to_collect)) * 100
                hours_elapsed = (request_count * RATE_LIMIT_DELAY) / 3600
                logger.info(f"Block progress: {i + 1}/{len(blocks_to_collect)} ({progress_pct:.1f}%) - "
                          f"Collected: {collected_count}, Requests: {request_count}, "
                          f"Hours elapsed: {hours_elapsed:.1f}")
                
                # Save checkpoint every 50 blocks
                save_progress_checkpoint({
                    'last_collected_height': height,
                    'collected_count': collected_count,
                    'failed_count': failed_count,
                    'progress_pct': progress_pct
                }, 'block_collection', minio_client)
            
            # Reduced batch pause with optimized timing
            if collected_count % BATCH_SIZE == 0:
                logger.info(f"Block batch completed. Processed {collected_count} blocks")
                
                # Check for shutdown during batch pause
                for _ in range(5):  # 5 seconds total pause, check every second
                    if shutdown_requested:
                        logger.info("Shutdown requested during batch pause. Stopping safely.")
                        break
                    time.sleep(1)
                
                if shutdown_requested:
                    break
                
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to collect block at height {height}: {e}")
            
            if failed_count > 20:
                logger.error("Too many block collection failures, stopping historical collection")
                break
            
            # Check for shutdown during error pause
            if not shutdown_requested:
                time.sleep(1)
    
    logger.info(f"Historical block collection completed. Collected: {collected_count}, Failed: {failed_count}")
    logger.info(f"Total API requests made: {request_count}")
    
    # Final checkpoint
    save_progress_checkpoint({
        'final_summary': True,
        'collected_count': collected_count,
        'failed_count': failed_count,
        'total_requests': request_count,
        'completed_gracefully': not shutdown_requested
    }, 'block_collection_final', minio_client)
    
    return collected_count, failed_count

def collect_historical_fee_data(access_token, minio_client, fee_heights_to_collect):
    global shutdown_requested
    logger.info(f"Starting historical fee data collection for {len(fee_heights_to_collect)} data points")
    
    collected_count = 0
    failed_count = 0
    request_count = 0
    
    for i, height in enumerate(fee_heights_to_collect):
        # Check for shutdown request
        if shutdown_requested:
            logger.info(f"Graceful shutdown requested. Stopping fee collection after {collected_count} data points.")
            save_progress_checkpoint({
                'last_collected_height': height - FEE_SAMPLE_INTERVAL if i > 0 else None,
                'collected_count': collected_count,
                'failed_count': failed_count,
                'remaining_fee_points': len(fee_heights_to_collect) - i
            }, 'fee_collection', minio_client)
            break
            
        try:
            # Track total requests for burst protection  
            request_count += 3  # Each fee point requires 3 API calls
            
            fee_estimates = get_fee_estimates(access_token)
            
            # Check shutdown between API calls
            if shutdown_requested:
                logger.info("Shutdown requested during fee estimation. Stopping safely.")
                break
                
            block_hash = get_block_hash_by_height(height, access_token)
            
            if shutdown_requested:
                logger.info("Shutdown requested during block hash retrieval. Stopping safely.")
                break
                
            block_data = get_block_data(block_hash, access_token)
            
            raw_fee_record = {
                'height': height,
                'block_hash': block_hash,
                'collection_timestamp': datetime.now().isoformat(),
                'raw_fee_estimates': fee_estimates,
                'raw_block_data_for_fees': {
                    'timestamp': block_data.get('timestamp'),
                    'tx_count': len(block_data.get('tx', [])),
                    'size': block_data.get('size'),
                    'weight': block_data.get('weight'),
                    'transactions': block_data.get('tx', [])  
                },
                'request_count': request_count
            }
            
            fee_object_path = f"fees/historical/height_{height}_fees.json"
            save_to_minio(raw_fee_record, fee_object_path, minio_client)
            
            collected_count += 1
            
            if (i + 1) % 20 == 0:
                progress_pct = ((i + 1) / len(fee_heights_to_collect)) * 100
                hours_elapsed = (request_count * RATE_LIMIT_DELAY) / 3600
                logger.info(f"Fee progress: {i + 1}/{len(fee_heights_to_collect)} ({progress_pct:.1f}%) - "
                          f"Collected: {collected_count}, Requests: {request_count}, "
                          f"Hours elapsed: {hours_elapsed:.1f}")
                
                # Save checkpoint every 20 fee data points
                save_progress_checkpoint({
                    'last_collected_height': height,
                    'collected_count': collected_count,
                    'failed_count': failed_count,
                    'progress_pct': progress_pct
                }, 'fee_collection', minio_client)
            
            if collected_count % 30 == 0:  # Increased from 25 to 30
                logger.info(f"Fee batch completed. Processed {collected_count} fee data points")
                
                # Check for shutdown during batch pause
                for _ in range(3):  # 3 seconds total pause
                    if shutdown_requested:
                        logger.info("Shutdown requested during fee batch pause. Stopping safely.")
                        break
                    time.sleep(1)
                
                if shutdown_requested:
                    break
                
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to collect fee data at height {height}: {e}")
     
            if failed_count > 10:
                logger.error("Too many fee collection failures, stopping historical fee collection")
                break
            
            if not shutdown_requested:
                time.sleep(1)
    
    logger.info(f"Historical fee collection completed. Collected: {collected_count}, Failed: {failed_count}")
    logger.info(f"Total API requests made: {request_count}")
    
    # Final checkpoint
    save_progress_checkpoint({
        'final_summary': True,
        'collected_count': collected_count,
        'failed_count': failed_count,
        'total_requests': request_count,
        'completed_gracefully': not shutdown_requested
    }, 'fee_collection_final', minio_client)
    
    return collected_count, failed_count

def collect_current_mempool_data(access_token, minio_client):
    timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    date_str = timestamp.strftime("%Y/%m/%d")
    
    try:
        mempool_data = get_mempool_data(access_token)
        
        raw_mempool_record = {
            'collection_timestamp': timestamp.isoformat(),
            'raw_mempool_data': mempool_data
        }
        
        object_path = f"mempool/{date_str}/mempool_snapshot_{timestamp_str}.json"
        save_to_minio(raw_mempool_record, object_path, minio_client)
        
        logger.info(f"Collected raw mempool snapshot at {timestamp_str}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to collect mempool data: {e}")
        return False

def collect_current_fee_data(access_token, minio_client):
    timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    date_str = timestamp.strftime("%Y/%m/%d")
    
    try:
        fee_estimates = get_fee_estimates(access_token)

        raw_fee_record = {
            'collection_timestamp': timestamp.isoformat(),
            'raw_fee_estimates': fee_estimates
        }

        object_path = f"fees/current/{date_str}/fee_estimates_{timestamp_str}.json"
        save_to_minio(raw_fee_record, object_path, minio_client)
        
        logger.info(f"Collected raw current fee estimates at {timestamp_str}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to collect current fee data: {e}")
        return False

def main():
    global shutdown_requested
    try:
        logger.info(f"Starting Bitcoin data collection - FULL YEAR ({HISTORICAL_DAYS} DAYS)")
        logger.info("To gracefully stop the collection, press Ctrl+C")
        logger.info("The script will finish the current operation and save progress before exiting")
        
        logger.info("Initializing MinIO connection...")
        minio_client = initialize_minio()

        logger.info("Scanning existing data in MinIO...")
        existing_block_heights = get_existing_block_heights(minio_client)
        existing_fee_heights = get_existing_fee_data_points(minio_client)

        logger.info("Authenticating with Blockstream API...")
        access_token = authenticate()
        
        # Only collect current data if not shutting down
        if not shutdown_requested:
            logger.info("Collecting current raw mempool snapshot...")
            mempool_success = collect_current_mempool_data(access_token, minio_client)
            
            logger.info("Collecting current raw fee data...")
            fee_success = collect_current_fee_data(access_token, minio_client)
        else:
            mempool_success = fee_success = False

        start_height, end_height, blocks_to_collect, fee_heights_to_collect = calculate_historical_ranges(
            access_token, existing_block_heights, existing_fee_heights
        )

        # Show estimated completion time
        total_items = len(blocks_to_collect) + len(fee_heights_to_collect)
        if total_items > 0:
            estimated_hours = (total_items * RATE_LIMIT_DELAY * 2.5) / 3600
            estimated_completion = datetime.now() + timedelta(hours=estimated_hours)
            logger.info(f"Estimated completion time: {estimated_completion.strftime('%Y-%m-%d %H:%M:%S')}")

        block_collected, block_failed = 0, 0
        if blocks_to_collect and not shutdown_requested:
            logger.info(f"Starting historical block data collection for FULL YEAR ({HISTORICAL_DAYS} days)...")
            logger.info(f"This will collect {len(blocks_to_collect)} blocks and may take several days to complete")
            block_collected, block_failed = collect_historical_block_data(access_token, minio_client, blocks_to_collect)
        elif shutdown_requested:
            logger.info("Shutdown requested - skipping block collection")
        else:
            logger.info(f"No new blocks to collect - all blocks from last {HISTORICAL_DAYS} days already exist")

        fee_collected, fee_failed = 0, 0
        if fee_heights_to_collect and not shutdown_requested:
            logger.info(f"Starting historical fee data collection for FULL YEAR ({HISTORICAL_DAYS} days)...")
            logger.info(f"This will collect {len(fee_heights_to_collect)} fee data points")
            fee_collected, fee_failed = collect_historical_fee_data(access_token, minio_client, fee_heights_to_collect)
        elif shutdown_requested:
            logger.info("Shutdown requested - skipping fee collection")
        else:
            logger.info(f"No new fee data to collect - all fee data from last {HISTORICAL_DAYS} days already exists")
        
        final_status = "completed" if not shutdown_requested else "stopped_gracefully"
        
        summary = {
            'collection_status': final_status,
            'collection_completed': datetime.now().isoformat(),
            'historical_period_days': HISTORICAL_DAYS,
            'height_range': [start_height, end_height],
            'existing_blocks': len(existing_block_heights),
            'existing_fee_data_points': len(existing_fee_heights),
            'blocks_collected': block_collected,
            'blocks_failed': block_failed,
            'fee_data_collected': fee_collected,
            'fee_data_failed': fee_failed,
            'mempool_snapshot': mempool_success if not shutdown_requested else False,
            'current_fee_snapshot': fee_success if not shutdown_requested else False,
            'collection_type': f'full_year_{HISTORICAL_DAYS}_days_comprehensive',
            'shutdown_requested': shutdown_requested
        }
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_minio(summary, f"metadata/collection_summary_{timestamp}.json", minio_client)
        
        if shutdown_requested:
            logger.info("Data collection stopped gracefully by user request")
            logger.info("Progress has been saved. You can restart the script to continue from where it left off.")
        else:
            logger.info("Data collection completed successfully")
            
        logger.info(f"Summary: {summary}")
        
    except Exception as e:
        logger.error(f"Main process failed: {e}")
        if shutdown_requested:
            logger.info("Shutdown was requested, but an error occurred during graceful exit")
        raise

def collect_current_only():
    try:
        logger.info("Collecting current mempool and fee data only...")
        minio_client = initialize_minio()
        access_token = authenticate()
        
        mempool_success = collect_current_mempool_data(access_token, minio_client)
        fee_success = collect_current_fee_data(access_token, minio_client)
        
        if mempool_success and fee_success:
            logger.info("Current data snapshots collected successfully")
        else:
            logger.error("Failed to collect some current data")
            
        return mempool_success and fee_success
        
    except Exception as e:
        logger.error(f"Current data collection failed: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    # Allow running current-only collection with command line argument
    if len(sys.argv) > 1 and sys.argv[1] == "--current-only":
        collect_current_only()
    else:
        main()


