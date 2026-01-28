import boto3
import os
import sys
import logging
from botocore.exceptions import ClientError
from src import config

# Configure logging
logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)

def download_from_s3():
    """Download raw data from S3 to local raw directory"""
    
    # Check if AWS credentials are set
    if not config.AWS_ACCESS_KEY or not config.AWS_SECRET_KEY:
        logger.warning("AWS Credentials not found. Skipping S3 download.")
        logger.warning(f"Looking for local file at: {config.RAW_DATA_FILE}")
        if os.path.exists(config.RAW_DATA_FILE):
             logger.info("Local file found. Using existing data.")
             return True
        else:
             logger.error("No local data found and no AWS credentials provided.")
             return False

    s3_client = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_KEY,
        region_name=config.AWS_REGION
    )

    try:
        logger.info(f"Connecting to S3 bucket: {config.S3_BUCKET_NAME}")
        logger.info(f"Downloading key: {config.S3_RAW_DATA_KEY}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(config.RAW_DATA_FILE), exist_ok=True)
        
        s3_client.download_file(
            config.S3_BUCKET_NAME,
            config.S3_RAW_DATA_KEY,
            config.RAW_DATA_FILE
        )
        logger.info("S3 Download successful!")
        return True
        
    except ClientError as e:
        logger.error(f"S3 Download failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        return False

if __name__ == "__main__":
    if not download_from_s3():
        sys.exit(1)
