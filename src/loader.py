import pandas as pd
import boto3
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import os
import sys
import logging
from datetime import datetime
from src import config

# Configure logging
logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)

def upload_to_s3(local_file):
    """Upload predictions to S3"""
    if not config.AWS_ACCESS_KEY:
        logger.warning("AWS Credentials missing. Skipping S3 upload.")
        return

    s3 = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_KEY,
        region_name=config.AWS_REGION
    )
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{config.S3_PREDICTIONS_KEY_PREFIX}predictions_{timestamp}.csv"
    
    try:
        logger.info(f"Uploading to S3: {s3_key}")
        s3.upload_file(local_file, config.S3_BUCKET_NAME, s3_key)
        logger.info("S3 Upload successful")
    except Exception as e:
        logger.error(f"S3 Upload failed: {e}")

def load_to_postgres(filepath):
    """Load predictions into PostgreSQL"""
    try:
        # Check if we have DB credentials (simple check)
        if config.DB_HOST == "localhost" and config.DB_PASSWORD == "postgres":
             # This might be default/unset, but we try anyway if it's running locally
             pass
        
        logger.info(f"Connecting to Database {config.DB_NAME} at {config.DB_HOST}...")
        
        # Create SQLAlchemy engine
        connection_str = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
        engine = create_engine(connection_str)
        
        # Read Data
        df = pd.read_csv(filepath)
        
        # Write to DB
        df.to_sql(
            config.DB_TABLE_NAME, 
            engine, 
            if_exists='append', 
            index=False
        )
        logger.info(f"Successfully loaded {len(df)} rows to PostgreSQL table '{config.DB_TABLE_NAME}'")
        
    except Exception as e:
        logger.error(f"Database load failed: {str(e)}")
        # We don't exit(1) here because maybe S3 worked, or it's just a connection issue.
        # Strictness depends on requirement.

def run_loader():
    """Main loader logic"""
    if not os.path.exists(config.PREDICTIONS_FILE):
        logger.error(f"Predictions file not found: {config.PREDICTIONS_FILE}")
        sys.exit(1)
        
    # 1. Archive to S3
    upload_to_s3(config.PREDICTIONS_FILE)
    
    # 2. Push to Database
    load_to_postgres(config.PREDICTIONS_FILE)

if __name__ == "__main__":
    print("\n" + "="*50)
    print("STARTING DATA LOADING")
    print("="*50)
    
    run_loader()
    
    print("="*50)
    print("LOADING COMPLETED")
    print("="*50 + "\n")
