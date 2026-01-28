import boto3
import psycopg2
import sys
import logging
from src import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_s3_connection():
    """Test AWS S3 Connection"""
    logger.info("Testing S3 Connection...")
    
    if not config.AWS_ACCESS_KEY:
        logger.error("AWS Credentials missing in .env")
        return False
        
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY,
            aws_secret_access_key=config.AWS_SECRET_KEY,
            region_name=config.AWS_REGION
        )
        
        # Try to list buckets or specific bucket
        response = s3.list_buckets()
        logger.info(f"S3 Connection Successful! Authenticated as owner of {len(response['Buckets'])} buckets.")
        
        # Check specific bucket
        try:
            s3.head_bucket(Bucket=config.S3_BUCKET_NAME)
            logger.info(f"Found bucket: {config.S3_BUCKET_NAME}")
        except Exception as e:
            logger.error(f"Could not find/access bucket '{config.S3_BUCKET_NAME}': {e}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"S3 Connection Failed: {e}")
        return False

def test_db_connection():
    """Test PostgreSQL Database Connection"""
    logger.info("Testing Database Connection...")
    
    try:
        # Try connecting to the configured DB
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            connect_timeout=5
        )
        logger.info(f"Database Connection Successful! Connected to '{config.DB_NAME}'")
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        if 'database "' + config.DB_NAME + '" does not exist' in str(e):
            logger.warning(f"Database '{config.DB_NAME}' does not exist. Attempting to create it...")
            
            # Connect to default 'postgres' DB to create the new DB
            try:
                conn = psycopg2.connect(
                    host=config.DB_HOST,
                    port=config.DB_PORT,
                    dbname="postgres",  # Default DB
                    user=config.DB_USER,
                    password=config.DB_PASSWORD,
                    connect_timeout=5
                )
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(f"CREATE DATABASE {config.DB_NAME};")
                cur.close()
                conn.close()
                logger.info(f"Successfully created database '{config.DB_NAME}'!")
                return True
            except Exception as create_error:
                logger.error(f"Failed to create database: {create_error}")
                return False
        else:
            logger.error(f"Database Connection Failed: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Database Connection Failed: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "="*50)
    print("CONNECTION TESTER")
    print("="*50 + "\n")
    
    s3_test = test_s3_connection()
    print("-" * 30)
    db_test = test_db_connection()
    
    print("\n" + "="*50)
    if s3_test and db_test:
        print("ALL SYSTEMS GO! READY FOR PRODUCTION.")
    else:
        print("SOME CHECKS FAILED. PLEASE FIX .env")
    print("="*50 + "\n")
