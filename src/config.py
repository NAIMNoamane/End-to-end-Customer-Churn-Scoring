import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

# Project Root Directory
ROOT_DIR = Path(__file__).parent.parent.absolute()

# Data Directories
DATA_DIR = os.path.join(ROOT_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
FEATURED_DATA_DIR = os.path.join(DATA_DIR, "featured")
PREDICTIONS_DIR = os.path.join(DATA_DIR, "predictions")
MODELS_DIR = os.path.join(ROOT_DIR, "models")

# File Paths
RAW_DATA_FILE = os.path.join(RAW_DATA_DIR, "customer_churn.csv")
FEATURED_DATA_FILE = os.path.join(FEATURED_DATA_DIR, "customer_churn_featured.csv")
MODEL_FILE = os.path.join(MODELS_DIR, "model.pkl")
PREDICTIONS_FILE = os.path.join(PREDICTIONS_DIR, "churn_predictions.csv")

# Spark Settings
SPARK_APP_NAME = "churn-pipeline"
SPARK_SHUFFLE_PARTITIONS = "4"

# Logging Settings
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = "INFO"

# AWS S3 Settings
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "churn-project-data")
S3_RAW_DATA_KEY = "raw/customer_churn.csv"
S3_PREDICTIONS_KEY_PREFIX = "predictions/"

# PostgreSQL Settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "churn_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_TABLE_NAME = "churn_predictions"

# Email / SMTP Settings
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_MAIL_FROM = os.getenv("SMTP_MAIL_FROM", "churn-alerts@example.com")
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL")

