# Base Image: Python 3.10 on Debian Bookworm (Stable)
FROM python:3.10-slim-bookworm

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME="/app/airflow"
ENV PYTHONPATH="/app"
ENV PYSPARK_PYTHON="python"
ENV PYSPARK_DRIVER_PYTHON="python"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# 1. Install System Dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    gcc \
    python3-dev \
    libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# 2. Robust Installation of Python Packages
# Using Airflow constraints as the primary source of truth for versions
RUN pip install --no-cache-dir --upgrade pip --default-timeout=1000 && \
    pip install --no-cache-dir --default-timeout=1000 \
    "apache-airflow[celery,amazon,postgres]==2.7.1" \
    "pyspark" \
    "pandas" \
    "tensorflow" \
    "keras" \
    "boto3" \
    "sqlalchemy" \
    "psycopg2-binary" \
    "python-dotenv" \
    "psycopg2" \
    "scikit-learn" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.10.txt"

# 3. Setup Project Structure
COPY src/ ./src/
COPY ETL/ ./ETL/
COPY models/ ./models/
COPY dags/ ./airflow/dags/

# Create necessary directories
RUN mkdir -p data/raw data/featured data/predictions ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/dags

# Default Command
# Standalone is good for testing/local, but compose uses specific commands
CMD ["airflow", "standalone"]
