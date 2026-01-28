# 📉 Automated Customer Churn Scoring Pipeline

##  Overview
This project implements a production-grade **MLOps pipeline** for batch customer churn prediction. It automates the end-to-end process of data ingestion, processing, model inference, and results storage using **Apache Airflow**, **PySpark**, and **AWS**.

The pipeline is **event-driven**, designed to detect new data arriving in an S3 bucket, process it using distributed computing techniques (Spark), generate churn probabilities using a pre-trained Keras model, and load the results into a Production Database (PostgreSQL).

##  Architecture
1.  **Ingestion**: An **Airflow S3 Key Sensor** detects new raw data (`customer_churn.csv`) in the AWS S3 bucket.
2.  **ETL**: **PySpark** performs data cleaning, One-Hot Encoding, and Min-Max scaling.
3.  **Inference**: A containerized **TensorFlow/Keras** model generates batch predictions.
4.  **Loading**: Results are archived back to **S3** and inserted into an **AWS RDS PostgreSQL** database.
5.  **Monitoring**: Real-time **Email Notifications** (SMTP) report pipeline success or failure.

## Tech Stack
*   **Orchestration**: Apache Airflow 2.7
*   **Data Processing**: PySpark 3.5
*   **Machine Learning**: TensorFlow / Keras & Scikit-Learn
*   **Containerization**: Docker & Docker Compose
*   **Cloud Infrastructure**: AWS S3 (Data Lake) & AWS RDS (PostgreSQL)
*   **Language**: Python 3.10

---

##  Project Structure
```bash
├── dags/
│   └── churn_scoring_dag.py    # Airflow DAG definition
├── ETL/
│   └── etl_job.py              # PySpark Transformation logic
├── src/
│   ├── config.py               # Centralized configuration
│   ├── ingest.py               # S3 Download script
│   ├── inference.py            # Model batch prediction logic
│   ├── loader.py               # S3 Upload & Postgres Insert logic
│   └── test_connections.py     # Connectivity verification tool
├── models/
│   └── model.pkl               # Pre-trained ML Model
├── docker-compose.yml          # Container services definition
├── Dockerfile                  # Custom Image with Spark, Airflow, TF
├── Pipfile                     # Python dependencies
└── README.md                   # Project Documentation
```

---

##  Setup & Installation

### 1. Prerequisites
*   **Docker Desktop** (running with at least 8GB RAM recommended)
*   **AWS Account** (S3 Bucket & RDS Instance)
*   **Gmail Account** (for SMTP notifications - App Password required)

### 2. Clone the Repository
```bash
git clone https://github.com/your-username/churn-pipeline.git
cd churn-pipeline
```

### 3. Configure Environment
Create a `.env` file in the root directory with your credentials:

```ini
# AWS S3 Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# Database Credentials (AWS RDS or Local)
DB_HOST=your-rds-endpoint.amazonaws.com
DB_PORT=5432
DB_NAME=churn_db
DB_USER=postgres
DB_PASSWORD=your_db_password

# Email / SMTP Settings (Gmail Example)
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
SMTP_MAIL_FROM=your_email@gmail.com
NOTIFICATION_EMAIL=recipient_email@gmail.com
```

### 4. Build and Run
Launch the entire stack using Docker Compose:

```bash
docker-compose up --build
```
*Note: The first build may take 10-15 minutes to download Spark and TensorFlow layers.*

---

##  Usage

1.  **Access Airflow UI**:
    *   Open [http://localhost:8080](http://localhost:8080)
    *   Login: `admin` / `admin`

2.  **Enable the Pipeline**:
    *   Toggle the `customer_churn_scoring_pipeline` DAG to **ON**.

3.  **Trigger the Workflow**:
    *   **Option A (Auto)**: Upload a file named `customer_churn.csv` to the `raw/` folder in your S3 bucket. The sensor will detect it within 10 seconds.
    *   **Option B (Manual)**: Click the "Play" button in the Airflow UI to perform a test run.

4.  **View Results**:
    *   **S3**: Check the `predictions/` folder for the timestamped CSV.
    *   **Database**: Query the `churn_predictions` table in your PostgreSQL instance.

---

##  Testing
To test connections before running the full pipeline, you can use the utility script inside the container or locally:

```bash
# Run inside container
docker exec -it airflow_webserver python src/test_connections.py
```

##  License
This project is licensed under the MIT License. Only for educational purposes.
