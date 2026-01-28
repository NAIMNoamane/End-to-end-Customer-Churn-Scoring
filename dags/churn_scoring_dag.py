from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import sys
import os

# Add project root to sys.path
sys.path.append('/app')

from src.ingest import download_from_s3
from ETL.etl_job import run_etl
from src.inference import run_main_inference
from src.loader import run_loader
from src import config

def email_failure_notification(context):
    """Send email on task failure"""
    subject = f"Airflow Task Failed: {context['task_instance'].task_id}"
    body = f"""
    <h2>Task Failed</h2>
    <p><b>Task:</b> {context['task_instance'].task_id}</p>
    <p><b>DAG:</b> {context['task_instance'].dag_id}</p>
    <p><b>Execution Time:</b> {context['execution_date']}</p>
    <p><b>Log URL:</b> <a href="{context['task_instance'].log_url}">{context['task_instance'].log_url}</a></p>
    """
    if config.NOTIFICATION_EMAIL:
        send_email(to=config.NOTIFICATION_EMAIL, subject=subject, html_content=body)

def email_success_notification(context):
    """Send email on DAG success (optional task)"""
    subject = f"Airflow Pipeline Success: {context['dag'].dag_id}"
    body = f"""
    <h2>Pipeline Completed Successfully</h2>
    <p><b>DAG:</b> {context['dag'].dag_id}</p>
    <p><b>Execution Time:</b> {context['execution_date']}</p>
    <p><b>Predictions Saved to:</b> {config.PREDICTIONS_FILE}</p>
    """
    if config.NOTIFICATION_EMAIL:
        send_email(to=config.NOTIFICATION_EMAIL, subject=subject, html_content=body)

default_args = {
    'owner': 'churn_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': [config.NOTIFICATION_EMAIL] if config.NOTIFICATION_EMAIL else [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': email_failure_notification
}

with DAG(
    'customer_churn_scoring_pipeline',
    default_args=default_args,
    description='End-to-end churn scoring pipeline - Data Triggered',
    schedule_interval='@daily',
    catchup=False,
    tags=['churn', 'pyspark', 'ml']
) as dag:

    # 1. Wait for file to arrive in S3
    wait_for_file = S3KeySensor(
        task_id='wait_for_s3_file',
        bucket_name=config.S3_BUCKET_NAME,
        bucket_key=config.S3_RAW_DATA_KEY,
        aws_conn_id=None,
        timeout=18 * 60 * 60, # Timeout after 18 hours
        poke_interval=10      # Check every 10 seconds
    )

    ingest_task = PythonOperator(
        task_id='ingest_data_from_s3',
        python_callable=download_from_s3
    )

    etl_task = PythonOperator(
        task_id='pyspark_etl_transformation',
        python_callable=run_etl
    )

    inference_task = PythonOperator(
        task_id='model_inference_keras',
        python_callable=run_main_inference
    )

    load_task = PythonOperator(
        task_id='load_results_to_s3_and_rds',
        python_callable=run_loader,
        on_success_callback=email_success_notification
    )

    # Define task dependencies
    wait_for_file >> ingest_task >> etl_task >> inference_task >> load_task
