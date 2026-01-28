import subprocess
import sys
import os
import logging
from src import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format=config.LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log")
    ]
)
logger = logging.getLogger(__name__)

def run_step(command, step_name):
    """Run a pipeline step as a subprocess"""
    logger.info(f"STARTING STEP: {step_name}")
    try:
        # Add current directory to PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = str(config.ROOT_DIR)
        if "PYTHONPATH" in os.environ:
             env["PYTHONPATH"] = str(config.ROOT_DIR) + os.pathsep + os.environ["PYTHONPATH"]

        # Run command and stream output
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            shell=True,
            env=env
        )
        
        # Print output in real-time
        for line in process.stdout:
            print(line, end='')
            
        process.wait()
        
        if process.returncode != 0:
            logger.error(f"STEP FAILED: {step_name}")
            # Print stderr
            print(process.stderr.read())
            return False
            
        logger.info(f"STEP COMPLETED: {step_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error executing {step_name}: {e}")
        return False

def main():
    
    # Step 1: Ingestion
    ingest_cmd = f"{sys.executable} -m src.ingest"
    if not run_step(ingest_cmd, "Data Ingestion (S3)"):
        # We proceed if local file exists (handled by script)
        pass 
        
    # Step 2: ETL
    etl_cmd = f"{sys.executable} ETL/etl_job.py"
    if not run_step(etl_cmd, "ETL Transformation"):
        sys.exit(1)
    
    # Step 3: Inference
    inference_cmd = f"{sys.executable} -m src.inference"
    if not run_step(inference_cmd, "Batch Inference"):
        sys.exit(1)
        
    # Step 4: Loading
    loader_cmd = f"{sys.executable} -m src.loader"
    if not run_step(loader_cmd, "Data Loading (S3/DB)"):
        logger.warning("Loading step had issues (check logs).")

        
    print("\n" + "="*50)
    print("PIPELINE FINISHED SUCCESSFULLY")
    print(f"Predictions available at: {config.PREDICTIONS_FILE}")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
