from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.types import IntegerType, DoubleType, FloatType
import os
import glob
import pickle
import pandas as pd
import sys

# Force Spark to use the current pipenv Python
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



import sys
from pathlib import Path

# Add project root to sys.path to allow importing src
project_root = str(Path(__file__).parent.parent.absolute())
if project_root not in sys.path:
    sys.path.append(project_root)

from src import config

def init_spark():
    """Initialize Spark Session with optimized settings"""
    spark = SparkSession.builder \
        .appName(config.SPARK_APP_NAME) \
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS) \
        .getOrCreate()
    return spark


def read_csv(spark, path):
    """Read CSV file into Spark DataFrame"""
    df = spark.read.csv(path, header=True, inferSchema=True)
    return df


def clean_and_transform(df):
    """
    Apply ETL transformations to the churn dataset
    
    Transformations:
    1. Drop duplicates on customerID
    2. Convert SeniorCitizen and tenure to int64
    3. Convert MonthlyCharges and TotalCharges to float64
    4. Replace 'No phone service' with 'No'
    5. Replace 'No internet service' with 'No'
    6. Encode Yes/No columns to 1/0
    7. Encode gender (Male=0, Female=1)
    8. Create dummy variables for categorical columns
    9. Apply Min-Max scaling to tenure, MonthlyCharges, TotalCharges
    """
    # Drop Duplicates on customerID
    df = df.dropDuplicates(['customerID'])
   

    # Convert SeniorCitizen and tenure to int
    df = df.withColumn("SeniorCitizen", col("SeniorCitizen").cast(IntegerType()))
    df = df.withColumn("tenure", col("tenure").cast(IntegerType()))
    
    # Clean TotalCharges (remove spaces) and convert to float
    df = df.withColumn("TotalCharges", 
                       regexp_replace(col("TotalCharges"), " ", ""))
    df = df.withColumn("TotalCharges", 
                       when(col("TotalCharges") == "", None)
                       .otherwise(col("TotalCharges").cast(FloatType())))
    
    # Convert MonthlyCharges to float
    df = df.withColumn("MonthlyCharges", col("MonthlyCharges").cast(FloatType()))
    
    # Replace 'No phone service' with 'No'
    df = df.withColumn("MultipleLines", 
                       when(col("MultipleLines") == "No phone service", "No")
                       .otherwise(col("MultipleLines")))
    
    # Replace 'No internet service' with 'No' for all internet-related columns
    internet_columns = ['OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 
                       'TechSupport', 'StreamingTV', 'StreamingMovies']
    
    for column in internet_columns:
        df = df.withColumn(column, 
                          when(col(column) == "No internet service", "No")
                          .otherwise(col(column)))


    # Binary columns to encode (Yes/No -> 1/0)
    binary_columns = ['Partner', 'Dependents', 'PhoneService', 'MultipleLines',
                     'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 
                     'TechSupport', 'StreamingTV', 'StreamingMovies',
                     'PaperlessBilling']
    
    for column in binary_columns:
        df = df.withColumn(column,
                          when(col(column) == "Yes", 1)
                          .when(col(column) == "No", 0)
                          .otherwise(col(column)))
    
    # Gender Encoding (Male=0, Female=1)
    df = df.withColumn("gender",
                      when(col("gender") == "Male", 0)
                      .when(col("gender") == "Female", 1)
                      .otherwise(col("gender")))
    
    # Create dummy variables for categorical columns
    categorical_columns = ['InternetService', 'Contract', 'PaymentMethod']
    
    for column in categorical_columns:
        # Get unique values using DataFrame operations (not RDD)
        unique_values = [row[column] for row in df.select(column).distinct().collect()]
        unique_values = sorted([v for v in unique_values if v is not None])
        
        # Create dummy columns 
        for value in unique_values:
            new_col_name = f"{column}_{value.replace(' ', '_')}"
            df = df.withColumn(new_col_name,when(col(column) == value, 1).otherwise(0))
        
        # Drop original categorical column
        df = df.drop(column)
    
    # Min-Max Scaling for numerical columns
    # Formula: (value - min) / (max - min)
    from pyspark.sql.functions import min as spark_min, max as spark_max
    
    scaling_columns = ['tenure', 'MonthlyCharges', 'TotalCharges']
    
    for column in scaling_columns:
        # Get min and max values using Spark functions
        stats = df.select(
            spark_min(col(column)).alias('min_val'),
            spark_max(col(column)).alias('max_val')
        ).collect()[0]
        
        min_val = stats['min_val']
        max_val = stats['max_val']
        
        # Apply Min-Max scaling
        # Handle case where min == max to avoid division by zero
        if max_val is not None and min_val is not None and max_val != min_val:
            df = df.withColumn(
                column,
                when(col(column).isNull(), None)
                .otherwise((col(column) - min_val) / (max_val - min_val))
            )
    
    return df


def save_transformed_data(df, output_path):
    """ Save the transformed data to CSV format """
    # Convert Spark DataFrame to Pandas
    pandas_df = df.toPandas()
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save as CSV with .csv extension
    if not output_path.endswith('.csv'):
        output_path = output_path + '.csv'
    
    pandas_df.to_csv(output_path, index=False)





def run_etl():
    """Main ETL execution function for Airflow/Orchestration"""
    # Initialize Spark
    spark = init_spark()
    
    # Read raw data
    print("\nReading raw data...")
    df = read_csv(spark, config.RAW_DATA_FILE)
    print(f"Raw data loaded: {df.count()} rows, {len(df.columns)} columns")

    # Apply transformations
    try:
        df_transformed = clean_and_transform(df)
        print("Data transformation completed")
        # Save transformed data
        save_transformed_data(df_transformed, config.FEATURED_DATA_FILE)
        print(f"Transformed data saved to: {config.FEATURED_DATA_FILE}")
    except Exception as e:
        print(f"Error during data transformation: {str(e)}")
        spark.stop()
        raise e
    
    spark.stop()

if __name__=='__main__':
    run_etl()
