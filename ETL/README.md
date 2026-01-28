# Customer Churn ETL Pipeline

## Overview
This ETL (Extract, Transform, Load) pipeline processes raw customer churn data and prepares it for machine learning model training.

## Pipeline Steps

### 1. **Data Extraction**
- **Input**: `data/raw/customer_churn.csv`
- **Rows**: 7,043 customers
- **Columns**: 20 features

### 2. **Data Transformation**

The pipeline applies the following transformations:

#### **Step 1: Data Quality**
- Remove duplicate records based on `customerID`

#### **Step 2-3: Type Conversions**
- Convert `SeniorCitizen` and `tenure` to **integer**
- Convert `MonthlyCharges` and `TotalCharges` to **float**
- Clean `TotalCharges` (remove spaces, handle empty values)

#### **Step 4-5: Value Standardization**
- Replace `"No phone service"` → `"No"`
- Replace `"No internet service"` → `"No"` (for all internet-related columns)

#### **Step 6: Binary Encoding**
Encode Yes/No columns to 1/0:
- Partner, Dependents, PhoneService, MultipleLines
- OnlineSecurity, OnlineBackup, DeviceProtection
- TechSupport, StreamingTV, StreamingMovies
- PaperlessBilling

#### **Step 7: Gender Encoding**
- Male → 0
- Female → 1

#### **Step 8: One-Hot Encoding (Dummy Variables)**
Create dummy variables for categorical columns:
- **InternetService**: DSL, Fiber optic, No
- **Contract**: Month-to-month, One year, Two year
- **PaymentMethod**: Electronic check, Mailed check, Bank transfer, Credit card

#### **Step 9: Min-Max Scaling**
Normalize numerical features to [0, 1] range:
- `tenure`: Customer tenure in months
- `MonthlyCharges`: Monthly billing amount
- `TotalCharges`: Total amount charged

**Formula**: `scaled_value = (value - min) / (max - min)`

### 3. **Data Loading**
- **Output**: `data/featured/customer_churn_featured.csv`
- **Format**: CSV with headers
- **Size**: ~809 KB
- **Method**: Converted to Pandas for Windows compatibility

## Technical Details

### Technologies Used
- **PySpark**: Distributed data processing
- **Pandas**: Final CSV export (Windows compatibility)
- **Python 3.x**


### File Structure
```
Customer Churn/
├── data/
│   ├── raw/
│   │   └── customer_churn.csv          # Original data
│   └── featured/
│       └── customer_churn_featured.csv # Transformed data ready for Classification
├── ETL/
    ├── etl_job.py                      # Main ETL script
    └── README.md                       

```

## How to Run

### Prerequisites
```bash
pipenv install pyspark pandas
```

### Execute ETL Pipeline
```bash
# From project root
pipenv run python ETL\etl_job.py
```

