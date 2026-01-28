import pandas as pd
import pickle
import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_data(filepath):
    """Load transformed features from CSV"""
    if not os.path.exists(filepath):
        logger.error(f"Input file not found: {filepath}")
        sys.exit(1)
        
    logger.info(f"Loading data from {filepath}")
    return pd.read_csv(filepath)

def load_model(model_path):
    """Load the trained model pickle"""
    if not os.path.exists(model_path):
        logger.error(f"Model file not found: {model_path}")
        logger.error("Please train a model first or check the path.")
        sys.exit(1)
        
    logger.info(f"Loading model from {model_path}")
    try:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        return model
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        sys.exit(1)

def run_inference(input_path, model_path, output_path):
    """Run batch inference pipeline"""
    
    # 1. Load Data
    df = load_data(input_path)
    
    # 2. Prepare Features
    # Save customerID to re-attach later
    if 'customerID' not in df.columns:
        logger.error("customerID column missing from input data")
        sys.exit(1)
        
    customer_ids = df['customerID']
    features = df.drop('customerID', axis=1)
    
    # 3. Load Model
    model = load_model(model_path)
    
    # 4. Generate Predictions
    logger.info("Generating predictions...")
    try:
        predictions = model.predict(features)
        
        # Flatten predictions if they are 2D (Keras/TensorFlow often returns [[prob], [prob]])
        if len(predictions.shape) > 1 and predictions.shape[1] == 1:
            predictions = predictions.flatten()
            
        # Try to get probabilities if available (for classification)
        if hasattr(model, "predict_proba"):
            probs = model.predict_proba(features)[:, 1] # Probability of Churn=1
        else:
            # For Keras, the prediction IS often the probability
            probs = predictions if predictions.dtype == float else None
            # If predictions are probabilities, assume threshold 0.5 for label
            if probs is not None:
                predictions = (probs > 0.5).astype(int)
            
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        logger.error(f"Feature columns expected by model vs provided: \n{features.columns.tolist()}")
        sys.exit(1)
        
    # 5. Create Results DataFrame
    results = pd.DataFrame({
        'customerID': customer_ids,
        'prediction': predictions
    })
    
    if probs is not None:
        results['churn_probability'] = probs
        
    # 6. Save Results
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    results.to_csv(output_path, index=False)
    logger.info(f"Predictions saved to {output_path}")
    logger.info(f"Processed {len(results)} customers")

def run_main_inference():
    """Main inference execution function for Airflow/Orchestration"""
    from src import config
    run_inference(
        config.FEATURED_DATA_FILE, 
        config.MODEL_FILE, 
        config.PREDICTIONS_FILE
    )

if __name__ == "__main__":
    run_main_inference()

