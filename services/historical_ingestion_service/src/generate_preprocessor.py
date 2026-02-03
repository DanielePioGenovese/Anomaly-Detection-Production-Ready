# generate_preprocessor.py
import logging
import joblib
import pandas as pd
from pathlib import Path
from config import IsolationForestConfig

# Import your classes from your project files
from services.historical_ingestion_service.src import DataLoader, DataPreprocessor

# Basic logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ArtifactGenerator")

RAW_DATA_PATH = Path(IsolationForestConfig.TRAIN_PATH)  
OUTPUT_PATH = Path("models/preprocessor.joblib")

def generate():
    # 1. Load data
    logger.info("Loading data...")
    # DataLoader handles reading CSV/Parquet files

    # ---- Loading the Historical data
    loader = DataLoader(data_dir=RAW_DATA_PATH)
    try:
        # Try loading parquet files first
        df = loader.load_data(filename="*.parquet")
    except:
        # Fallback to CSV if parquet fails
        df = loader.load_data(filename="*.csv")

    # 2. Initialize the Preprocessor
    # Define columns to EXCLUDE from scaling (ID, timestamp, label)
    # ---- Preprocessing data
    preprocessor = DataPreprocessor(
        label_columns=['Is_Anomaly', 'Anomaly_Type', 'Machine_ID', 'timestamp'],
        scaler_type='standard'
    )

    # 3. Fit (compute mean/variance)
    logger.info("Fitting the preprocessor...")
    # This computes statistics and populates self.feature_columns
    preprocessor.preprocess_data(df, fit=True)

    # 4. Save preprocessor
    logger.info(f"Saving to {OUTPUT_PATH}...")
    # We use the modified save_scaler method from before
    preprocessor.save_scaler(OUTPUT_PATH)
    
    logger.info("✅ Done! You can now start streaming.")

