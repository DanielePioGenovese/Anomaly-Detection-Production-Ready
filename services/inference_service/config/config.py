import os
from pathlib import Path

class Config:
    """Internal configuration for the Inference Service"""
    
    # Root directories mapped in docker-compose
    DATA_DIR = Path('/inference_service/data')
    MODELS_DIR = DATA_DIR / 'models'
    
    # Model Artifacts
    MODEL_PATH = MODELS_DIR / 'isolation_forest_model.pkl'
    PREPROCESSOR_JOBLIB = MODELS_DIR / 'preprocessor.joblib'

    # List of columns to drop - matching training configuration
    _drop_cols_str = os.getenv('COLUMNS_TO_DROP', 'Anomaly_Type,timestamp,Machine_ID,Is_Anomaly')
    DROP_COLUMNS = [c.strip() for c in _drop_cols_str.split(',')]
