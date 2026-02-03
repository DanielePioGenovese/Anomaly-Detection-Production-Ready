"""
Configuration for the Isolation Forest model
"""
from pathlib import Path

# Data paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data/historical_data"
MODEL_DIR = BASE_DIR / "models"
METRICS_DIR = BASE_DIR / "metrics"

# Data files
TRAIN_FILENAME= "train_set.parquet"
TRAIN_PATH= DATA_DIR / TRAIN_FILENAME

TEST_FILENAME= "test_set.parquet"
TEST_PATH= DATA_DIR / TEST_FILENAME

# Isolation Forest parameters
ISOLATION_FOREST_PARAMS = {
    'n_estimators': 100,
    'max_samples': 'auto',
    'contamination': 0.1,  # Expected percentage of anomalies
    'max_features': 1.0,
    'bootstrap': False,
    'random_state': 42,
    'n_jobs': -1,
    'verbose': 0
}

# #Output files
MODEL_FILENAME = "isolation_forest_model.pkl"
METRICS_FILENAME = "training_metrics.json"
SCALER_FILENAME = "scaler.pkl"

# Full output paths
MODEL_PATH = MODEL_DIR / MODEL_FILENAME
SCALER_PATH = MODEL_DIR / SCALER_FILENAME
METRICS_PATH = METRICS_DIR / METRICS_FILENAME