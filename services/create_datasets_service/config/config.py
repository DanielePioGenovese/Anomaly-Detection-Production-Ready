from pathlib import Path

"""
Configuration file for Industrial Washer Dataset Generator
"""
BASE_DIR = Path(__file__).resolve().parent.parent 

# directory
# MODEL_REGISTRY_PATH = BASE_DIR / "models" 

DATASETS_PATH = Path('data') / 'synthetic_datasets'

# Spark configurations
SPARK_APP_NAME = "Industrial Washer Generator"
SPARK_DRIVER_MEMORY = "4g"
SPARK_SHUFFLE_PARTITIONS = 200

# Dataset parameters
DEFAULT_NUM_ROWS = 1_000_000
DEFAULT_ANOMALY_RATE = 0.02  # 2%

# Machine configuration
NUM_MACHINES = 10  # Changed from 50 to 10

# Time configuration
TIMESTAMP_START = "2024-01-01 00:00:00"
TIMESTAMP_INTERVAL_SECONDS = 1  # All machines report every 1 second