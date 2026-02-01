import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    # 1. Project Root
    # Path(__file__) is 'config/config_file.py' -> .parent is 'config/' -> .parent is 'root/'
    ROOT_DIR = Path(__file__).resolve().parent.parent
    
    # 2. Kafka Settings (Matches Docker compose)
    # We grab from .env, but provide 'localhost:9092' as a safe default
    KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC_TELEMETRY = os.getenv('TOPIC_TELEMETRY', 'telemetry-data')

    # 3. Data Directories
    # Use the root + the folder name from .env
    DATA_DIR = ROOT_DIR / "data"
    STREAMING_DIR = DATA_DIR / os.getenv('STREAMING_DIR', 'streaming_data')
    HISTORICAL_DIR = DATA_DIR / os.getenv('HISTORICAL_DIR', 'historical_data')
    
    SYNTHETIC_DIR = DATA_DIR / "synthetic_data_creation"
    
    # 4. Files
    INPUT_DATA_PATH = SYNTHETIC_DIR / "test_data.csv"
    SYNTHETIC_OUTPUT_PATH = SYNTHETIC_DIR / "synthetic_data_sdv.parquet"

    STREAMING_DATASET = STREAMING_DIR / os.getenv('STREAMING_DATASET', '')

    # 5. ML Settings
    TARGET = os.getenv('TARGET_COLUMN', 'Is_Anomaly')
    
    # Logic for columns to drop:
    # If the .env has "Col1,Col2", this converts it into a Python list ['Col1', 'Col2']
    _drop_cols_str = os.getenv('COLUMNS_TO_DROP_TRAIN', 'Anomaly_Type,timestamp,Machine_ID')
    DROP_COLUMNS = [c.strip() for c in _drop_cols_str.split(',')]