import os
from pathlib import Path
from typing import Dict, Any

class Config:
    """Centralized configuration for the Washing Machine Batch Pipeline"""

    # Spark Configuration
    SPARK_APP_NAME = "WashingMachine-Batch-Append-Clean"
    SPARK_TIMEZONE = "UTC"
    SPARK_CONFIG: Dict[str, str] = {
        "spark.sql.session.timeZone": SPARK_TIMEZONE,
    }

    # Data Paths
    HISTORICAL_DIR = os.getenv("HISTORICAL_DIR", "/app/data/historical_data")
    OFFLINE_DIR = os.getenv("OFFLINE_DIR", "/app/data/feature_store")
    OUTPUT_FEATURE_NAME = "machine_batch_features"
    OUTPUT_PATH = Path(OFFLINE_DIR) / OUTPUT_FEATURE_NAME

    # Feature Engineering
    FEATURE_CONFIG_PATH = "config/feature_config.yaml"
    
    # Processing Configuration
    OUTPUT_REPARTITION = 1
    WRITE_MODE = "append"  # Options: "overwrite", "append", "ignore", "error"
    
    # Feature Column Names
    MACHINE_ID_COL = "Machine_ID"
    TIMESTAMP_COL = "timestamp"
    TIMESTAMP_GRAIN = "day"  # Options: "day", "hour", "minute"
    TARGET_FEATURE_COL = "Daily_Vibration_PeakMean_Ratio"
    OUTPUT_TIMESTAMP_COL = "event_timestamp"
    
    # Output Schema
    OUTPUT_COLUMNS = [
        MACHINE_ID_COL,
        OUTPUT_TIMESTAMP_COL,
        TARGET_FEATURE_COL,
    ]
    
    # Quality Checks
    ALLOW_NULL_VALUES = False
    
    # Logging
    SHOW_SAMPLE_ROWS = 5
    
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "spark_app_name": cls.SPARK_APP_NAME,
            "spark_timezone": cls.SPARK_TIMEZONE,
            "historical_dir": cls.HISTORICAL_DIR,
            "offline_dir": cls.OFFLINE_DIR,
            "output_path": str(cls.OUTPUT_PATH),
            "feature_config_path": cls.FEATURE_CONFIG_PATH,
            "write_mode": cls.WRITE_MODE,
            "output_columns": cls.OUTPUT_COLUMNS,
        }
    
    @classmethod
    def validate(cls) -> bool:
        """Validate critical configuration paths"""
        try:
            if not Path(cls.HISTORICAL_DIR).exists():
                print(f"[!] Warning: HISTORICAL_DIR does not exist: {cls.HISTORICAL_DIR}")
            return True
        except Exception as e:
            print(f"[!] Configuration validation error: {e}")
            return False