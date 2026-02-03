"""
Package for training Isolation Forest models

This package provides a complete modular structure for:
- Configuration and path validation
- Data loading and preprocessing
- Isolation Forest model training
- Metrics computation and saving
- Complete training pipeline

Main modules:
- config_model: Parameter configuration
- validation: Configuration and path validation
- dataloader: Data loading and preprocessing
- model: Isolation Forest model
- metrics: Evaluation metrics computation
- train: Training pipeline

Usage example:
    from train import TrainingPipeline
    
    pipeline = TrainingPipeline()
    metrics = pipeline.run(
        file_pattern="*.csv",
        target_column="is_anomaly",
        drop_columns=['id', 'timestamp']
    )
"""


__version__ = "1.0.0"
__author__ = "Denise"

# Import main classes
from .validation import ConfigValidator
from .dataloader import DataLoader
from .model import IsolationForestModel
from .metrics import MetricsCalculator
from .train import TrainingPipeline

# Import configurations
from .config_model import (
    DATA_DIR,
    MODEL_DIR,
    METRICS_DIR,
    ISOLATION_FOREST_PARAMS,
    TRAIN_FILENAME,
    TEST_FILENAME,
    MODEL_FILENAME,
    METRICS_FILENAME,
    SCALER_FILENAME
)

__all__ = [
    # Classes
    'ConfigValidator',
    'DataLoader',
    'IsolationForestModel',
    'MetricsCalculator',
    'TrainingPipeline',
    
    
    # Configurations
    'DATA_DIR',
    'MODEL_DIR',
    'METRICS_DIR',
    'ISOLATION_FOREST_PARAMS',
    'TRAIN_FILENAME',
    'TEST_FILENAME',
    'MODEL_FILENAME',
    'METRICS_FILENAME',
    'SCALER_FILENAME',
]
