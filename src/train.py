# src/train.py
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from pathlib import Path
import logging
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ModelTraining")

def train_isolation_forest():
    """
    Model training function.
    Loads historical data, trains the Isolation Forest, and saves the model.
    """
    logger.info("Starting training procedure...")

    # 1. Paths
    train_data_path = Config.HISTORICAL_DIR / "train_set.parquet"
    models_dir = Config.ROOT_DIR / "models"
    models_dir.mkdir(exist_ok=True)
    model_save_path = models_dir / "isolation_forest.pkl"

    # 2. Check if data exists
    if not train_data_path.exists():
        logger.error(f"Training file not found: {train_data_path}")
        raise FileNotFoundError(f"Training data not found at {train_data_path}")

    # 3. Load Data
    logger.info(f"Loading data from {train_data_path}...")
    df = pd.read_parquet(train_data_path)
    
    # Select only useful numerical features for training
    # We assume the dataset is already cleaned by 'create_parquet_datasets.py'
    # Removing any non-numerical columns if present for safety
    features = df.select_dtypes(include=['float64', 'float32', 'int64', 'int32'])
    
    logger.info(f"Training model on {len(features)} rows and {features.shape[1]} columns...")

    # 4. Training
    # Isolation Forest: excellent for unsupervised anomaly detection
    model = IsolationForest(
        n_estimators=100, 
        contamination=0.05, # We expect around 5% anomalies
        random_state=42,
        n_jobs=-1 # Use all cores
    )
    
    model.fit(features)

    # 5. Save Model (Artifact)
    logger.info(f"Saving model to {model_save_path}...")
    joblib.dump(model, model_save_path)
    
    logger.info("Training completed successfully!")
    return str(model_save_path)

if __name__ == "__main__":
    train_isolation_forest()
