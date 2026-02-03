# src/train.py
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from pathlib import Path
import logging
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ModelTraining")

def train_isolation_forest():
    """
    Model training function with preprocessing.
    Saves both the preprocessor (Scaler/OHE) and the model.
    """
    logger.info("Starting training procedure with Preprocessing...")

    # 1. Paths
    train_data_path = Config.HISTORICAL_DIR / "train_set.parquet"
    models_dir = Config.ROOT_DIR / "models"
    models_dir.mkdir(exist_ok=True)
    
    model_save_path = models_dir / "isolation_forest.pkl"
    preprocessor_save_path = models_dir / "preprocessor.joblib"

    # 2. Load Data
    if not train_data_path.exists():
        logger.error(f"Training file not found: {train_data_path}")
        return

    df = pd.read_parquet(train_data_path)
    
    # 3. Define Preprocessing
    # Categorical columns for One-Hot Encoding
    categorical_cols = ['Cycle_Phase_ID']
    # Numerical columns for Scaling (excluding identifiers)
    numerical_cols = [col for col in df.select_dtypes(include=['float64', 'int64']).columns 
                      if col not in categorical_cols + Config.DROP_COLUMNS]

    logger.info(f"Features: {len(numerical_cols)} numerical, {len(categorical_cols)} categorical")

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numerical_cols),
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_cols)
        ]
    )

    # 4. Fit Preprocessor and Train Model
    X_train = preprocessor.fit_transform(df)
    
    logger.info(f"Training Isolation Forest on shape: {X_train.shape}")
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X_train)

    # 5. Save Artifacts
    joblib.dump(model, model_save_path)
    joblib.dump(preprocessor, preprocessor_save_path)
    
    logger.info(f"✅ Model saved to {model_save_path}")
    logger.info(f"✅ Preprocessor saved to {preprocessor_save_path}")

if __name__ == "__main__":
    train_isolation_forest()
