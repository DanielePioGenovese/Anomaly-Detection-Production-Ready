import logging
import os
import joblib
import pandas as pd
import mlflow
import mlflow.sklearn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from feast import FeatureStore
import uvicorn
import preprocessor # Module required for joblib deserialization
from config.config import IsolationForestConfig

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("InferenceService")

app = FastAPI(
    title="Washing Machine Anomaly Detection Inference API",
    description="Real-time anomaly detection using Isolation Forest and Feast Online Store (Redis)",
    version="1.0.0"
)

# Global State
model = None
preproc = None
store = None
FEATURE_VIEW_NAME = "machine_stream_features_v1"

class PredictionRequest(BaseModel):
    machine_id: int

class PredictionResponse(BaseModel):
    machine_id: int
    is_anomaly: int
    anomaly_score: float
    model_version: str = "v1"

@app.on_event("startup")
def startup_event():
    """Load model artifacts and initialize Feast connection on startup."""
    global model, preproc, store
    
    try:
        # 1. Load Preprocessor
        preproc_path = IsolationForestConfig.PREPROCESSOR_JOBLIB
        logger.info(f"Loading preprocessor from {preproc_path}...")
        if not preproc_path.exists():
            raise FileNotFoundError(f"Preprocessor artifact not found at {preproc_path}. Ensure training service has run.")
        
        preproc = joblib.load(preproc_path)
        logger.info(f"✓ Preprocessor loaded (Expects features: {preproc.feature_columns})")

        # 2. Load Model from MLflow Registry
        logger.info("Connecting to MLflow Tracking Server...")
        mlflow.set_tracking_uri("http://mlflow:5000")
        client = mlflow.tracking.MlflowClient()
        
        model_name = "AnomalyForest"
        logger.info(f"Fetching latest version of model '{model_name}' from MLflow...")
        
        try:
            # Get latest version (any stage)
            # Note: In a real scenario, you might filter by "Production" stage
            versions = client.get_latest_versions(model_name, stages=["None", "Staging", "Production"])
            
            if not versions:
                raise RuntimeError(f"No registered model found for '{model_name}'")
                
            # Sort by version number desc just in case
            versions.sort(key=lambda x: int(x.version), reverse=True)
            latest_version = versions[0].version
            
            model_uri = f"models:/{model_name}/{latest_version}"
            logger.info(f"Loading model from {model_uri}...")
            
            model = mlflow.sklearn.load_model(model_uri)
            logger.info(f"✓ Model loaded successfully (Version {latest_version})")
            
        except Exception as e:
            logger.error(f"Failed to load model from MLflow: {e}")
            logger.warning("Falling back to local file system model if available...")
            model_path = IsolationForestConfig.MODEL_PATH
            if model_path.exists():
                model = joblib.load(model_path)
                logger.info("✓ Fallback: Local model loaded successfully")
            else:
                 raise e

        # 3. Initialize Feast
        repo_path = os.getenv("FEAST_REPO_PATH", "/inference_service")
        logger.info(f"Initializing Feast Feature Store from {repo_path}...")
        store = FeatureStore(repo_path=repo_path)
        logger.info("✓ Feast Feature Store connection established")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        # Build should fail if artifacts are missing? Or loop?
        # We raise error to restart container
        raise e

@app.post("/predict", response_model=PredictionResponse)
def predict_anomaly(request: PredictionRequest):
    """
    Predict anomaly status for a given machine_id based on real-time features from Redis.
    """
    if not store or not model or not preproc:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    machine_id = request.machine_id
    
    # 1. Construct Feature Vector Request
    # We ask Feast for all features required by the preprocessor
    features_to_fetch = [f"{FEATURE_VIEW_NAME}:{col}" for col in preproc.feature_columns]
    
    try:
        logger.debug(f"Fetching online features for machine_id={machine_id}")
        
        response = store.get_online_features(
            features=features_to_fetch,
            entity_rows=[{"Machine_ID": machine_id}]
        )
        
        data_dict = response.to_dict()
        
        # Determine if we got valid data.
        # Feast returns None for missing features.
        # We need to check if primary features are present.
        # Let's check a critical feature like 'Current_L1' or 'timestamp'
        # Or simply check if any value is None in the retrieved dict (excluding metadata)
        
        df = pd.DataFrame(data_dict)
        
        # Reorder columns to match preprocessor expectation
        # (Though sklearn scaler usually handles column order if dataframe is passed, 
        #  but safeguard is better if we passed numpy array. here we pass df.)
        # Ideally preprocessor handles it.
        
        # The DataFrame must contain exactly the columns preprocessor expects
        # But Feast response might contain extra columns (like machine_id) 
        # or columns in different order.
        
        # Filter/Reorder
        try:
            df_features = df[preproc.feature_columns]
        except KeyError as e:
             logger.error(f"Feature Store returned incomplete data. Missing: {e}")
             raise HTTPException(status_code=500, detail=f"Feature/Model Mismatch: {e}")

        # Check for NaNs (Feast returns None if key not in Redis)
        if df_features.isnull().values.any():
            # If all are null, machine likely inactive or not transmitting
            if df_features.isnull().all().all():
                logger.warning(f"No features found for machine_id={machine_id}")
                raise HTTPException(status_code=404, detail="Machine not found or offline")
            else:
                # Partial data? Fill with 0 or fail? 
                # For anomaly detection, 0 might be dangerous (viewed as normal or anomaly depending).
                # Let's fail for now to be safe.
                logger.warning(f"Partial features found for machine_id={machine_id} (some are None)")
                raise HTTPException(status_code=422, detail="Incomplete feature data available")

    except Exception as e:
        logger.error(f"Error fetching features: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # 2. Preprocess
    try:
        X_scaled = preproc.transform(df_features)
    except Exception as e:
        logger.error(f"Preprocessing error: {e}")
        raise HTTPException(status_code=500, detail=f"Preprocessing Error: {str(e)}")

    # 3. Predict
    # Isolation Forest: -1 (Anomaly), 1 (Normal)
    try:
        prediction_code = model.predict(X_scaled)[0]
        score = model.decision_function(X_scaled)[0]
        
        is_anomaly = 1 if prediction_code == -1 else 0
        
        logger.info(f"Prediction for Machine {machine_id}: Is_Anomaly={is_anomaly}, Score={score:.4f}")
        
        return {
            "machine_id": machine_id,
            "is_anomaly": is_anomaly,
            "anomaly_score": float(score)
        }
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=f"Model Prediction Error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
