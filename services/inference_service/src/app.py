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
from config.config import Config

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
FEATURE_VIEW_NAME = "machine_streaming_features"

class PredictionRequest(BaseModel):
    machine_id: str

class PredictionResponse(BaseModel):
    machine_id: str
    is_anomaly: int
    anomaly_score: float
    model_version: str = "v1"

@app.on_event("startup")
def startup_event():
    """Load model artifacts and initialize Feast connection on startup."""
    global model, store
    
    try:

        # 2. Load Model from MLflow Registry
        logger.info("Connecting to MLflow Tracking Server...")
        mlflow.set_tracking_uri("http://mlflow:5000")
        client = mlflow.tracking.MlflowClient()
        
        model_name = "if_anomaly_detector"
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
            model_path = Config.MODEL_PATH
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
    if not store or not model:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    machine_id = request.machine_id
    
    # 0. Convert 'M_0010' to integer '10' for Feast Entity matching
    # Feast expects INT64 for Machine_ID as defined in entity.py
    try:
        if machine_id.startswith("M_"):
            numeric_id = int(machine_id.replace("M_", ""))
        else:
            numeric_id = int(machine_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid machine_id format: {machine_id}. Expected format like 'M_0010' or '10'")

    # 1. Fetch Features from Feast using FeatureService
    # We ask Feast for all available features for the machine using the predefined service
    try:
        logger.debug(f"Fetching online features via FeatureService for numeric_id={numeric_id}")
        
        # 'machine_anomaly_service_v1' was defined in features.py or feature_services.py
        # It aggregates both streaming and batch features properly
        response = store.get_online_features(
            features=store.get_feature_service("machine_anomaly_service_v1"),
            entity_rows=[{"Machine_ID": numeric_id}]
        )
        
        data_dict = response.to_dict()
        df = pd.DataFrame(data_dict)
        
        # 1.5 Map Feast columns to MLflow required features
        # model.feature_names_in_ holds the exact columns the Pipeline expects
        try:
            feature_columns = list(model.feature_names_in_)
            df_features = df[feature_columns]
        except AttributeError:
             logger.error("Model does not expose feature_names_in_")
             raise HTTPException(status_code=500, detail="Cannot determine required features from Model Pipeline")
        except KeyError as e:
             logger.error(f"Feature Store returned incomplete data for Model. Missing: {e}")
             raise HTTPException(status_code=500, detail=f"Feature/Model Mismatch: {e}")

        # Check for NaNs (Feast returns None if key not in Redis)
        if df_features.isnull().values.any():
            if df_features.isnull().all().all():
                logger.warning(f"No features found for machine_id={machine_id} (numeric {numeric_id})")
                raise HTTPException(status_code=404, detail="Machine not found or offline")
            else:
                missing_cols = df_features.columns[df_features.isnull().any()].tolist()
                logger.warning(f"Partial features found for machine_id={machine_id} (numeric {numeric_id}). Missing exactly: {missing_cols}")
                # Also log the full fetched dictionary for absolute clarity
                logger.debug(f"Full Feast payload received: {data_dict}")
                raise HTTPException(status_code=422, detail=f"Incomplete feature data available. Missing exactly: {missing_cols}")

    except Exception as e:
        logger.error(f"Error fetching features: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # 2. Predict directly using the Pipeline (which includes the internal preprocessor)
    try:
        prediction_code = model.predict(df_features)[0]
        score = model.decision_function(df_features)[0]
        
        # Isolation Forest: -1 (Anomaly), 1 (Normal)
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

