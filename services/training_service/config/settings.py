import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_experiment_name: str = "isolation_forest_prod"
    mlflow_model_name: str = "if_anomaly_detector"
    entity_df_path: str = "data/entity_df.parquet"
    feast_repo_path: str = "./feature_repo"
    feature_service_name: str = ""
    event_timestamp_column: str = "timestamp"
    output_dir: str = "outputs"
    

    class TrainingConfig:
        test_size: float = 0.2
        contamination: float = 0.1
        if_n_estimators: int = 100
        random_state: int = 42

    training: TrainingConfig = TrainingConfig()

    class Config:
        env_file = ".env"