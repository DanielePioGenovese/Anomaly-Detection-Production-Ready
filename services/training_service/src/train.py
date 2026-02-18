"""
Training pipeline for Isolation Forest anomaly detection model.

This module orchestrates the complete training workflow:
1. Load processed training data (from Historical Ingestion Service)
2. Fit a StandardScaler preprocessor on training data
3. Train Isolation Forest model on scaled features
4. Evaluate on test data
5. Save all artifacts (model + preprocessor + metrics)
"""
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
import os

# Configurazione base MLflow
mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

from config import config
from dataloader import DataLoader
from model import IsolationForestModel
from metrics import MetricsCalculator
from preprocessor import Preprocessor
from validation import ConfigValidator


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logging():
    """Configure logging for the training pipeline."""
    config.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(config.LOG_FILE)
        ]
    )


logger = logging.getLogger(__name__)


# ============================================================================
# TRAINING PIPELINE
# ============================================================================

class TrainingPipeline:
    """
    Complete pipeline for Isolation Forest model training.
    
    This pipeline:
    - Loads the preprocessor created by the Historical Ingestion Service
    - Loads raw training and test data
    - Applies preprocessing using the fitted preprocessor
    - Trains the Isolation Forest model
    - Evaluates and saves artifacts
    """
    
    def __init__(
        self,
        train_data_dir: Path = config.TRAIN_DATA_DIR,
        test_data_dir: Path = config.TEST_DATA_DIR,
        models_dir: Path = config.MODELS_DIR,
        metrics_dir: Path = config.METRICS_DIR
    ):
        """
        Initialize the training pipeline.
        
        Args:
            train_data_dir: Directory containing training data (normal operations)
            test_data_dir: Directory containing test data (with anomalies)
            models_dir: Directory for model artifacts
            metrics_dir: Directory for metrics output
        """
        self.train_data_dir = Path(train_data_dir)
        self.test_data_dir = Path(test_data_dir)
        self.models_dir = Path(models_dir)
        self.metrics_dir = Path(metrics_dir)
        
        # Initialize components — separate loaders for train and test
        self.train_loader = DataLoader(self.train_data_dir)
        self.test_loader = DataLoader(self.test_data_dir)
        self.preprocessor = None
        self.model = None
        self.metrics_calculator = MetricsCalculator()
        
        # Validator for the training data directory
        self.validator = ConfigValidator(
            data_dir=self.train_data_dir,
            model_dir=self.models_dir,
            metrics_dir=self.metrics_dir
        )
        
        logger.info("TrainingPipeline initialized")
        logger.info(f"  Train data: {self.train_data_dir}")
        logger.info(f"  Test data:  {self.test_data_dir}")
        logger.info(f"  Models:     {self.models_dir}")
        logger.info(f"  Metrics:    {self.metrics_dir}")

    def validate_configuration(self) -> bool:
        """
        Validate that all required paths and files exist.
        
        Returns:
            bool: True if validation successful, False otherwise
        """
        logger.info("=" * 80)
        logger.info("VALIDATING CONFIGURATION")
        logger.info("=" * 80)
        
        is_valid, error_msg = self.validator.validate_all()
        
        if not is_valid:
            logger.error(f"Configuration validation failed: {error_msg}")
            return False
        
        logger.info("✓ Configuration validation successful")
        return True

    def fit_preprocessor(self, df_train: pd.DataFrame) -> None:
        """
        Create and fit a StandardScaler preprocessor on training data.

        The fitted preprocessor is saved as an artifact so the streaming
        service can apply the exact same scaling at inference time.

        Args:
            df_train: Raw training DataFrame (including label/meta columns).
        """
        logger.info("Fitting preprocessor on training data...")

        self.preprocessor = Preprocessor()
        self.preprocessor.fit(df_train)

        logger.info(
            f"✓ Preprocessor fitted on {len(self.preprocessor.feature_columns)} features"
        )

    def load_and_preprocess_data(
        self,
        data_loader: DataLoader,
        file_pattern: str,
        dataset_name: str = "data"
    ) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
        """
        Load raw data and apply preprocessing.
        
        Args:
            data_loader: DataLoader instance pointing to the right directory
            file_pattern: Glob pattern for files to load
            dataset_name: Name of dataset (for logging)
        
        Returns:
            Tuple[pd.DataFrame, Optional[pd.Series]]: 
                - Preprocessed features (X)
                - Labels if available (y), None otherwise
        """
        logger.info(f"Loading {dataset_name} data...")
        
        # Load raw data
        df_raw = data_loader.load_data(file_pattern)
        logger.info(f"  Loaded {len(df_raw)} samples")
        
        # Extract labels if present
        y = None
        if 'Is_Anomaly' in df_raw.columns:
            y = df_raw['Is_Anomaly']
            logger.info(f"  Found labels: {y.sum()} anomalies ({y.mean():.2%})")
        else:
            logger.warning(f"  No labels found in {dataset_name}")
        
        # Apply preprocessing using the fitted preprocessor
        logger.info(f"Applying preprocessing to {dataset_name}...")

        if self.preprocessor is None:
            raise RuntimeError(
                "Preprocessor not fitted. Call fit_preprocessor() first."
            )

        X = self.preprocessor.preprocess_data(df_raw, fit=False)
        logger.info(f"✓ Preprocessing complete: {X.shape[1]} features")

        return X, y

    def train_model(
        self,
        X_train: pd.DataFrame,
        model_params: Optional[dict] = None
    ) -> None:
        """
        Train the Isolation Forest model.
        
        Args:
            X_train: Preprocessed training features
            model_params: Model parameters (uses config defaults if None)
        """
        logger.info("=" * 80)
        logger.info("TRAINING MODEL")
        logger.info("=" * 80)
        
        if model_params is None:
            model_params = config.ISOLATION_FOREST_PARAMS
        
        logger.info(f"Training on {X_train.shape[0]} samples, {X_train.shape[1]} features")
        logger.info(f"Model parameters: {model_params}")
        
        try:
            self.model = IsolationForestModel(**model_params)
            self.model.train(X_train)
            logger.info("✓ Model training complete")
            
        except Exception as e:
            logger.error(f"Error during training: {e}")
            raise

    def evaluate_model(
        self,
        X_test: pd.DataFrame,
        y_test: Optional[pd.Series] = None
    ) -> dict:
        """
        Evaluate the trained model on test data.
        
        Args:
            X_test: Preprocessed test features
            y_test: Test labels (optional)
        
        Returns:
            dict: Evaluation metrics
        """
        logger.info("=" * 80)
        logger.info("EVALUATING MODEL")
        logger.info("=" * 80)
        
        if self.model is None or not self.model.is_trained:
            raise RuntimeError("Model must be trained before evaluation")
        
        logger.info(f"Evaluating on {X_test.shape[0]} samples")
        
        # Get predictions and scores
        y_pred = self.model.predict(X_test)
        scores = self.model.score_samples(X_test)
        
        # Save predictions
        self._save_predictions(X_test, y_pred, scores, y_test)
        
        # Calculate metrics
        if y_test is not None:
            logger.info("Calculating supervised metrics...")
            metrics = self.metrics_calculator.calculate_supervised_metrics(
                y_true=y_test.values,
                y_pred=y_pred,
                scores=scores
            )
        else:
            logger.info("No labels available - calculating unsupervised metrics...")
            metrics = self.metrics_calculator.calculate_unsupervised_metrics(
                y_pred=y_pred,
                scores=scores
            )
        
        # Print summary
        self.metrics_calculator.print_summary()
        
        return metrics

    def _save_predictions(
        self,
        X_test: pd.DataFrame,
        y_pred: pd.Series,
        scores: pd.Series,
        y_test: Optional[pd.Series] = None
    ) -> None:
        """
        Save predictions to CSV file.
        
        Args:
            X_test: Test features
            y_pred: Predictions
            scores: Anomaly scores
            y_test: True labels (optional)
        """
        # Create predictions DataFrame
        df_pred = pd.DataFrame({
            'anomaly_prediction': y_pred,
            'anomaly_score': scores
        })
        
        # Add true labels if available
        if y_test is not None:
            df_pred['is_anomaly_true'] = y_test.values
        
        # Save to file
        predictions_path = self.metrics_dir / config.PREDICTIONS_FILE
        df_pred.to_csv(predictions_path, index=False)
        
        logger.info(f"✓ Predictions saved to: {predictions_path}")

    def save_artifacts(self) -> None:
        """Save all training artifacts (model, preprocessor, and metrics)."""
        logger.info("=" * 80)
        logger.info("SAVING ARTIFACTS")
        logger.info("=" * 80)

        if self.model is None or not self.model.is_trained:
            raise RuntimeError("Model must be trained before saving")

        # Save model
        model_path = self.models_dir / config.MODEL_ARTIFACT
        self.model.save_model(model_path)
        logger.info(f"✓ Model saved to: {model_path}")

        # Save preprocessor (needed by the streaming service for real-time inference)
        preprocessor_path = self.models_dir / config.PREPROCESSOR_ARTIFACT
        self.preprocessor.save(preprocessor_path)
        logger.info(f"✓ Preprocessor saved to: {preprocessor_path}")

        # Save metrics
        metrics_path = self.metrics_dir / config.METRICS_ARTIFACT
        self.metrics_calculator.save_metrics(metrics_path)
        logger.info(f"✓ Metrics saved to: {metrics_path}")

        logger.info("✓ All artifacts saved successfully")

    def run(
        self,
        train_pattern: str = config.TRAIN_FILE_PATTERN,
        test_pattern: str = config.TEST_FILE_PATTERN,
        model_params: Optional[dict] = None
    ) -> dict:
        """
        Execute the complete training pipeline.
        
        Args:
            train_pattern: Pattern for training files
            test_pattern: Pattern for test files
            model_params: Model parameters (optional)
        
        Returns:
            dict: Evaluation metrics
        """
        try:
            logger.info("=" * 80)
            logger.info("STARTING TRAINING PIPELINE")
            logger.info("=" * 80)
            
            # Step 1: Validate configuration
            if not self.validate_configuration():
                raise RuntimeError("Configuration validation failed")

            # Initialize MLflow
            try:
                mlflow.set_tracking_uri("http://mlflow:5000")
                mlflow.set_experiment("washing_machines_anomaly")
            except Exception as e:
                 logger.warning(f"Could not connect to MLflow: {e}")

            # Step 2: Load raw training data
            logger.info("=" * 80)
            logger.info("LOADING TRAINING DATA")
            logger.info("=" * 80)
            df_train = self.train_loader.load_data(train_pattern)
            logger.info(f"  Loaded {len(df_train)} training samples")

            # Start MLflow Run
            with mlflow.start_run() as run:
                # Log parameters
                if model_params:
                    mlflow.log_params(model_params)
                else:
                    mlflow.log_params(config.ISOLATION_FOREST_PARAMS)

                # Step 3: Fit preprocessor on training data
                self.fit_preprocessor(df_train)

                # Step 4: Apply preprocessing to training data
                X_train, _ = self.load_and_preprocess_data(
                    data_loader=self.train_loader,
                    file_pattern=train_pattern,
                    dataset_name="training"
                )

                # Step 5: Train model
                self.train_model(X_train, model_params)
                
                # Log model to MLflow (and register it)
                mlflow.sklearn.log_model(
                    self.model.model, 
                    "model",
                    registered_model_name="AnomalyForest"
                )

                # Step 6: Load and preprocess test data
                logger.info("=" * 80)
                logger.info("LOADING TEST DATA")
                logger.info("=" * 80)
                X_test, y_test = self.load_and_preprocess_data(
                    data_loader=self.test_loader,
                    file_pattern=test_pattern,
                    dataset_name="test"
                )

                # Step 7: Evaluate model
                metrics = self.evaluate_model(X_test, y_test)
                
                # Log metrics to MLflow
                mlflow.log_metrics(metrics)

                # Step 8: Save artifacts (model + preprocessor + metrics)
                self.save_artifacts()
                # Also log preprocessor as artifact to MLflow for completeness
                mlflow.log_artifact(str(self.models_dir / config.PREPROCESSOR_ARTIFACT), artifact_path="preprocessor")

            logger.info("=" * 80)
            logger.info("✓ TRAINING PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
            return metrics
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error("✗ TRAINING PIPELINE FAILED")
            logger.error("=" * 80)
            logger.error(f"Error: {e}", exc_info=True)
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point for the training script."""
    setup_logging()
    
    logger.info("Initializing training pipeline...")
    
    pipeline = TrainingPipeline()
    
    try:
        metrics = pipeline.run()
        logger.info(f"Training completed with metrics: {metrics}")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()