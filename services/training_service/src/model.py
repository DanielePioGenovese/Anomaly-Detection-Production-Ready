"""
Module for the Isolation Forest model
"""
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, Optional
from sklearn.ensemble import IsolationForest
import joblib

logger = logging.getLogger(__name__)


class IsolationForestModel:
    """Class to manage the Isolation Forest model"""
    
    def __init__(self, **params):
        """
        Initializes the Isolation Forest model
        
        Args:
            **params: Parameters for sklearn's IsolationForest
        """
        self.model = IsolationForest(**params)
        self.params = params
        self.is_trained = False
        logger.info(f"Isolation Forest model initialized with parameters: {params}")

    def train(self, X: pd.DataFrame, y: Optional[pd.Series] = None):
        """
        Trains the model
        
        Args:
            X: Training features (DataFrame)
            y: Labels (optional, Series for Isolation Forest)
        """
        logger.info(f"Starting training on {X.shape[0]} samples with {X.shape[1]} features")

        try:
            self.model.fit(X)
            self.is_trained = True
            logger.info("Training completed successfully")
        except Exception as e:
            logger.error(f"Error during training: {e}")
            raise

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predicts anomalies
        
        Args:
            X: Features for prediction (DataFrame)
        
        Returns:
            np.ndarray: Array of predictions (1 = normal, -1 = anomaly)
        """
        if not self.is_trained:
            raise RuntimeError("The model must be trained before making predictions")
        
        return self.model.predict(X)
    
    def score_samples(self, X: pd.DataFrame) -> np.ndarray:
        """
        Computes anomaly scores
        
        Args:
            X: Features
        
        Returns:
            np.ndarray: Anomaly scores (more negative = more anomalous)
        """
        if not self.is_trained:
            raise RuntimeError("The model must be trained before computing scores")
        
        return self.model.score_samples(X)
    
    def decision_function(self, X: pd.DataFrame) -> np.ndarray:
        """
        Computes the decision function
        
        Args:
            X: Features
        
        Returns:
            np.ndarray: Decision function values
        """
        if not self.is_trained:
            raise RuntimeError("The model must be trained before computing the decision function")
        
        return self.model.decision_function(X)
    
    def save_model(self, filepath: Path):
        """
        Saves the model to disk
        
        Args:
            filepath: Path to save the model
        """
        if not self.is_trained:
            logger.warning("Saving an untrained model")
        
        try:
            joblib.dump(self.model, filepath)
            logger.info(f"Model saved to {filepath}")
        except Exception as e:
            logger.error(f"Error saving the model: {e}")
            raise
    
    def load_model(self, filepath: Path):
        """
        Loads a model from disk
        
        Args:
            filepath: Path of the model to load
        """
        try:
            self.model = joblib.load(filepath)
            self.is_trained = True
            logger.info(f"Model loaded from {filepath}")
        except Exception as e:
            logger.error(f"Error loading the model: {e}")
            raise
    
    def get_params(self) -> Dict:
        """Returns the model parameters"""
        return self.model.get_params()