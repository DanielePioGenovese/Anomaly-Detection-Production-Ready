"""
Script to retrieve Historical and Online Features from the Feast Feature Store  
Entity: Machine (Machine_ID)  
Calculated features: Placeholder (to be implemented)
"""

import pandas
from feast import FeatureStore
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger= logging.getLogger(__name__)


# Configure and connect to the Feast Feature Store
class FeatureClient:
    """ Client to interact with the Feast Feature Store for retrieving features."""

def __init__(self, repo_path: str = ""): # Specify the path to the feature_store.yaml
    """
    Initializes the FeatureClient by connecting to the Feast Feature Store using the provided configuration file path.

    Args:
        repo_path (str): Path to the feature_store.yaml configuration file. """

    try:
        self.store = FeatureStore(repo_path = repo_path)
        logger.info("Successfully connected to the Feast Feature Store.")
        self._log_registry_info() # Log the contents of the registry for debugging purposes registry.db
    except Exception as e:
        logger.error(f"Failed to initialize FeatureClient: {e}")
        raise

def _log_registry_info(self):
    """ Logs the contents of the Feast registry for debugging purposes. """
    try:
        feature_views = self.store.list_feature_views()
        logger.info(f"Feature Views in the registry: {[fv.name for fv in feature_views]}")
    except Exception as e:
        logger.warning(f"Could not retrieve feature views from the registry: {e}")

# Historical features (Training/ Batch Job)
def get_historical_features(self, 
                            entity_rows: list,
                            feature_view_name: str = ""):  # PERSONALIZE with your feature view name
        """
        Retrieve historical features for training
        
        Args:
            entity_rows: List of dicts with Machine_ID and event_timestamp
                Example:
                [
                    {"Machine_ID": "MACHINE_001", "event_timestamp": datetime(2024,1,1,10,0,0)},
                    {"Machine_ID": "MACHINE_002", "event_timestamp": datetime(2024,1,5,15,30,0)},
                ]
            
            feature_view_name: Name of the feature view
                      (CUSTOMIZE with your actual name)
        
        Returns:
            pd.DataFrame with historical features 
        """
        
        logger.info(f"Retrieve historical features for {len(entity_rows)} rows")
        
        try:
            # Build feature references
            # CUSTOMIZE: use your feature view name and actual features
            feature_refs = [
                f"{feature_view_name}:feature_1",  # PLACEHOLDER - sostituisci con nomi reali
                f"{feature_view_name}:feature_2",  # PLACEHOLDER
                f"{feature_view_name}:feature_3",  # PLACEHOLDER
            ]
            
            logger.info(f"Requested features: {feature_refs}")
            
            # Retrieve historical features from Feast
            historical_features = self.store.get_historical_features(
                entity_rows=entity_rows,
                features=feature_refs,
                full_feature_names=True
            )
            
            df_historical = historical_features.to_pandas()
            logger.info(f"Retrieved {len(df_historical)} rows with {len(df_historical.columns)} columns")
            
            return df_historical
            
        except Exception as e:
            logger.error(f"Error retrieving historical features: {e}")
            raise

# Online features (Real-time/ Inference)
def get_online_features(self,
                        entity_rows: list,
                        feature_view_name: str = ""):  # PERSONALIZE with your feature view name
    """
        Retrieve online features for real-time inference
        
        Args:
            entity_rows: List of dicts with Machine_ID
                Example:
                [
                    {"Machine_ID": "MACHINE_001"},
                    {"Machine_ID": "MACHINE_002"},
                ]
            feature_view_name: Name of the feature view
                        (CUSTOMIZE with your actual name) 
        Returns:
            pd.DataFrame with online features or None if retrieval fails
    """

    logger.info(f"Retrieve online features...")

    try:
        # Feature references
            feature_refs = [
                f"{feature_view_name}:feature_1",  # PERSONALIZZA
                f"{feature_view_name}:feature_2",  # PERSONALIZZA
                f"{feature_view_name}:feature_3",  # PERSONALIZZA
            ]
            
            # Recupera da Feast
            online_features = self.store.get_online_features(
                features=feature_refs,
                entity_rows=entity_rows
            )
            
            df_online = online_features.to_pandas()
            logger.info(f"Retrieved online features for {len(df_online)} rows with {len(df_online.columns)} columns")

            return df_online
        
    except Exception as e:
         logger.error(f"Error retrieving online features: {e}")
         return None
    
    def add_computed_features(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Add calculated/derived features
        
        Args:
            df: DataFrame with base features
        
        Returns:
            DataFrame with computed features added
        """
    
        df = df.copy()
    
        # PLACEHOLDER: Add your computed features here
        # Example:
        # if "machine_features:temperature" in df.columns:
        #     df["computed:temp_normalized"] = (df["machine_features:temperature"] - 20) / 10
    
        return df
