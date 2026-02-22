import pandas as pd
from feast import FeatureStore
import logging

logger = logging.getLogger(__name__)

class DataManager:
    """
    Responsible for loading data from Feast Feature Store.
    Isolates data loading logic from the rest of the pipeline.
    """
    def __init__(self, settings):
        """Constructor: saves settings and initializes Feast client"""
        self.s = settings
        self.store = FeatureStore(repo_path=self.s.feast_repo_path)     # FeatureStore: client to communicate with Feast repo_path: path to Feast metadata (feature_store.yaml)
        
    def load_data(self) -> pd.DataFrame:
        """
        Carica i dati elaborati da PySpark.
        Bypassa Feast (get_historical_features) per il training poiché i dati
        in /data/offline/machines_batch_features contengono già tutte le 
        feature temporali (rolling e batch) aggiunte dal Data Engineering service.
        Questo evita il blocco memoria di Dask durante il point-in-time join.
        """
        logger.info(f"[DATA] Loading historical PySpark features directly from {self.s.entity_df_path}")
        
        # Leggi l'intera directory Parquet elaborata precedentemente da Spark
        df = pd.read_parquet(self.s.entity_df_path)
        
        # Mantieni UTC coerentemente
        df[self.s.event_timestamp_column] = pd.to_datetime(
            df[self.s.event_timestamp_column], utc=True
        )
        
        logger.info(f"[DATA] Loaded {len(df)} rows ready for training")

        return df

"""    
        def prepare_features(self, df, drop_cols):
        
        Prepara features per il modello.
        Rimuove colonne non predittive (timestamp, ID, ecc.)
        
        Args:
            df: DataFrame completo
            drop_cols: Lista di colonne da escludere
        
        Returns:
            x: DataFrame con sole features
        
        x = df.drop(columns=[c for c in drop_cols if c in df.columns])
        logger.info(f"[DATA] Features prepared: {x.shape}")
        return x"""