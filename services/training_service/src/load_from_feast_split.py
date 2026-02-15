import pandas as pd
from feast import FeatureStore
import logging

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self, settings):
        self.s = settings
        self.store = FeatureStore(repo_path=self.s.feast_repo_path)

    def load_data(self) -> pd.DataFrame:
        """
        Carica dati da Feast Feature Store.
        Ritorna il dataset completo ordinato temporalmente.
        """
        logger.info("[DATA] Loading entity df and features from Feast")
        entity_df = pd.read_parquet(self.s.entity_df_path)
        
        # Converti timestamp a UTC
        entity_df[self.s.event_timestamp_column] = pd.to_datetime(
            entity_df[self.s.event_timestamp_column], utc=True
        )

        feature_service = self.store.get_feature_service(self.s.feature_service_name)
        df = self.store.get_historical_features(
            entity_df=entity_df,
            features=feature_service
        ).to_df()
        
        # Mantieni UTC coerentemente
        df[self.s.event_timestamp_column] = pd.to_datetime(
            df[self.s.event_timestamp_column], utc=True
        )
        
        logger.info(f"[DATA] Loaded {len(df)} rows from Feast")
        return df

    def prepare_features(self, df, drop_cols):
        """
        Prepara features per il modello.
        Rimuove colonne non predittive (timestamp, ID, ecc.)
        
        Args:
            df: DataFrame completo
            drop_cols: Lista di colonne da escludere
        
        Returns:
            x: DataFrame con sole features
        """
        x = df.drop(columns=[c for c in drop_cols if c in df.columns])
        logger.info(f"[DATA] Features prepared: {x.shape}")
        return x