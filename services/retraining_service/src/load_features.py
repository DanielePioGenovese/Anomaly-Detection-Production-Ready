"""
src/load_features.py
====================
Loads training features from the Feast feature store via a point-in-time join.

Data flow:
    entity_df (parquet)          ← raw sensor records: entity IDs + event_timestamp
         │
         ▼
    Feast.get_historical_features()   ← point-in-time join against Redis / parquet offline store
         │
         ▼
    pd.DataFrame                 ← full feature set ready for training
"""

import logging
import time

import pandas as pd
from feast import FeatureStore

logger = logging.getLogger(__name__)


class FeatureLoader:
    """
    Fetches historical features from Feast for offline (re)training.

    Feast performs a point-in-time join between the entity DataFrame
    (which contains entity IDs and timestamps) and the offline feature
    store (parquet files on disk), so that each row receives the feature
    values that were valid at the moment of its event_timestamp.
    This avoids any data leakage from future feature values.
    """

    def __init__(self, settings):
        """
        Args:
            settings: Settings object with Feast paths and column names.
        """
        self.s = settings

    def load(self) -> pd.DataFrame:
        """
        Execute the full feature loading process:
            1. Read entity_df from parquet
            2. Parse and normalise the event_timestamp column
            3. Call Feast for the point-in-time join
            4. Drop the timestamp (not a model feature)
            5. Return the clean feature DataFrame

        Returns:
            pd.DataFrame ready for model training (no timestamp, no entity IDs
            that are not features — those are dropped in retrain.py).

        Raises:
            FileNotFoundError: if the entity_df parquet path does not exist.
            KeyError:          if the timestamp column is missing.
        """
        t0 = time.time()
        ts_col = self.s.event_timestamp_column

        # ── Step 1: load entity_df ────────────────────────────────────────────
        logger.info(f"[FEAST] Loading entity_df from: {self.s.entity_df_path}")
        entity_df = pd.read_parquet(self.s.entity_df_path)
        logger.info(f"[FEAST] entity_df loaded — {len(entity_df):,} rows")

        if ts_col not in entity_df.columns:
            raise KeyError(
                f"[FEAST] Timestamp column '{ts_col}' not found in entity_df. "
                f"Available columns: {entity_df.columns.tolist()}"
            )

        # ── Step 2: normalise timestamp ───────────────────────────────────────
        # Feast requires timezone-aware UTC timestamps for point-in-time joins.
        logger.info("[FEAST] Parsing event_timestamp as UTC...")
        entity_df[ts_col] = pd.to_datetime(
            entity_df[ts_col], utc=True, errors="raise"
        ).dt.tz_convert(None)  # strip timezone after UTC normalisation (Feast offline store requirement)

        # ── Step 3: point-in-time join via Feast ──────────────────────────────
        logger.info(f"[FEAST] Connecting to feature store at: {self.s.feast_repo_path}")
        store = FeatureStore(repo_path=self.s.feast_repo_path)

        feature_service = store.get_feature_service(self.s.feature_service_name)
        logger.info(f"[FEAST] Using FeatureService: '{self.s.feature_service_name}'")

        logger.info("[FEAST] Running point-in-time join (this may take a moment)...")
        df = store.get_historical_features(
            entity_df=entity_df,
            features=feature_service,
        ).to_df()
        logger.info(f"[FEAST] Join completed — {len(df):,} rows, {df.shape[1]} columns")

        # ── Step 4: drop the timestamp ────────────────────────────────────────
        # The timestamp was needed for the join but is not a model feature.
        # Entity ID columns (Machine_ID, etc.) are dropped later in retrain.py
        # so they can still be used for logging/debugging at that stage.
        if ts_col in df.columns:
            df = df.drop(columns=[ts_col])

        elapsed = time.time() - t0
        logger.info(f"[FEAST] Feature loading completed in {elapsed:.2f}s")
        return df