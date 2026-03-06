"""
Batch Feature Pipeline — Washing Machine Daily Vibration Ratio
==============================================================
Reads telemetry data, computes Daily_Vibration_PeakMean_Ratio
(max / mean of Vibration_mm_s) for every machine × calendar day,
writes the result to the Feast offline store, then materializes
into Redis (online store) in one single run.

Output schema (3 columns only):
  Machine_ID                     Int64
  timestamp                      Timestamp UTC  ← max(timestamp) in the window
  Daily_Vibration_PeakMean_Ratio Float32

Configuration is loaded from config.yaml (override with CONFIG_PATH env-var).
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import yaml
from feast import FeatureStore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ── Settings ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Settings:
    entitydf_dir:         str
    offline_dir:          str
    spark_app_name:       str
    spark_master:         str
    spark_extra_configs:  Dict[str, str]
    timestamp_column:     str
    write_mode:           str
    feast_repo_path:      str
    feast_feature_views:  List[str]


def load_settings(config_path: str = "config.yaml") -> Settings:
    resolved = os.getenv("CONFIG_PATH", config_path)
    if not Path(resolved).exists():
        raise FileNotFoundError(f"Config not found: {resolved}")

    with open(resolved) as fh:
        cfg = yaml.safe_load(fh)

    return Settings(
        entitydf_dir        = cfg["paths"]["entitydf_dir"],
        offline_dir         = cfg["paths"]["offline_store_dir"],
        spark_app_name      = cfg["spark"].get("app_name",  "batch-feature-pipeline-washing-machines"),
        spark_master        = cfg["spark"].get("master",    "local[*]"),
        spark_extra_configs = cfg["spark"].get("configs",   {}),
        timestamp_column    = cfg["schema"].get("timestamp_column", "timestamp"),
        write_mode          = cfg["processing"].get("write_mode",   "append"),
        feast_repo_path     = cfg["feast"]["repo_path"],
        feast_feature_views = cfg["feast"]["feature_views"],
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────

def read_inputs(spark: SparkSession, path: str, timestamp_col: str) -> DataFrame:
    """Read telemetry; keep only the 3 columns we need."""
    logger.info(f"Reading parquet from: {path}")
    df = (
        spark.read.parquet(path)
             .select("Machine_ID", timestamp_col, "Vibration_mm_s")
             .withColumn(timestamp_col, F.col(timestamp_col).cast("timestamp"))
    )
    logger.info(f"  → {df.count()} rows loaded")
    return df


def compute_daily_features(df: DataFrame, timestamp_col: str) -> DataFrame:
    """
    Group by (Machine_ID, calendar day) and compute:
      Daily_Vibration_PeakMean_Ratio = max(Vibration_mm_s) / mean(Vibration_mm_s)
      timestamp = max(timestamp) in that day window   ← used as Feast event_timestamp
    Returns one row per machine × day.
    """
    return (
        df.groupBy("Machine_ID", F.window(timestamp_col, "1 day"))
          .agg(
              (F.max("Vibration_mm_s") / F.mean("Vibration_mm_s"))
                  .cast("float")
                  .alias("Daily_Vibration_PeakMean_Ratio"),
              F.max(timestamp_col).alias(timestamp_col),
          )
          .select("Machine_ID", timestamp_col, "Daily_Vibration_PeakMean_Ratio")
    )


def write_single_file(df: DataFrame, out_path: Path, write_mode: str) -> None:
    """Write all rows into a single parquet file (coalesce to 1 partition)."""
    out_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing to: {out_path}  [mode={write_mode}]")
    df.coalesce(1).write.mode(write_mode).parquet(str(out_path))
    logger.info("  → Written successfully")


def materialize(s: Settings) -> None:
    """Push offline store rows into Redis via Feast materialize_incremental."""
    end_date = datetime.now(tz=timezone.utc)
    logger.info("Feast repo      : %s", s.feast_repo_path)
    logger.info("Feature views   : %s", s.feast_feature_views)
    logger.info("Materializing up to: %s", end_date.isoformat())

    store = FeatureStore(repo_path=s.feast_repo_path)
    store.materialize_incremental(
        end_date=end_date,
        feature_views=s.feast_feature_views,
    )
    logger.info("✓ Materialization complete — Daily_Vibration_PeakMean_Ratio is now in Redis")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    s = load_settings()
    logger.info(f"entitydf_dir : {s.entitydf_dir}")
    logger.info(f"offline_dir  : {s.offline_dir}")
    logger.info(f"write_mode   : {s.write_mode}")

    builder = SparkSession.builder.appName(s.spark_app_name).master(s.spark_master)
    for key, value in s.spark_extra_configs.items():
        builder = builder.config(key, str(value))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = read_inputs(spark, s.entitydf_dir, s.timestamp_column)
        batch_features = compute_daily_features(df, s.timestamp_column)
        batch_features.show(10, truncate=False)
        write_single_file(batch_features, Path(s.offline_dir), s.write_mode)
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        raise
    finally:
        spark.stop()

    materialize(s)

if __name__ == "__main__":
    main()