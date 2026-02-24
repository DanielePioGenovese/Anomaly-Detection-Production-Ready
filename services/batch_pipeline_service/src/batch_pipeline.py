from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

from config import Config
from data_engineering_service.data_engineering import FeatureEngineering


def run_batch_pipeline():
    """
    Main batch pipeline for washing machine feature engineering.
    
    Workflow:
    1. Load raw data from historical data lake
    2. Apply feature engineering transformations
    3. Clean and select required columns
    4. Remove duplicates and null values
    5. Write features to offline feature store (append mode)
    """
    
    # Validate configuration
    if not Config.validate():
        print("[!] Configuration validation failed. Aborting pipeline.")
        return

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .config("spark.sql.session.timeZone", Config.SPARK_TIMEZONE) \
        .getOrCreate()

    try:
        # 1. Load raw data from Data Lake
        print(f"[*] Loading raw data from {Config.HISTORICAL_DIR}")
        raw_df = spark.read.parquet(f"{Config.HISTORICAL_DIR}/*.parquet")
        print(f"[*] Loaded {raw_df.count()} rows")

        # 2. Apply Feature Engineering
        print("[*] Executing feature engineering...")
        engineer = FeatureEngineering(config_path=Config.FEATURE_CONFIG_PATH)
        enriched_df = engineer._apply_batch_features(raw_df)

        # 3. Clean schema and remove duplicates
        print("[*] Cleaning schema and removing duplicates...")
        final_batch_df = enriched_df.select(
            Config.MACHINE_ID_COL,
            F.date_trunc(
                Config.TIMESTAMP_GRAIN, 
                F.col(Config.TIMESTAMP_COL)
            ).alias(Config.OUTPUT_TIMESTAMP_COL),
            Config.TARGET_FEATURE_COL
        ).distinct()

        # 4. Quality checks: filter null values if configured
        if not Config.ALLOW_NULL_VALUES:
            print("[*] Filtering null values...")
            final_batch_df = final_batch_df.filter(
                F.col(Config.TARGET_FEATURE_COL).isNotNull()
            )
        
        row_count = final_batch_df.count()
        print(f"[*] Rows to write: {row_count}")

        # 5. Write to Feature Store (Append Mode)
        print(f"[*] Writing features to {Config.OUTPUT_PATH}")
        (
            final_batch_df
            .repartition(Config.OUTPUT_REPARTITION)
            .write
            .mode(Config.WRITE_MODE)
            .parquet(str(Config.OUTPUT_PATH))
        )

        print(f"[✓] Pipeline completed successfully. {row_count} rows appended.")
        print(f"[*] Sample output (first {Config.SHOW_SAMPLE_ROWS} rows):")
        final_batch_df.show(Config.SHOW_SAMPLE_ROWS)

    except FileNotFoundError as e:
        print(f"[!] File not found error: {e}")
    except Exception as e:
        print(f"[!] Error during pipeline execution: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_batch_pipeline()