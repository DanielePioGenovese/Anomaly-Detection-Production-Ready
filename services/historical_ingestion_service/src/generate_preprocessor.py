# generate_preprocessor.py
import logging
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col  
from pyspark.ml.functions import vector_to_array  
import os

from config import RAW_DATA_PATH, MODEL_REGISTRY_PATH, PROCESSED_DATA_PATH

sys.path.insert(0, str(Path(__file__).parent))

from dataloader import DataLoader
from hist_feature_engineering import SparkDataPreprocessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ArtifactGenerator")

def generate():
    # 1. Initialize Spark Session with timestamp handling configs
    spark = SparkSession.builder \
        .appName("FeatureEngineeringBatch") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .getOrCreate()
    
    logger.info("Spark Session initialized.")

    try:
        # 2. Load Data
        loader = DataLoader(spark, RAW_DATA_PATH)
        
        # Try Parquet, fall back to CSV if needed
        try:
            logger.info("Attempting to load Parquet files...")
            df = loader.load_data(file_pattern="*.parquet", file_format="parquet")
        except Exception as parquet_error:
            logger.warning(f"Parquet load failed: {parquet_error}")
            logger.info("Falling back to CSV...")
            try:
                df = loader.load_data(file_pattern="*.csv", file_format="csv")
            except Exception as csv_error:
                logger.error(f"CSV load also failed: {csv_error}")
                raise RuntimeError("Could not load data from either Parquet or CSV") from csv_error

        cols_to_exclude = ['timestamp', 'Machine_ID', 'Is_Anomaly', 'Anomaly_Type']
        
        # 3. Preprocess (Scaling)
        preprocessor = SparkDataPreprocessor(
            label_columns=cols_to_exclude,
            scaler_type='standard'
        )

        # This returns the DF with a new column "features" (Vector)
        df_transformed = preprocessor.fit_transform(df)
        
        # 4. Convert Vector to individual columns for Feast compatibility
        df_with_array = df_transformed.withColumn(
            "features_array", 
            vector_to_array(col("features"))
        )
        
        # Explode array into individual feature columns
        feature_cols = preprocessor.feature_cols
        for idx, col_name in enumerate(feature_cols):
            df_with_array = df_with_array.withColumn(
                f"scaled_{col_name}",
                col("features_array")[idx]
            )
        
        # Keep only what Feast needs
        columns_to_keep = (
            ['timestamp', 'Machine_ID', 'Is_Anomaly', 'Anomaly_Type'] +
            [f"scaled_{col_name}" for col_name in feature_cols]
        )
        df_final = df_with_array.select(columns_to_keep)

        # 5. Save the Model (The Scaler/Pipeline)
        model_output_path = MODEL_REGISTRY_PATH / "spark_preprocessor"
        os.makedirs(str(MODEL_REGISTRY_PATH), exist_ok=True)
        
        preprocessor.save_model(model_output_path)
        logger.info(f"✅ Pipeline Model saved to {model_output_path}")

        # 6. Save the Processed Data for Feast
        os.makedirs(str(PROCESSED_DATA_PATH.parent), exist_ok=True)
        
        df_final.write \
            .mode("overwrite") \
            .parquet(str(PROCESSED_DATA_PATH))
            
        logger.info(f"✅ Processed data saved to {PROCESSED_DATA_PATH}")

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    generate()