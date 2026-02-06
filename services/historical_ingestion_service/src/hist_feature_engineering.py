import logging
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sqrt, abs as spark_abs, when, lit, variance, lag
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

class SparkDataPreprocessor:
    def __init__(self, enable_expensive_features: bool = True):
        """
        Args:
            enable_expensive_features: If True, includes window-based features (slower)
        """
        self.enable_expensive_features = enable_expensive_features
        self.derived_feature_cols: List[str] = []

    def _engineer_features(self, df: DataFrame) -> DataFrame:
        """
        Create derived features based on the calculation logic.
        Optimized version with optional expensive operations.
        """
        logger.info("Engineering derived features...")
        logger.info(f"Expensive features (windows): {'ENABLED' if self.enable_expensive_features else 'DISABLED'}")
        
        # 1. Current_Avg - average across three phases
        df = df.withColumn(
            "Current_Avg",
            (col("Current_L1") + col("Current_L2") + col("Current_L3")) / 3.0
        )
        
        # 2. Apparent_Power = sqrt(3) * V * I
        df = df.withColumn(
            "Apparent_Power",
            sqrt(lit(3)) * col("Voltage_L_L") * col("Current_Avg")
        )
        
        # 3. Active_Power = sqrt(3) * V * I * PF (assume PF=0.95 initially)
        df = df.withColumn(
            "Active_Power",
            sqrt(lit(3)) * col("Voltage_L_L") * col("Current_Avg") * lit(0.95)
        )
        
        # 4. Reactive_Power (simplified)
        df = df.withColumn(
            "Reactive_Power",
            col("Voltage_L_L") * col("Current_Avg")
        )
        
        # 5. Power_Factor = P / S
        df = df.withColumn(
            "Power_Factor",
            when(col("Apparent_Power") > 0, 
                 col("Active_Power") / col("Apparent_Power"))
            .otherwise(lit(0.0))
        )
        
        # 6. THD_Current (simplified estimation)
        df = df.withColumn(
            "THD_Current",
            col("THD_Voltage") * lit(1.2)
        )
        
        # 7. Current_P_to_P (Peak-to-Peak)
        from pyspark.sql.functions import greatest, least
        
        df = df.withColumn(
            "Current_P_to_P",
            greatest(col("Current_L1"), col("Current_L2"), col("Current_L3")) -
            least(col("Current_L1"), col("Current_L2"), col("Current_L3"))
        )
        
        # 8. Max_Current_Instance
        df = df.withColumn(
            "Max_Current_Instance",
            greatest(col("Current_L1"), col("Current_L2"), col("Current_L3"))
        )
        
        # 9. Inrush_Peak (only during warmup)
        df = df.withColumn(
            "Inrush_Peak",
            when(col("Cycle_Phase_ID") == 1, col("Max_Current_Instance"))
            .otherwise(lit(0.0))
        )
        
        # 10. Phase_Imbalance
        df = df.withColumn(
            "Phase_Imbalance",
            (
                (spark_abs(col("Current_L1") - col("Current_Avg")) +
                 spark_abs(col("Current_L2") - col("Current_Avg")) +
                 spark_abs(col("Current_L3") - col("Current_Avg"))) / 3.0
            ) / when(col("Current_Avg") > 0, col("Current_Avg")).otherwise(lit(1.0)) * 100.0
        )
        
        # 11. Energy_per_Cycle (simplified - assumes 1 second intervals)
        df = df.withColumn(
            "Energy_per_Cycle",
            col("Active_Power") * lit(1.0)
        )
        
        # Basic derived features (always included)
        self.derived_feature_cols = [
            "Current_Avg",
            "Apparent_Power",
            "Active_Power",
            "Reactive_Power",
            "Power_Factor",
            "THD_Current",
            "Current_P_to_P",
            "Max_Current_Instance",
            "Inrush_Peak",
            "Phase_Imbalance",
            "Energy_per_Cycle"
        ]
        
        # EXPENSIVE OPERATIONS (window-based) - only if enabled
        if self.enable_expensive_features:
            logger.info("Computing expensive window-based features...")
            
            # Reduce shuffle partitions for better performance
            spark_session = df.sparkSession
            original_partitions = spark_session.conf.get("spark.sql.shuffle.partitions")
            spark_session.conf.set("spark.sql.shuffle.partitions", "10")
            
            try:
                # Data frequency: 30 seconds per reading
                # 10 minutes batch = 20 readings (600 seconds / 30 seconds)
                
                # 12. Power_Var_10min - variance over last 10 minutes (20 rows at 30-sec intervals)
                # Using row-based window: -19 to 0 = last 20 readings (current + 19 previous)
                window_10min = Window.partitionBy("Machine_ID").orderBy("timestamp").rowsBetween(-19, 0)
                
                df = df.withColumn(
                    "Power_Var_10min",
                    variance(col("Active_Power")).over(window_10min)
                )
                
                # 13. Current_Trend_5min - change in current over 5 minutes
                df = df.withColumn(
                    "Current_5min_Ago",
                    lag(col("Current_Avg"), 10).over(window_5min)
                )
                
                df = df.withColumn(
                    "Current_Trend_5min",
                    when(col("Current_5min_Ago").isNotNull(),
                         col("Current_Avg") - col("Current_5min_Ago"))
                    .otherwise(lit(0.0))
                ).drop("Current_5min_Ago")
                
                # Fill nulls with 0 for first rows
                df = df.fillna(0.0, subset=["Power_Var_10min", "Power_Avg_5min", "Current_Trend_5min"])
                
                # 14. Inrush_Duration (simplified - time in warmup phase)
                df = df.withColumn(
                    "Inrush_Duration",
                    when(col("Cycle_Phase_ID") == 1, lit(100.0))
                    .otherwise(lit(0.0))
                )
                
                self.derived_feature_cols.extend([
                    "Power_Var_10min", 
                    "Power_Avg_5min", 
                    "Current_Trend_5min",
                    "Inrush_Duration"
                ])
                
            finally:
                # Restore original partition setting
                spark_session.conf.set("spark.sql.shuffle.partitions", original_partitions)
        
        logger.info(f"Created {len(self.derived_feature_cols)} derived features")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Engineers features and returns the DataFrame with all original columns + calculated features.
        NO SCALING OR ENCODING - just raw calculated values.
        """
        logger.info("Starting feature engineering (no scaling)...")
        
        # Cache input if it's large to avoid recomputation
        row_count = df.count()
        logger.info(f"Input data: {row_count} rows")
        
        if row_count > 1000:
            logger.info("Caching input DataFrame for better performance...")
            df.cache()

        # Engineer derived features
        df_transformed = self._engineer_features(df)

        # Unpersist if we cached
        if row_count > 1000:
            df.unpersist()

        return df_transformed