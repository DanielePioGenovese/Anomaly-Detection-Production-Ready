"""
Historical Data Ingestion Service with Rolling Window Features
Processes industrial washer data with configurable rolling aggregations
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalIngestionService:
    """Service for processing historical industrial washer data with rolling features"""
    
    def __init__(self, config_path: str):
        """
        Initialize the ingestion service
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration file"""
        logger.info(f"Loading configuration from {config_path}")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        logger.info("Creating Spark session")
        
        builder = SparkSession.builder.appName("HistoricalIngestion_RollingFeatures")
        
        # Apply Spark configurations from config
        if 'spark_config' in self.config:
            for key, value in self.config['spark_config'].items():
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def _parse_window_duration(self, duration_str: str) -> int:
        """
        Parse window duration string to seconds
        
        Args:
            duration_str: Duration string like '10 minutes', '1 hour', '30 seconds'
            
        Returns:
            Duration in seconds
        """
        parts = duration_str.lower().split()
        if len(parts) != 2:
            raise ValueError(f"Invalid duration format: {duration_str}")
        
        value = int(parts[0])
        unit = parts[1]
        
        multipliers = {
            'second': 1, 'seconds': 1,
            'minute': 60, 'minutes': 60,
            'hour': 3600, 'hours': 3600,
            'day': 86400, 'days': 86400
        }
        
        if unit not in multipliers:
            raise ValueError(f"Unknown time unit: {unit}")
        
        return value * multipliers[unit]
    
    def _read_dataset(self, dataset_config: Dict) -> Any:
        """Read dataset based on configuration"""
        logger.info(f"Reading dataset: {dataset_config['name']}")
        
        input_path = dataset_config['input_path']
        file_format = dataset_config.get('file_format', 'parquet')
        
        if file_format == 'parquet':
            df = self.spark.read.parquet(input_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        logger.info(f"Read {df.count()} rows from {input_path}")
        return df
    
    def _validate_data_quality(self, df: Any, dataset_config: Dict) -> Any:
        """Perform data quality checks"""
        timestamp_col = self.config['schema']['timestamp_column']
        
        if self.config['data_quality'].get('check_null_timestamps', True):
            null_count = df.filter(F.col(timestamp_col).isNull()).count()
            if null_count > 0:
                logger.warning(f"Found {null_count} null timestamps - filtering out")
                df = df.filter(F.col(timestamp_col).isNotNull())
        
        if self.config['data_quality'].get('check_duplicate_timestamps', True):
            original_count = df.count()
            df = df.dropDuplicates([timestamp_col, 'Machine_ID'])
            dropped = original_count - df.count()
            if dropped > 0:
                logger.warning(f"Removed {dropped} duplicate timestamp-machine combinations")
        
        return df
    
    def _validate_rolling_feature(self, df: Any, feature_name: str, source_column: str, aggregation: str):
        """Validate rolling window feature calculations"""
        logger.info(f"  Validating rolling feature: {feature_name}")
        
        # Validate based on aggregation type
        if aggregation == 'max':
            # Rolling max should always be >= source value
            invalid_count = df.filter(F.col(feature_name) < F.col(source_column)).count()
            if invalid_count > 0:
                logger.error(f"  ❌ VALIDATION FAILED: Found {invalid_count} rows where {feature_name} < {source_column}")
            else:
                logger.info(f"  ✓ Validation passed: All rolling max values >= source values")
                
        else:
            logger.error(f"❌ VALIDATION FAILED, aggregation must be 'max'")
        
        # Check for unexpected null values
        null_count = df.filter(F.col(feature_name).isNull() & F.col(source_column).isNotNull()).count()
        if null_count > 0:
            logger.warning(f"  ⚠ Found {null_count} null values in {feature_name} where source is not null")
        
        # Show sample statistics
        stats = df.select(
            F.max(source_column).alias('source_max'),
            F.max(feature_name).alias('rolling_max'),
        ).collect()[0]
        
        logger.info(f"  Statistics: Source [{stats['source_max']:.2f}], "
                   f"Rolling [{stats['rolling_max']:.2f}]")
    
    def _apply_rolling_features(self, df: Any) -> Any:
        """Apply rolling window features based on configuration"""

        logger.info("Applying rolling window features")
        
        timestamp_col = self.config['schema']['timestamp_column']
        partition_cols = self.config['schema']['partition_columns']
        
        # Ensure timestamp is in correct format
        df = df.withColumn(timestamp_col, F.col(timestamp_col).cast(TimestampType()))
        
        # Sort by timestamp and partition columns for correct ordering
        logger.info(f"Sorting data by {timestamp_col}, {partition_cols} for window calculations")
        df = df.orderBy(timestamp_col, *partition_cols)
        
        # Process each enabled rolling feature
        # You can add any rolling feature you need in config file 
        for feature_config in self.config['rolling_features']: 
            if not feature_config.get('enabled', False):
                logger.info(f"Skipping disabled feature: {feature_config['feature_name']}")
                continue
            
            feature_name = feature_config['feature_name']
            source_column = feature_config['source_column']
            aggregation = feature_config['aggregation']
            window_duration_str = feature_config['window_duration']
            
            logger.info(f"Creating feature: {feature_name} ({feature_config['description']})")
            logger.info(f"  Source: {source_column}, Aggregation: {aggregation}, Window: {window_duration_str}")
            
            # Parse window duration
            window_duration_seconds = self._parse_window_duration(window_duration_str)
            logger.info(f"  Window duration: {window_duration_seconds} seconds")
            
            
            # CRITICAL FIX: Create time-based window specification WITH partitionBy
            # This ensures rolling windows are calculated SEPARATELY for each machine
            # Without partitionBy, data from all machines would be mixed together!
            window_spec = (
                Window
                .partitionBy(*partition_cols)  # SEPARATE WINDOWS PER MACHINE
                .orderBy(F.col(timestamp_col).cast("long"))
                .rangeBetween(-window_duration_seconds, 0)
            )
            
            # Apply aggregation function
            # Add other aggregations inside the config file (if you need them, ex: STD, MEAN etc...)
            if aggregation == 'max':
                df = df.withColumn(feature_name, F.max(source_column).over(window_spec))
            else:
                logger.warning(f"Unknown aggregation: {aggregation} for feature {feature_name}")
                continue
            
            # Validate the rolling window calculation
            if self.config.get('data_quality', {}).get('validate_rolling_windows', True):
                self._validate_rolling_feature(df, feature_name, source_column, aggregation)
            
            logger.info(f"✓ Created feature: {feature_name}")
        
        # CRITICAL FIX: Re-sort by timestamp and machine_ID to match required output format
        # Output format: time 00:00:00 Machine 1, time 00:00:00 Machine 2, etc.
        logger.info(f"Final sort: Re-ordering data by {timestamp_col}, {partition_cols} to maintain required order")
        df = df.orderBy(timestamp_col, *partition_cols)
        
        return df
    
    def _write_dataset(self, df: Any, dataset_config: Dict):
        """Write processed dataset WITHOUT Machine_ID partitioning (flat structure)"""
        output_path = dataset_config['output_path']
        write_mode = self.config['processing'].get('write_mode', 'overwrite')
        timestamp_col = self.config['schema']['timestamp_column']
        partition_cols = self.config['schema']['partition_columns']
        
        logger.info(f"Writing dataset to {output_path} (mode: {write_mode})")
        
        # Final sort to ensure correct order: timestamp, then Machine_ID
        # This matches the screenshot: 2024-01-01 00:00:00 Machine 1, 2, 3, 4...
        logger.info(f"Final sort by {timestamp_col}, {partition_cols} before writing")
        df = df.orderBy(timestamp_col, *partition_cols)
        
        # Verify timestamp order before writing
        if self.config.get('data_quality', {}).get('verify_timestamp_order', True):
            self._verify_timestamp_order(df)
        
        # Optional repartitioning for better parallelism
        if self.config['processing'].get('repartition', False):
            num_partitions = self.config['processing'].get('num_partitions', 10)
            logger.info(f"Repartitioning to {num_partitions} partitions and sorting internally")
            
        
        # CRITICAL FIX: Write WITHOUT partitionBy to avoid Machine_ID folders
        # This creates multiple parquet files in a flat structure, just like the original datasets
        logger.info(f"Writing without partitionBy - creating flat parquet structure")
        df.write \
            .mode(write_mode) \
            .parquet(output_path)
        
        logger.info(f"✓ Dataset written successfully")
        logger.info(f"   Output structure: {output_path}/part-*.parquet (flat structure, no Machine_ID folders)")
    
    def _verify_timestamp_order(self, df: Any):
        """Verify that timestamps are in chronological order within each partition"""
        logger.info("Verifying timestamp order...")
        
        timestamp_col = self.config['schema']['timestamp_column']
        partition_cols = self.config['schema']['partition_columns']
        
        # Sample check: Get first and last timestamp for each machine
        summary = df.groupBy(partition_cols).agg(
            F.min(timestamp_col).alias('first_timestamp'),
            F.max(timestamp_col).alias('last_timestamp'),
            F.count('*').alias('row_count')
        ).orderBy(partition_cols)
        
        logger.info("Timestamp range per machine:")
        summary.show(10, truncate=False)
        
        # Show sample of final output order
        logger.info("Sample of final output order (first 20 rows):")
        df.select(timestamp_col, *partition_cols).show(20, truncate=False)

    
    def process_dataset(self, dataset_config: Dict):
        """Process a single dataset with rolling features"""
        logger.info(f"=" * 80)
        logger.info(f"Processing dataset: {dataset_config['name']}")
        logger.info(f"=" * 80)
        
        # Read data
        df = self._read_dataset(dataset_config)
        
        # Data quality checks
        df = self._validate_data_quality(df, dataset_config)
        
        # Cache if configured
        if self.config['processing'].get('cache_intermediate', False):
            df = df.cache()
        
        # Apply rolling features
        df = self._apply_rolling_features(df)
        
        # Show sample results
        logger.info("Sample output (first 5 rows):")
        df.show(5, truncate=False)
        
        # Write results
        self._write_dataset(df, dataset_config)
        
        # Unpersist cache
        if self.config['processing'].get('cache_intermediate', False):
            df.unpersist()
        
        logger.info(f"✓ Completed processing: {dataset_config['name']}")
    
    def process_all_datasets(self):
        """Process all datasets defined in configuration"""
        logger.info("Starting historical ingestion with rolling features")
        logger.info(f"Total datasets to process: {len(self.config['datasets'])}")
        
        for dataset_config in self.config['datasets']:
            try:
                self.process_dataset(dataset_config)
            except Exception as e:
                logger.error(f"Error processing {dataset_config['name']}: {str(e)}", exc_info=True)
                continue
        
        logger.info("=" * 80)
        logger.info("Historical ingestion completed!")
        logger.info("=" * 80)
    
    def stop(self):
        """Stop Spark session"""
        logger.info("Stopping Spark session")
        self.spark.stop()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Historical Data Ingestion with Rolling Window Features'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='services/historical_ingestion_service/config/feature_engineering_config.yaml',
        help='Path to configuration YAML file'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        default=None,
        help='Process specific dataset by name (optional)'
    )
    
    args = parser.parse_args()
    
    # Initialize service
    service = HistoricalIngestionService(args.config)
    
    try:
        if args.dataset:
            # Process specific dataset
            dataset_config = next(
                (d for d in service.config['datasets'] if d['name'] == args.dataset),
                None
            )
            if dataset_config:
                service.process_dataset(dataset_config)
            else:
                logger.error(f"Dataset '{args.dataset}' not found in configuration")
                sys.exit(1)
        else:
            # Process all datasets
            service.process_all_datasets()
    finally:
        service.stop()


if __name__ == "__main__":
    main()